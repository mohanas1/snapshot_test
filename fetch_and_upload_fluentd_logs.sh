#!/bin/bash
################################################################################
# Enhanced script to fetch PC logs and upload to filer
#
# Usage:
#   ./fetch_and_upload_pc_logs.sh <pc_ip> [password] [bug_folder] [output_dir]
#
# Example:
#   ./fetch_and_upload_pc_logs.sh 10.114.55.128
#   ./fetch_and_upload_pc_logs.sh 10.114.55.128 custom_pass ENG-937578
#   ./fetch_and_upload_pc_logs.sh 10.114.55.128 nutanix/4u ENG-937578 /tmp/logs
#
# Note: Default password is 'nutanix/4u' if not provided
#
# Environment Variables:
#   PC_PASSWORD      - Default password for PC SSH
#   PC_USER          - SSH user for PC (default: nutanix)
#   FILER_HOST       - Filer server IP (default: 10.46.1.165)
#   FILER_USER       - Filer SSH user (default: nutanix)
#   FILER_PASSWORD   - Filer SSH password
#   FILER_BASE_PATH  - Base path on filer (default: /home/nutanix/data/Bugs)
################################################################################

set -e

# Configuration
PC_IP="${1}"
PC_PASSWORD="${2:-${PC_PASSWORD:-nutanix/4u}}"
BUG_FOLDER="${3}"
OUTPUT_DIR="${4:-./pc_logs}"
PC_USER="${PC_USER:-nutanix}"
KUBECONFIG_DIR="./kubeconfigs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Filer configuration
FILER_HOST="${FILER_HOST:-10.46.1.165}"
FILER_USER="${FILER_USER:-nutanix}"
FILER_PASSWORD="${FILER_PASSWORD:-nutanix/4u}"
FILER_BASE_PATH="${FILER_BASE_PATH:-/home/nutanix/data/Bugs}"

# Pod and namespace configuration
POD_NAME="fluentd-aggregator-0"
NAMESPACE="ntnx-system"
SOURCE_PATH="/fluentd/data/logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

################################################################################
# Functions
################################################################################

print_header() {
    echo "================================================================================"
    echo -e "${BLUE}$1${NC}"
    echo "================================================================================"
}

print_section() {
    echo ""
    echo "--------------------------------------------------------------------------------"
    echo -e "${CYAN}$1${NC}"
    echo "--------------------------------------------------------------------------------"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

check_requirements() {
    local missing=0
    
    for cmd in kubectl ssh scp tar; do
        if ! command -v $cmd &> /dev/null; then
            print_error "$cmd is not installed"
            missing=1
        fi
    done
    
    if [ -n "$PC_PASSWORD" ] && ! command -v sshpass &> /dev/null; then
        print_warning "sshpass not installed - will require manual password entry for PC"
    fi
    
    if [ -n "$FILER_PASSWORD" ] && ! command -v sshpass &> /dev/null; then
        print_warning "sshpass not installed - will require manual password entry for filer"
    fi
    
    # Optional but recommended for better upload reliability
    if ! command -v rsync &> /dev/null; then
        print_warning "rsync not installed - falling back to scp (slower, no resume support)"
        print_info "    Install rsync for better upload reliability: yum install rsync"
    fi
    
    return $missing
}

ssh_exec() {
    local host=$1
    local user=$2
    local password=$3
    shift 3
    local cmd="$@"
    
    local ssh_cmd="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
    
    if [ -n "$password" ] && command -v sshpass &> /dev/null; then
        ssh_cmd="sshpass -p '$password' $ssh_cmd"
    fi
    
    eval "$ssh_cmd ${user}@${host} '$cmd'"
}

scp_upload() {
    local source=$1
    local host=$2
    local user=$3
    local password=$4
    local dest=$5
    
    local scp_cmd="scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -r"
    
    if [ -n "$password" ] && command -v sshpass &> /dev/null; then
        scp_cmd="sshpass -p '$password' $scp_cmd"
    fi
    
    eval "$scp_cmd '$source' ${user}@${host}:'$dest/'"
}

fetch_kubeconfig() {
    local pc_ip=$1
    local output_file=$2
    
    ssh_exec "$pc_ip" "$PC_USER" "$PC_PASSWORD" \
        '/usr/local/nutanix/cluster/bin/mspctl cls kubeconfig nc' > "$output_file" 2>/dev/null
    
    return $?
}

verify_kubeconfig() {
    local kubeconfig_file=$1
    
    if [ ! -s "$kubeconfig_file" ]; then
        return 1
    fi
    
    kubectl --kubeconfig="$kubeconfig_file" cluster-info &> /dev/null
    return $?
}

copy_pod_logs() {
    local kubeconfig_file=$1
    local output_dir=$2
    
    print_info "Starting kubectl cp operation..."
    print_info "Source: ${NAMESPACE}/${POD_NAME}:${SOURCE_PATH}"
    print_info "Destination: $output_dir (will create 'logs' subdirectory)"
    print_info "This operation streams files and may take 1-3 minutes..."
    
    # Store output for debugging
    # IMPORTANT: NO trailing slash on destination to create the 'logs' directory
    local output
    output=$(kubectl --kubeconfig="$kubeconfig_file" cp \
        -n "$NAMESPACE" \
        "${POD_NAME}:${SOURCE_PATH}" \
        "$output_dir" \
        2>&1 | grep -v "Defaulted container" | grep -v "tar: removing leading" || true)
    
    local exit_code=${PIPESTATUS[0]}
    
    # Print any non-filtered output
    if [ -n "$output" ]; then
        echo "$output"
    fi
    
    if [ $exit_code -eq 0 ]; then
        print_info "kubectl cp command completed successfully"
        print_info "Logs should now be in: $output_dir/logs/"
    else
        print_error "kubectl cp failed with exit code: $exit_code"
    fi
    
    return $exit_code
}

create_filer_folder() {
    local folder_path=$1
    
    print_info "Creating folder on filer: $folder_path"
    print_info "  Filer: ${FILER_USER}@${FILER_HOST}"
    
    # Capture error output for debugging
    local ssh_output
    ssh_output=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "mkdir -p '$folder_path'" 2>&1)
    local ssh_exit=$?
    
    if [ $ssh_exit -eq 0 ]; then
        print_success "Folder created on filer"
        
        # Verify folder actually exists
        local verify_output
        verify_output=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "[ -d '$folder_path' ] && echo 'exists' || echo 'missing'" 2>&1)
        
        if [ "$verify_output" = "exists" ]; then
            print_success "Folder verified: $folder_path"
            return 0
        else
            print_error "Folder creation reported success but folder not found"
            print_error "  Verification output: $verify_output"
            return 1
        fi
    else
        print_error "Failed to create folder on filer (exit code: $ssh_exit)"
        print_error "  Command: mkdir -p '$folder_path'"
        if [ -n "$ssh_output" ]; then
            print_error "  SSH output: $ssh_output"
        fi
        
        # Additional diagnostics
        print_info "  Testing SSH connection..."
        local test_output
        test_output=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "echo 'SSH OK'" 2>&1)
        local test_exit=$?
        
        if [ $test_exit -eq 0 ]; then
            print_info "  SSH connection OK: $test_output"
            print_info "  Checking parent directory permissions..."
            local parent_dir=$(dirname "$folder_path")
            local parent_check
            parent_check=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "ls -ld '$parent_dir' 2>&1 || echo 'Parent dir not accessible'" 2>&1)
            print_info "  Parent dir: $parent_check"
        else
            print_error "  SSH connection test failed (exit code: $test_exit)"
            print_error "  Test output: $test_output"
        fi
        
        return 1
    fi
}

compress_logs() {
    local source_dir=$1
    local output_file=$2
    
    print_info "Compressing logs for faster upload..."
    
    local dir_size=$(du -sh "$source_dir" 2>/dev/null | cut -f1)
    print_info "  Original size: $dir_size"
    
    if tar -czf "$output_file" -C "$(dirname "$source_dir")" "$(basename "$source_dir")" 2>/dev/null; then
        local compressed_size=$(du -sh "$output_file" 2>/dev/null | cut -f1)
        print_success "Logs compressed: $compressed_size"
        return 0
    else
        print_error "Failed to compress logs"
        return 1
    fi
}

extract_namespaces() {
    local logs_dir=$1
    
    # Find all folders matching kube.* pattern and extract unique namespaces
    local namespaces=()
    
    if [ -d "$logs_dir" ]; then
        # Look for folders starting with kube. and extract namespace (second part after kube.)
        while IFS= read -r folder; do
            if [[ -n "$folder" ]]; then
                # Extract namespace from kube.namespace.* pattern
                local namespace=$(echo "$folder" | sed -n 's/^kube\.\([^.]*\)\..*$/\1/p')
                if [[ -n "$namespace" ]]; then
                    namespaces+=("$namespace")
                fi
            fi
        done < <(find "$logs_dir" -maxdepth 1 -type d -name "kube.*" -exec basename {} \;)
        
        # Get unique namespaces
        if [ ${#namespaces[@]} -gt 0 ]; then
            printf '%s\n' "${namespaces[@]}" | sort -u
        fi
    fi
}

compress_logs_by_namespace() {
    local source_dir=$1
    local output_dir=$2
    # Logs are directly in source_dir (kubectl cp puts them there)
    local logs_dir="$source_dir"
    
    print_info "Analyzing logs structure..." >&2
    print_info "Looking for logs in: $logs_dir" >&2
    
    if [ ! -d "$logs_dir" ]; then
        print_error "Logs directory not found: $logs_dir" >&2
        return 1
    fi
    
    # Extract unique namespaces
    local namespaces
    
    # Check if FLUENTD_NAMESPACES environment variable is set (comma-separated list)
    if [ -n "${FLUENTD_NAMESPACES:-}" ]; then
        if [ "$FLUENTD_NAMESPACES" = "NONE" ]; then
            print_warning "No namespaces selected - skipping fluentd log compression" >&2
            return 0
        fi
        print_info "Using selected namespaces from filter: $FLUENTD_NAMESPACES" >&2
        # Convert comma-separated string to array
        IFS=',' read -ra namespaces <<< "$FLUENTD_NAMESPACES"
        # Trim whitespace from each namespace
        for i in "${!namespaces[@]}"; do
            namespaces[$i]=$(echo "${namespaces[$i]}" | xargs)
        done
    else
        print_info "No namespace filter specified, detecting all namespaces..." >&2
        # Use mapfile for better compatibility (readarray is an alias)
        mapfile -t namespaces < <(extract_namespaces "$logs_dir")
    fi
    
    if [ ${#namespaces[@]} -eq 0 ]; then
        print_warning "No namespace folders found, compressing all logs together" >&2
        local output_file="${output_dir}/all_logs.tar.gz"
        # Compress the logs directory itself
        tar -czf "$output_file" -C "$(dirname "$logs_dir")" "$(basename "$logs_dir")" 2>&1 | grep -v "Removing leading" || true
        if [ -f "$output_file" ]; then
            echo "$output_file"
            return 0
        else
            return 1
        fi
    fi
    
    print_success "Processing ${#namespaces[@]} namespace(s): ${namespaces[*]}" >&2
    echo "" >&2
    
    # Create output directory for compressed files
    mkdir -p "$output_dir"
    
    local compressed_files=()
    local failed=0
    
    # Compress each namespace separately
    for namespace in "${namespaces[@]}"; do
        print_section "Compressing namespace: $namespace" >&2
        
        local output_file="${output_dir}/${namespace}.tar.gz"
        local pattern="kube.${namespace}.*"
        
        # Find all folders matching this namespace
        local folder_count=$(find "$logs_dir" -maxdepth 1 -type d -name "$pattern" | wc -l)
        print_info "  Found $folder_count folder(s) for namespace '$namespace'" >&2
        
        if [ $folder_count -eq 0 ]; then
            print_warning "  No folders found for pattern: $pattern" >&2
            continue
        fi
        
        # Get size before compression
        local namespace_size=$(du -sh "$logs_dir" 2>/dev/null | awk '{s+=$1}END{print s}' || echo "0")
        local size_display=$(find "$logs_dir" -maxdepth 1 -type d -name "$pattern" -exec du -sh {} + 2>/dev/null | awk '{sum+=$1; print}' | tail -1 | cut -f1)
        print_info "  Size: $size_display" >&2
        
        # Create tar with all matching folders for this namespace
        print_info "  Creating archive: $(basename "$output_file")" >&2
        
        # Get actual directory names (not wildcards)
        local dirs_to_compress=()
        while IFS= read -r dir; do
            dirs_to_compress+=("$(basename "$dir")")
        done < <(find "$logs_dir" -maxdepth 1 -type d -name "$pattern")
        
        if [ ${#dirs_to_compress[@]} -eq 0 ]; then
            print_error "  No directories found to compress for pattern: $pattern" >&2
            failed=1
            continue
        fi
        
        print_info "  Compressing ${#dirs_to_compress[@]} directories" >&2
        
        # Disable set -e temporarily to handle tar failures gracefully
        set +e
        tar_output=$(tar -czf "$output_file" -C "$logs_dir" "${dirs_to_compress[@]}" 2>&1 | grep -v "Removing leading" || true)
        tar_exit=$?
        set -e
        
        if [ $tar_exit -eq 0 ] && [ -f "$output_file" ]; then
            local compressed_size=$(du -sh "$output_file" 2>/dev/null | cut -f1)
            print_success "  Compressed: $compressed_size" >&2
            compressed_files+=("$output_file")
        else
            if echo "$tar_output" | grep -q "No space left on device"; then
                print_error "  ⚠️  No space left on device while compressing $namespace" >&2
                print_error "  Please free up disk space and try again" >&2
            else
                print_error "  Failed to compress namespace: $namespace (tar exit code: $tar_exit)" >&2
                if [ -n "$tar_output" ]; then
                    print_error "  Error: $tar_output" >&2
                fi
            fi
            failed=1
        fi
        echo "" >&2
    done
    
    if [ ${#compressed_files[@]} -eq 0 ]; then
        print_error "No files were compressed successfully" >&2
        return 1
    fi
    
    # Print compressed files (one per line for caller to read)
    printf '%s\n' "${compressed_files[@]}"
    
    return $failed
}

upload_file_to_filer() {
    local local_file=$1
    local filer_path=$2
    local max_retries=3
    local retry_count=0
    
    local upload_name=$(basename "$local_file")
    
    print_info "  Source: $local_file"
    print_info "  Destination: ${FILER_HOST}:${filer_path}/"
    print_info "  Size: $(du -sh "$local_file" 2>/dev/null | cut -f1)"
    
    # Try with rsync first (supports resume and progress)
    if command -v rsync &> /dev/null && command -v sshpass &> /dev/null && [ -n "$FILER_PASSWORD" ]; then
        print_info "  Using rsync for reliable transfer..."
        
        while [ $retry_count -lt $max_retries ]; do
            if [ $retry_count -gt 0 ]; then
                print_warning "  Retry attempt $retry_count/$max_retries..."
                sleep 5
            fi
            
            rsync_output=$(sshpass -p "$FILER_PASSWORD" rsync -avz --progress --timeout=300 \
                -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR" \
                "$local_file" "${FILER_USER}@${FILER_HOST}:${filer_path}/" 2>&1 | \
                grep -v "StrictHostKeyChecking" | grep -v "Warning" || true)
            
            rsync_exit=$?
            
            if [ $rsync_exit -eq 0 ]; then
                print_success "  Uploaded: $upload_name"
                return 0
            else
                # Provide detailed error based on output
                if echo "$rsync_output" | grep -qi "permission denied"; then
                    print_error "  ERROR: Permission denied on filer"
                    print_error "  Path: $filer_path"
                    print_error "  User: $FILER_USER may not have write access"
                elif echo "$rsync_output" | grep -qi "authentication failed\|password"; then
                    print_error "  ERROR: Authentication failed to filer"
                    print_error "  Check FILER_PASSWORD for user: $FILER_USER"
                elif echo "$rsync_output" | grep -qi "no such file\|not found"; then
                    print_error "  ERROR: Target path not found on filer"
                    print_error "  Path: $filer_path"
                elif echo "$rsync_output" | grep -qi "connection refused\|network\|unreachable\|timeout"; then
                    print_error "  ERROR: Cannot connect to filer"
                    print_error "  Filer: $FILER_HOST (check network/firewall)"
                else
                    print_error "  rsync failed (exit: $rsync_exit)"
                    if [ -n "$rsync_output" ]; then
                        print_error "  Details: $rsync_output"
                    fi
                fi
            fi
            
            retry_count=$((retry_count + 1))
        done
        
        print_error "  rsync upload failed after $max_retries attempts"
    fi
    
    # Fallback to scp with retries
    print_warning "  Falling back to scp..."
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if [ $retry_count -gt 0 ]; then
            print_warning "  Retry attempt $retry_count/$max_retries..."
            sleep 5
        fi
        
        if scp_upload "$local_file" "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "$filer_path" 2>&1 | grep -v "StrictHostKeyChecking" | grep -v "Warning"; then
            print_success "  Uploaded: $upload_name"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
    done
    
    print_error "  Upload failed: $upload_name"
    return 1
}

upload_to_filer() {
    local local_path=$1
    local filer_path=$2
    local max_retries=3
    local retry_count=0
    
    # Compress logs first
    local compressed_file="${local_path}.tar.gz"
    if ! compress_logs "$local_path" "$compressed_file"; then
        print_warning "Compression failed, uploading uncompressed (slower)"
        compressed_file=""
    fi
    
    # Use compressed file if available, otherwise use original
    local upload_source="${compressed_file:-$local_path}"
    local upload_name=$(basename "$upload_source")
    
    print_info "Uploading to filer..."
    print_info "  Source: $upload_source"
    print_info "  Destination: ${FILER_HOST}:${filer_path}"
    print_info "  Size: $(du -sh "$upload_source" 2>/dev/null | cut -f1)"
    
    # Try with rsync first (supports resume and progress)
    if command -v rsync &> /dev/null && command -v sshpass &> /dev/null && [ -n "$FILER_PASSWORD" ]; then
        print_info "Using rsync for reliable transfer with progress..."
        
        while [ $retry_count -lt $max_retries ]; do
            if [ $retry_count -gt 0 ]; then
                print_warning "Retry attempt $retry_count/$max_retries..."
                sleep 5
            fi
            
            if sshpass -p "$FILER_PASSWORD" rsync -avz --progress --timeout=300 \
                -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR" \
                "$upload_source" "${FILER_USER}@${FILER_HOST}:${filer_path}/" 2>&1 | \
                grep -v "StrictHostKeyChecking" | grep -v "Warning"; then
                
                print_success "Logs uploaded successfully via rsync"
                
                # Cleanup compressed file if used
                if [ -n "$compressed_file" ] && [ -f "$compressed_file" ]; then
                    rm -f "$compressed_file"
                    print_info "Cleaned up compressed file"
                fi
                
                return 0
            fi
            
            retry_count=$((retry_count + 1))
        done
        
        print_error "rsync upload failed after $max_retries attempts"
    fi
    
    # Fallback to scp with retries
    print_warning "Falling back to scp (no resume support)..."
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if [ $retry_count -gt 0 ]; then
            print_warning "Retry attempt $retry_count/$max_retries..."
            sleep 5
        fi
        
        if scp_upload "$upload_source" "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "$filer_path"; then
            print_success "Logs uploaded successfully via scp"
            
            # Cleanup compressed file if used
            if [ -n "$compressed_file" ] && [ -f "$compressed_file" ]; then
                rm -f "$compressed_file"
                print_info "Cleaned up compressed file"
            fi
            
            return 0
        fi
        
        retry_count=$((retry_count + 1))
    done
    
    print_error "Upload failed after $max_retries attempts"
    
    # Cleanup compressed file on failure
    if [ -n "$compressed_file" ] && [ -f "$compressed_file" ]; then
        rm -f "$compressed_file"
    fi
    
    return 1
}

verify_filer_upload() {
    local filer_path=$1
    local folder_name=$2
    
    print_info "Verifying upload on filer..."
    
    # Check if compressed file exists
    local compressed_name="${folder_name}.tar.gz"
    local compressed_exists=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
        "[ -f '$filer_path/$compressed_name' ] && echo 'yes' || echo 'no'" 2>/dev/null)
    
    if [ "$compressed_exists" = "yes" ]; then
        print_success "Verified: Compressed archive uploaded"
        
        local size=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
            "du -sh '$filer_path/$compressed_name' 2>/dev/null | cut -f1" 2>/dev/null || echo "unknown")
        print_info "Archive size on filer: $size"
        print_info "Archive name: $compressed_name"
        
        return 0
    fi
    
    # Check if uncompressed folder exists
    local file_count=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
        "find '$filer_path/$folder_name' -type f 2>/dev/null | wc -l" 2>/dev/null || echo "0")
    
    if [ "$file_count" -gt 0 ]; then
        print_success "Verified: $file_count files uploaded"
        
        local size=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
            "du -sh '$filer_path/$folder_name' 2>/dev/null | cut -f1" 2>/dev/null || echo "unknown")
        print_info "Total size on filer: $size"
        
        return 0
    else
        print_error "Upload verification failed - no files found on filer"
        return 1
    fi
}

get_filer_url() {
    local relative_path=$1
    # Generate web URL based on filer type
    if [[ "$FILER_BASE_PATH" == "/home/nutanix/data/Bugs"* ]] || [[ "$FILER_BASE_PATH" == "/home/nutanix/data/bugs"* ]]; then
        # For filer1, use /bugs as URL base (lowercase for web access)
        echo "http://${FILER_HOST}/bugs/${relative_path}"
    elif [[ "$FILER_BASE_PATH" == "/var/nfs_share"* ]]; then
        # For filer2, use base path as-is
        echo "http://${FILER_HOST}/${relative_path}"
    else
        # Generic fallback
        echo "http://${FILER_HOST}/${relative_path}"
    fi
}

################################################################################
# Main Script
################################################################################

# Check arguments
if [ -z "$PC_IP" ]; then
    echo -e "${RED}Error: PC IP address required${NC}"
    echo ""
    echo "Usage: $0 <pc_ip> [password] [bug_folder] [output_dir]"
    echo ""
    echo "Examples:"
    echo "  $0 10.114.55.128 nutanix/4u ENG-937578"
    echo "  $0 10.114.55.128 nutanix/4u ENG-937578 /tmp/logs"
    echo ""
    echo "Environment Variables:"
    echo "  PC_PASSWORD      - Default password for PC"
    echo "  FILER_PASSWORD   - Password for filer upload"
    echo "  FILER_HOST       - Filer server (default: 10.46.1.165)"
    echo "  FILER_BASE_PATH  - Base path on filer (default: /home/nutanix/data/Bugs)"
    exit 1
fi

# Generate bug folder name if not provided
if [ -z "$BUG_FOLDER" ]; then
    BUG_FOLDER="temp_${PC_IP}_${TIMESTAMP}"
    print_warning "No bug folder specified, using: $BUG_FOLDER"
fi

print_header "PC Log Fetcher with Filer Upload"
echo "PC IP:           $PC_IP"
echo "User:            $PC_USER"
echo "PC Password:     $([ -n "$PC_PASSWORD" ] && echo "***" || echo "not provided")"
echo "Bug Folder:      $BUG_FOLDER"
echo "Output Dir:      $OUTPUT_DIR"
echo "Kubeconfig Dir:  $KUBECONFIG_DIR"
echo "Timestamp:       $TIMESTAMP"
echo "Pod:             $NAMESPACE/$POD_NAME"
echo "Source Path:     $SOURCE_PATH"
echo ""
echo "Filer Settings:"
echo "  Host:          $FILER_HOST"
echo "  User:          $FILER_USER"
echo "  Password:      $([ -n "$FILER_PASSWORD" ] && echo "***" || echo "not provided")"
echo "  Base Path:     $FILER_BASE_PATH"
echo "  Target Folder: $FILER_BASE_PATH/$BUG_FOLDER"
echo ""

# Check requirements
if ! check_requirements; then
    exit 1
fi

# Create directories
mkdir -p "$KUBECONFIG_DIR"
mkdir -p "$OUTPUT_DIR"

KUBECONFIG_FILE="$KUBECONFIG_DIR/${PC_IP}_kubeconfig"
KUBECONFIG_FILE_NEW="$KUBECONFIG_DIR/${PC_IP}_kubeconfig_${TIMESTAMP}"
LOG_OUTPUT_DIR="$OUTPUT_DIR/${PC_IP}_${TIMESTAMP}"
LOG_FOLDER_NAME=$(basename "$LOG_OUTPUT_DIR")

# Step 1: Check and use existing kubeconfig
print_header "Step 1/7: Checking Kubeconfig"
NEED_NEW_KUBECONFIG=0

if [ -f "$KUBECONFIG_FILE" ]; then
    print_info "Found existing kubeconfig for ${PC_IP}"
    print_info "Verifying existing kubeconfig..."
    
    if verify_kubeconfig "$KUBECONFIG_FILE"; then
        print_success "Existing kubeconfig is valid and cluster is reachable"
        print_info "Using: ${KUBECONFIG_FILE}"
    else
        print_warning "Existing kubeconfig validation failed"
        NEED_NEW_KUBECONFIG=1
    fi
else
    print_info "No existing kubeconfig found for ${PC_IP}"
    NEED_NEW_KUBECONFIG=1
fi

if [ $NEED_NEW_KUBECONFIG -eq 1 ]; then
    print_info "Fetching new kubeconfig from ${PC_USER}@${PC_IP}..."
    
    if fetch_kubeconfig "$PC_IP" "$KUBECONFIG_FILE_NEW"; then
        print_success "New kubeconfig fetched successfully"
        # Replace old kubeconfig with new one
        mv "$KUBECONFIG_FILE_NEW" "$KUBECONFIG_FILE"
        print_info "Saved to: ${KUBECONFIG_FILE}"
    else
        print_error "Failed to fetch kubeconfig"
        exit 1
    fi
fi
echo ""

# Step 2: Final verification
print_header "Step 2/7: Final Kubeconfig Verification"
if verify_kubeconfig "$KUBECONFIG_FILE"; then
    print_success "Kubeconfig is valid and cluster is reachable"
else
    print_error "Kubeconfig validation failed"
    exit 1
fi
echo ""

# Step 3: Copy logs from pod
print_header "Step 3/7: Copying Fluentd Logs from Pod"
print_info "Creating output directory: ${LOG_OUTPUT_DIR}"
mkdir -p "$LOG_OUTPUT_DIR"

print_info "Copying logs from ${POD_NAME}:${SOURCE_PATH}..."
print_info "This may take several minutes depending on log size..."

if copy_pod_logs "$KUBECONFIG_FILE" "$LOG_OUTPUT_DIR"; then
    print_success "Logs copied successfully from pod"
    
    print_info "Verifying copied logs..."
    
    # kubectl cp copies the CONTENTS of /fluentd/data/logs directly into LOG_OUTPUT_DIR
    # So we look for directories directly in LOG_OUTPUT_DIR, not in LOG_OUTPUT_DIR/logs
    print_info "Checking copied log directories..."
    
    # Count namespace directories directly in output dir
    print_info "Counting log directories in output folder..."
    set +e
    dir_count=$(find "$LOG_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type d -name "kube.*" 2>/dev/null | wc -l)
    dir_find_exit=$?
    set -e
    
    # Trim all whitespace (spaces, tabs, newlines)
    dir_count=$(echo "$dir_count" | tr -d '[:space:]')
    
    # Debug output
    print_info "Raw count result: '$dir_count' (exit code: $dir_find_exit)"
    
    # Validate it's a number using simple pattern matching (more portable)
    case "$dir_count" in
        ''|*[!0-9]*)
            # Empty or contains non-digits
            print_warning "Count is not a valid number: '$dir_count', setting to 0"
            dir_count=0
            ;;
        *)
            # Valid number, convert to integer
            dir_count=$((dir_count + 0))
            print_info "Valid count: $dir_count log directories"
            ;;
    esac
    
    # If we have directories, we have logs
    if [ $dir_count -gt 0 ]; then
        print_success "Found $dir_count namespace log directories"
        
        # Calculate size
        print_info "Calculating total log size..."
        set +e
        log_size=$(du -sh "$LOG_OUTPUT_DIR" 2>/dev/null | cut -f1)
        du_exit=$?
        set -e
        
        if [ -z "$log_size" ] || [ $du_exit -ne 0 ]; then
            log_size="unknown"
            print_warning "Could not calculate log size (du exit code: $du_exit)"
        else
            print_info "Total log size: $log_size"
        fi
        
        print_info "Local logs: $dir_count directories, $log_size"
        print_success "Step 3 completed: Logs verified successfully"
    else
        print_error "No log directories found: dir_count=$dir_count"
        print_error "Pod logs directory may be empty or copy failed"
        print_info "Attempting alternative count method..."
        
        # Try alternative method: count with ls
        set +e
        alt_count=$(ls -1d "$LOG_OUTPUT_DIR"/kube.* 2>/dev/null | wc -l)
        alt_count=$(echo "$alt_count" | tr -d '[:space:]')
        set -e
        
        print_info "Alternative count: $alt_count directories"
        
        # If alternative method finds directories, use that
        if [ -n "$alt_count" ] && [ "$alt_count" -gt 0 ] 2>/dev/null; then
            print_success "Alternative method found $alt_count directories!"
            print_info "Continuing with verification..."
            dir_count=$alt_count
            
            # Calculate size
            print_info "Calculating total log size..."
            set +e
            log_size=$(du -sh "$LOG_OUTPUT_DIR" 2>/dev/null | cut -f1)
            du_exit=$?
            set -e
            
            if [ -z "$log_size" ] || [ $du_exit -ne 0 ]; then
                log_size="unknown"
                print_warning "Could not calculate log size (du exit code: $du_exit)"
            else
                print_info "Total log size: $log_size"
            fi
            
            print_info "Local logs: $dir_count directories, $log_size"
            print_success "Step 3 completed: Logs verified successfully"
        else
            print_error "Both counting methods failed"
            print_info "Listing directory contents (first 50 items):"
            ls -la "$LOG_OUTPUT_DIR" 2>&1 | head -50 || true
            
            print_info "Testing basic directory check..."
            if [ "$(ls -A "$LOG_OUTPUT_DIR" 2>/dev/null)" ]; then
                print_warning "Directory is not empty, but counting failed"
                print_warning "Proceeding anyway - manual verification recommended"
            else
                print_error "Directory appears to be empty"
                exit 1
            fi
        fi
    fi
else
    print_error "Failed to copy logs from pod"
    exit 1
fi
echo ""

# Step 4: Create folder on filer
print_header "Step 4/7: Creating Folder on Filer"
FILER_TARGET_PATH="$FILER_BASE_PATH/$BUG_FOLDER"

if ! create_filer_folder "$FILER_TARGET_PATH"; then
    print_error "Cannot proceed with upload"
    print_warning "Local logs are available at: $LOG_OUTPUT_DIR"
    exit 1
fi
echo ""

# Step 5: Compress logs by namespace
print_header "Step 5/7: Compressing Logs by Namespace"

# Verify log directories exist before attempting compression
dir_check=$(find "$LOG_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type d -name "kube.*" 2>/dev/null | wc -l | tr -d '[:space:]')
if [ -z "$dir_check" ] || [ "$dir_check" -eq 0 ]; then
    print_error "No log directories found in: $LOG_OUTPUT_DIR"
    print_error "Log copy from pod may have failed"
    print_warning "Check if pod logs were copied successfully in Step 3"
    exit 1
fi
print_info "Found $dir_check log directories to compress"

# Check available disk space before compression
print_info "Checking available disk space..."
available_space=$(df -BG "$OUTPUT_DIR" | awk 'NR==2 {print $4}' | tr -d 'G')
log_size=$(du -sm "$LOG_OUTPUT_DIR" 2>/dev/null | awk '{print $1}' || echo "0")

if [ "$available_space" -lt 1 ]; then
    print_error "⚠️  Insufficient disk space detected!"
    print_error "   Available: ${available_space}GB"
    print_error "   Estimated needed: ~${log_size}MB for compression"
    print_error "   Please free up disk space and try again"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    exit 1
fi

print_info "Available disk space: ${available_space}GB (Log size: ${log_size}MB)"

COMPRESSED_DIR="${OUTPUT_DIR}/compressed_${TIMESTAMP}"

# Try to create compressed directory with error handling
set +e
mkdir_output=$(mkdir -p "$COMPRESSED_DIR" 2>&1)
mkdir_exit=$?
set -e

if [ $mkdir_exit -ne 0 ]; then
    print_error "⚠️  Failed to create compressed directory!"
    if echo "$mkdir_output" | grep -q "No space left on device"; then
        print_error "   Error: No space left on device"
        print_error "   Please free up disk space and try again"
    else
        print_error "   Error: $mkdir_output"
    fi
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    exit 1
fi

# Get list of compressed files
print_info "Calling compress_logs_by_namespace..."
declare -a COMPRESSED_FILES

# Temporarily disable set -e to capture output and handle errors gracefully
set +e
mapfile -t COMPRESSED_FILES < <(compress_logs_by_namespace "$LOG_OUTPUT_DIR" "$COMPRESSED_DIR")
compress_exit_code=$?
set -e

if [ $compress_exit_code -ne 0 ]; then
    print_error "Compression function failed with exit code: $compress_exit_code"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    exit 1
fi

if [ ${#COMPRESSED_FILES[@]} -eq 0 ]; then
    # Check if this is because no namespaces were selected
    if [ "${FLUENTD_NAMESPACES:-}" = "NONE" ]; then
        print_warning "No fluentd namespaces selected - skipping file upload"
        print_info "Proceeding to next steps..."
    else
        print_error "No files were compressed"
        print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
        exit 1
    fi
fi

# Only proceed with size calculation and upload if files were compressed
if [ ${#COMPRESSED_FILES[@]} -gt 0 ]; then
    print_success "Created ${#COMPRESSED_FILES[@]} compressed file(s)"

    # Calculate total compressed size
    TOTAL_SIZE_BYTES=0
    for file in "${COMPRESSED_FILES[@]}"; do
        if [ -f "$file" ]; then
            # Get file size (temporarily disable set -e for compatibility check)
            set +e
            file_size=$(stat -c%s "$file" 2>/dev/null)
            if [ -z "$file_size" ] || [ "$file_size" = "" ]; then
                file_size=$(stat -f%z "$file" 2>/dev/null)
            fi
            if [ -z "$file_size" ] || [ "$file_size" = "" ]; then
                file_size="0"
            fi
            set -e
            TOTAL_SIZE_BYTES=$((TOTAL_SIZE_BYTES + file_size))
        fi
    done

    # Convert to GB for display (without bc)
    if [ $TOTAL_SIZE_BYTES -gt 0 ]; then
        # Use awk instead of bc for better compatibility
        TOTAL_SIZE_GB=$(awk "BEGIN {printf \"%.2f\", $TOTAL_SIZE_BYTES / 1024 / 1024 / 1024}")
        print_info "Total compressed size: ${TOTAL_SIZE_GB} GB"
    fi
    echo ""

    # Step 6: Upload compressed files to filer
    print_header "Step 6/7: Uploading Compressed Files to Filer"

    UPLOAD_SUCCESS_COUNT=0
    UPLOAD_FAILED_COUNT=0
    declare -a UPLOADED_FILES

    for compressed_file in "${COMPRESSED_FILES[@]}"; do
        file_name=$(basename "$compressed_file")
        print_section "Uploading: $file_name"
        
        if upload_file_to_filer "$compressed_file" "$FILER_TARGET_PATH"; then
            UPLOAD_SUCCESS_COUNT=$((UPLOAD_SUCCESS_COUNT + 1))
            UPLOADED_FILES+=("$file_name")
    else
        UPLOAD_FAILED_COUNT=$((UPLOAD_FAILED_COUNT + 1))
    fi
    echo ""
done

if [ $UPLOAD_FAILED_COUNT -gt 0 ]; then
    print_error "$UPLOAD_FAILED_COUNT file(s) failed to upload"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    print_warning "Compressed files at: $COMPRESSED_DIR"
    exit 1
fi

print_success "All $UPLOAD_SUCCESS_COUNT file(s) uploaded successfully"
echo ""

# Step 7: Verify and cleanup
print_header "Step 7/7: Verification and Cleanup"

# Verify uploads
VERIFY_SUCCESS=0
VERIFY_FAILED=0

print_info "Verifying uploaded files on filer..."
print_info "Checking ${#UPLOADED_FILES[@]} file(s) at: ${FILER_HOST}:${FILER_TARGET_PATH}"
echo "" >&2

for file_name in "${UPLOADED_FILES[@]}"; do
    print_info "Verifying: $file_name" >&2
    
    # Try to check file existence
    file_exists=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
        "[ -f '$FILER_TARGET_PATH/$file_name' ] && echo 'yes' || echo 'no'" 2>&1)
    
    verify_exit_code=$?
    
    if [ $verify_exit_code -ne 0 ]; then
        print_warning "  SSH verification command failed (exit code: $verify_exit_code)" >&2
        print_warning "  Output: $file_exists" >&2
        
        # Provide specific error context
        if echo "$file_exists" | grep -qi "permission denied"; then
            print_error "  $file_name - ERROR: Permission denied on filer"
            print_error "  Check user '${FILER_USER}' has access to: $FILER_TARGET_PATH"
        elif echo "$file_exists" | grep -qi "authentication failed\|password"; then
            print_error "  $file_name - ERROR: Authentication failed to filer"
            print_error "  Verify FILER_PASSWORD is correct for user '${FILER_USER}'"
        elif echo "$file_exists" | grep -qi "no such file\|not found"; then
            print_error "  $file_name - ERROR: Path not found on filer"
            print_error "  Path: $FILER_TARGET_PATH"
        elif echo "$file_exists" | grep -qi "connection refused\|network\|unreachable"; then
            print_error "  $file_name - ERROR: Cannot connect to filer"
            print_error "  Filer: ${FILER_HOST}"
        else
            print_error "  $file_name - ERROR: Verification failed (exit code: $verify_exit_code)"
            print_error "  Details: $file_exists"
        fi
        
        VERIFY_FAILED=$((VERIFY_FAILED + 1))
    elif [ "$file_exists" = "yes" ]; then
        size=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
            "du -sh '$FILER_TARGET_PATH/$file_name' 2>/dev/null | cut -f1" 2>/dev/null || echo "unknown")
        print_success "  $file_name ($size)"
        VERIFY_SUCCESS=$((VERIFY_SUCCESS + 1))
    else
        print_error "  $file_name - ERROR: File not found on filer after upload"
        print_info "    Expected path: $FILER_TARGET_PATH/$file_name" >&2
        print_info "    This usually means upload completed but file wasn't written" >&2
        VERIFY_FAILED=$((VERIFY_FAILED + 1))
    fi
done

echo "" >&2

if [ $VERIFY_FAILED -gt 0 ]; then
    print_error "Verification failed for $VERIFY_FAILED of ${#UPLOADED_FILES[@]} file(s)"
    print_error "Failed files may not have been uploaded successfully or path verification failed"
    print_info "Filer target path: ${FILER_HOST}:${FILER_TARGET_PATH}"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    print_warning "Compressed files at: $COMPRESSED_DIR"
    
    # List what was expected vs what was found
    echo "" >&2
    print_info "Debugging information:" >&2
    print_info "  Expected ${#UPLOADED_FILES[@]} files at: $FILER_TARGET_PATH" >&2
    print_info "  Verified: $VERIFY_SUCCESS successful, $VERIFY_FAILED failed" >&2
    
    exit 1
fi

print_success "All $VERIFY_SUCCESS file(s) verified on filer"
echo ""

# Cleanup local files
print_info "Cleaning up local files..."
if rm -rf "$LOG_OUTPUT_DIR"; then
    print_success "Local logs deleted"
else
    print_warning "Could not delete local logs at: $LOG_OUTPUT_DIR"
fi

if rm -rf "$COMPRESSED_DIR"; then
    print_success "Compressed files deleted"
else
    print_warning "Could not delete compressed files at: $COMPRESSED_DIR"
fi
echo ""

else
    # No files were compressed (no namespaces selected)
    print_info "Skipping Steps 6 & 7: No fluentd files to upload"
    print_success "Fluentd steps completed (skipped per configuration)"
    echo ""
fi

# Final summary
print_header "✅ Success - All Operations Completed"
echo ""

if [ ${#COMPRESSED_FILES[@]} -gt 0 ]; then
    echo "📁 Files:"
    echo "  Kubeconfig:      ${KUBECONFIG_FILE}"
    echo "  Filer Location:  ${FILER_HOST}:${FILER_TARGET_PATH}/"
    echo "  Format:          Compressed by namespace (tar.gz)"
    echo ""

    echo "📦 Uploaded Files (${#UPLOADED_FILES[@]}):"
    for file_name in "${UPLOADED_FILES[@]}"; do
    # Get file size from filer
    size=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
        "du -sh '$FILER_TARGET_PATH/$file_name' 2>/dev/null | cut -f1" 2>/dev/null || echo "?")
    
    # Extract namespace from filename (remove .tar.gz)
    namespace=$(basename "$file_name" .tar.gz)
    
    echo "  • $file_name ($size)"
    
    # Generate URL for this file
    file_url=$(get_filer_url "$BUG_FOLDER/$file_name")
    echo "    URL: ${file_url}"
done
echo ""

echo "🌐 Filer Folder URL:"
FILER_URL=$(get_filer_url "$BUG_FOLDER")
echo "  ${FILER_URL}/"
echo ""

    echo "📊 Summary:"
    echo "  ✓ Kubeconfig fetched from PC"
    echo "  ✓ Logs copied from fluentd pod"
    echo "  ✓ Logs split by namespace (${#UPLOADED_FILES[@]} namespaces)"
    echo "  ✓ Each namespace compressed separately"
    echo "  ✓ All files uploaded to filer"
    echo "  ✓ Upload verified"
    echo "  ✓ Local logs cleaned up"
    echo ""

    echo "💡 Tips:"
    echo "  - Each namespace has its own tar.gz file for faster downloads"
    echo "  - Extract specific namespace: tar -xzf namespace.tar.gz"
    echo "  - Use the URLs above to access files via browser"
    echo "  - Kubeconfig saved locally for future use"
    echo ""
else
    echo "📊 Summary:"
    echo "  ✓ Kubeconfig fetched from PC"
    echo "  ✓ Logs copied from fluentd pod"
    echo "  ⊘ Fluentd namespace compression skipped (no namespaces selected)"
    echo "  ⊘ File upload skipped"
    echo "  ℹ Local logs available at: $LOG_OUTPUT_DIR"
    echo ""
fi

print_header "Done"
