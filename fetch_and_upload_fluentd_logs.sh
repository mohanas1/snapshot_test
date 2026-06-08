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
    
    # Store output for debugging
    local output
    output=$(kubectl --kubeconfig="$kubeconfig_file" cp \
        -n "$NAMESPACE" \
        "${POD_NAME}:${SOURCE_PATH}" \
        "$output_dir/" \
        2>&1 | grep -v "Defaulted container" | grep -v "tar: removing leading" || true)
    
    local exit_code=${PIPESTATUS[0]}
    
    # Print any non-filtered output
    if [ -n "$output" ]; then
        echo "$output"
    fi
    
    return $exit_code
}

create_filer_folder() {
    local folder_path=$1
    
    print_info "Creating folder on filer: $folder_path"
    
    if ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "mkdir -p '$folder_path'" 2>/dev/null; then
        print_success "Folder created on filer"
        return 0
    else
        print_error "Failed to create folder on filer"
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
    local logs_dir="${source_dir}/logs"
    
    print_info "Analyzing logs structure..." >&2
    
    if [ ! -d "$logs_dir" ]; then
        print_error "Logs directory not found: $logs_dir" >&2
        return 1
    fi
    
    # Extract unique namespaces
    local namespaces
    readarray -t namespaces < <(extract_namespaces "$logs_dir")
    
    if [ ${#namespaces[@]} -eq 0 ]; then
        print_warning "No namespace folders found, compressing all logs together" >&2
        local output_file="${output_dir}/all_logs.tar.gz"
        compress_logs "$source_dir" "$output_file" >&2
        echo "$output_file"
        return $?
    fi
    
    print_success "Found ${#namespaces[@]} unique namespace(s): ${namespaces[*]}" >&2
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
        if tar -czf "$output_file" -C "$logs_dir" $pattern 2>/dev/null; then
            local compressed_size=$(du -sh "$output_file" 2>/dev/null | cut -f1)
            print_success "  Compressed: $compressed_size" >&2
            compressed_files+=("$output_file")
        else
            print_error "  Failed to compress namespace: $namespace" >&2
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
            
            if sshpass -p "$FILER_PASSWORD" rsync -avz --progress --timeout=300 \
                -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR" \
                "$local_file" "${FILER_USER}@${FILER_HOST}:${filer_path}/" 2>&1 | \
                grep -v "StrictHostKeyChecking" | grep -v "Warning"; then
                
                print_success "  Uploaded: $upload_name"
                return 0
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

KUBECONFIG_FILE="$KUBECONFIG_DIR/${PC_IP}_kubeconfig_${TIMESTAMP}"
LOG_OUTPUT_DIR="$OUTPUT_DIR/${PC_IP}_${TIMESTAMP}"
LOG_FOLDER_NAME=$(basename "$LOG_OUTPUT_DIR")

# Step 1: Fetch kubeconfig
print_header "Step 1/7: Fetching Kubeconfig"
print_info "Connecting to ${PC_USER}@${PC_IP}..."

if fetch_kubeconfig "$PC_IP" "$KUBECONFIG_FILE"; then
    print_success "Kubeconfig fetched successfully"
    print_info "Saved to: ${KUBECONFIG_FILE}"
else
    print_error "Failed to fetch kubeconfig"
    exit 1
fi
echo ""

# Step 2: Verify kubeconfig
print_header "Step 2/7: Verifying Kubeconfig"
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
if copy_pod_logs "$KUBECONFIG_FILE" "$LOG_OUTPUT_DIR"; then
    print_success "Logs copied successfully from pod"
    
    # Verify logs directory exists
    if [ ! -d "$LOG_OUTPUT_DIR/logs" ]; then
        print_error "Logs directory not created: $LOG_OUTPUT_DIR/logs"
        print_error "kubectl cp may have failed silently"
        print_info "Checking what was copied..."
        ls -la "$LOG_OUTPUT_DIR" || true
        exit 1
    fi
    
    # Show log stats and verify files exist
    file_count=$(find "$LOG_OUTPUT_DIR/logs" -type f 2>/dev/null | wc -l)
    if [ "$file_count" -eq 0 ]; then
        print_error "No log files found in $LOG_OUTPUT_DIR/logs"
        print_error "Pod logs directory may be empty or copy failed"
        exit 1
    fi
    
    log_size=$(du -sh "$LOG_OUTPUT_DIR/logs" 2>/dev/null | cut -f1)
    print_info "Local logs: $file_count files, $log_size"
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

# Verify logs directory exists before attempting compression
if [ ! -d "$LOG_OUTPUT_DIR/logs" ]; then
    print_error "Logs directory not found: $LOG_OUTPUT_DIR/logs"
    print_error "Log copy from pod may have failed"
    print_warning "Check if pod logs were copied successfully in Step 3"
    exit 1
fi

COMPRESSED_DIR="${OUTPUT_DIR}/compressed_${TIMESTAMP}"
mkdir -p "$COMPRESSED_DIR"

# Get list of compressed files
declare -a COMPRESSED_FILES
readarray -t COMPRESSED_FILES < <(compress_logs_by_namespace "$LOG_OUTPUT_DIR" "$COMPRESSED_DIR")

if [ ${#COMPRESSED_FILES[@]} -eq 0 ]; then
    print_error "No files were compressed"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    exit 1
fi

print_success "Created ${#COMPRESSED_FILES[@]} compressed file(s)"
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
for file_name in "${UPLOADED_FILES[@]}"; do
    file_exists=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
        "[ -f '$FILER_TARGET_PATH/$file_name' ] && echo 'yes' || echo 'no'" 2>/dev/null)
    
    if [ "$file_exists" = "yes" ]; then
        size=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" \
            "du -sh '$FILER_TARGET_PATH/$file_name' 2>/dev/null | cut -f1" 2>/dev/null || echo "unknown")
        print_success "  $file_name ($size)"
        VERIFY_SUCCESS=$((VERIFY_SUCCESS + 1))
    else
        print_error "  $file_name (not found)"
        VERIFY_FAILED=$((VERIFY_FAILED + 1))
    fi
done

if [ $VERIFY_FAILED -gt 0 ]; then
    print_error "Verification failed for $VERIFY_FAILED file(s)"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    print_warning "Compressed files at: $COMPRESSED_DIR"
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

# Final summary
print_header "✅ Success - All Operations Completed"
echo ""

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

print_header "Done"
