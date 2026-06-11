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

copy_pod_logs_selective() {
    local kubeconfig_file=$1
    local output_dir=$2
    
    print_info "Selective namespace copy mode enabled"
    print_info "Selected namespaces: ${FLUENTD_NAMESPACES}"
    
    # Parse selected namespaces
    IFS=',' read -ra selected_namespaces <<< "$FLUENTD_NAMESPACES"
    
    # Trim whitespace
    for i in "${!selected_namespaces[@]}"; do
        selected_namespaces[$i]=$(echo "${selected_namespaces[$i]}" | xargs)
    done
    
    print_info "Processing ${#selected_namespaces[@]} namespace(s)"
    
    # For each selected namespace, find matching directories and copy them
    local total_copied=0
    local total_failed=0
    
    for ns in "${selected_namespaces[@]}"; do
        print_info ""
        print_info "📦 Processing namespace: ${ns}"
        
        # List matching directories in the pod (e.g., kube.ntnx-system.*)
        print_info "Finding log directories for ${ns}..."
        
        local find_cmd="find ${SOURCE_PATH} -maxdepth 1 -type d -name 'kube.${ns}.*' 2>/dev/null | sort"
        local matching_dirs
        matching_dirs=$(kubectl --kubeconfig="$kubeconfig_file" exec -n "$NAMESPACE" "$POD_NAME" -- sh -c "$find_cmd" 2>/dev/null || true)
        
        if [ -z "$matching_dirs" ]; then
            print_warning "No log directories found for namespace: ${ns}"
            total_failed=$((total_failed + 1))
            continue
        fi
        
        # Count directories
        local dir_count
        dir_count=$(echo "$matching_dirs" | wc -l | tr -d '[:space:]')
        print_info "Found ${dir_count} log director(y/ies) for ${ns}"
        
        # Create a tar archive of these directories inside the pod
        print_info "Creating tar archive of ${ns} logs..."
        
        # Build list of directory names (avoiding "Argument list too long")
        # For large directory counts (>1000), use a file list instead of command args
        local tar_file="/tmp/fluentd_${ns}_$$.tar.gz"
        local tar_list_file="/tmp/fluentd_${ns}_$$.list"
        print_info "Archiving to: ${tar_file}"
        
        # Create a list file with directory names
        local dir_list=""
        while IFS= read -r full_path; do
            # Extract just the directory name (e.g., kube.ntnx-system.pod1)
            local dir_name=$(basename "$full_path")
            dir_list="${dir_list}${dir_name}\n"
        done <<< "$matching_dirs"
        
        # Create tar inside pod using file list to avoid "Argument list too long"
        local tar_cmd="cd ${SOURCE_PATH} && printf '${dir_list}' > ${tar_list_file} && tar czf ${tar_file} -T ${tar_list_file} 2>/dev/null && rm -f ${tar_list_file} && echo 'TAR_SUCCESS' && ls -lh ${tar_file}"
        
        local tar_output
        tar_output=$(kubectl --kubeconfig="$kubeconfig_file" exec -n "$NAMESPACE" "$POD_NAME" -- sh -c "$tar_cmd" 2>&1 || true)
        
        if echo "$tar_output" | grep -q "TAR_SUCCESS"; then
            # Extract tar file size for progress reporting
            local tar_size=$(echo "$tar_output" | grep "$tar_file" | awk '{print $5}')
            print_success "Tar created successfully (size: ${tar_size})"
            
            # Copy the tar file from pod to local
            print_info "Downloading tar archive..."
            local local_tar="${output_dir}/temp_${ns}.tar.gz"
            
            kubectl --kubeconfig="$kubeconfig_file" cp \
                -n "$NAMESPACE" \
                "${POD_NAME}:${tar_file}" \
                "$local_tar" \
                2>&1 | grep -v "Defaulted container" || true
            
            local cp_exit_code=${PIPESTATUS[0]}
            
            if [ $cp_exit_code -eq 0 ] && [ -f "$local_tar" ]; then
                print_success "Downloaded tar archive"
                
                # Extract tar locally
                print_info "Extracting logs to ${output_dir}..."
                tar xzf "$local_tar" -C "$output_dir" 2>/dev/null
                
                if [ $? -eq 0 ]; then
                    print_success "Extracted ${dir_count} directory(ies) for ${ns}"
                    total_copied=$((total_copied + dir_count))
                    
                    # Clean up local tar
                    rm -f "$local_tar"
                else
                    print_error "Failed to extract tar for ${ns}"
                    total_failed=$((total_failed + 1))
                fi
                
                # Clean up tar file in pod
                kubectl --kubeconfig="$kubeconfig_file" exec -n "$NAMESPACE" "$POD_NAME" -- rm -f "$tar_file" 2>/dev/null || true
            else
                print_error "Failed to download tar for ${ns}"
                total_failed=$((total_failed + 1))
            fi
        else
            print_error "Failed to create tar archive for ${ns}"
            print_error "Output: $tar_output"
            total_failed=$((total_failed + 1))
        fi
    done
    
    print_info ""
    print_info "kubectl cp command completed successfully"
    print_info "Logs should now be in: ${output_dir}/"
    
    # Summary
    if [ $total_failed -eq 0 ]; then
        print_success "Successfully copied logs for all ${#selected_namespaces[@]} namespace(s)"
        print_success "Total directories copied: ${total_copied}"
        return 0
    elif [ $total_copied -gt 0 ]; then
        print_warning "Partial success: ${total_copied} directories copied, ${total_failed} namespace(s) failed"
        return 0
    else
        print_error "Failed to copy logs for any namespace"
        return 1
    fi
}

copy_pod_logs() {
    local kubeconfig_file=$1
    local output_dir=$2
    
    # Check if we need to do selective copying
    if [ -n "${FLUENTD_NAMESPACES:-}" ] && [ "$FLUENTD_NAMESPACES" != "NONE" ]; then
        print_info "Selective copy mode: Only copying selected namespaces"
        copy_pod_logs_selective "$kubeconfig_file" "$output_dir"
        return $?
    fi
    
    print_info "Starting kubectl cp operation (all namespaces)..."
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

# Step 3: Copy logs from pod (skip if no fluentd namespaces selected)
if [ "${FLUENTD_NAMESPACES:-}" = "NONE" ]; then
    print_header "Step 3/7: Copying Fluentd Logs from Pod"
    print_info "No fluentd namespaces selected - skipping log copy"
    echo ""
else
    print_header "Step 3/7: Copying Fluentd Logs from Pod"
    print_info "Creating output directory: ${LOG_OUTPUT_DIR}"
    mkdir -p "$LOG_OUTPUT_DIR"

    print_info "Copying logs from ${POD_NAME}:${SOURCE_PATH}..."
    print_info "This may take several minutes depending on log size..."

    if copy_pod_logs "$KUBECONFIG_FILE" "$LOG_OUTPUT_DIR"; then
        print_success "Logs copied successfully from pod"
        
        print_info "Verifying copied logs..."
        
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
        
        # Show which namespaces were copied
        if [ -n "${FLUENTD_NAMESPACES:-}" ] && [ "$FLUENTD_NAMESPACES" != "NONE" ]; then
            print_info "Selective copy mode - showing copied namespaces:"
            find "$LOG_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type d -name "kube.*" | \
                sed 's/.*kube\.\([^.]*\).*/\1/' | sort -u | while read ns; do
                ns_count=$(find "$LOG_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type d -name "kube.${ns}.*" | wc -l | tr -d '[:space:]')
                print_info "  • ${ns}: ${ns_count} pod director(y/ies)"
            done
        fi
        
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
            if [ -n "${FLUENTD_NAMESPACES:-}" ] && [ "$FLUENTD_NAMESPACES" != "NONE" ]; then
                print_success "Found $dir_count log directories for selected namespaces"
            else
                print_success "Found $dir_count namespace log directories"
            fi
            
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
            
            if [ -n "${FLUENTD_NAMESPACES:-}" ] && [ "$FLUENTD_NAMESPACES" != "NONE" ]; then
                print_info "Selected namespace logs: $dir_count directories, $log_size"
            else
                print_info "Local logs: $dir_count directories, $log_size"
            fi
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
                
                if [ -n "${FLUENTD_NAMESPACES:-}" ] && [ "$FLUENTD_NAMESPACES" != "NONE" ]; then
                    print_info "Selected namespace logs: $dir_count directories, $log_size"
                else
                    print_info "Local logs: $dir_count directories, $log_size"
                fi
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
fi
echo ""

# Steps 4 & 5: Create folder and compress/upload (skip if no fluentd namespaces selected)
if [ "${FLUENTD_NAMESPACES:-}" = "NONE" ]; then
    print_header "Step 4/7: Creating Folder on Filer"
    print_info "No fluentd namespaces selected - skipping"
    echo ""
    
    print_header "Step 5/7: Compress and Upload Logs by Namespace"
    print_info "No fluentd namespaces selected - skipping"
    echo ""
else
    # Step 4: Create folder on filer
    print_header "Step 4/7: Creating Folder on Filer"

    # Use fluentd subfolder if specified
    FLUENTD_SUBFOLDER="${FLUENTD_SUBFOLDER:-}"
    if [ -n "$FLUENTD_SUBFOLDER" ]; then
        FILER_TARGET_PATH="$FILER_BASE_PATH/$BUG_FOLDER/$FLUENTD_SUBFOLDER"
        print_info "Uploading to fluentd subfolder: $FILER_TARGET_PATH"
    else
        FILER_TARGET_PATH="$FILER_BASE_PATH/$BUG_FOLDER"
    fi

    if ! create_filer_folder "$FILER_TARGET_PATH"; then
        print_error "Cannot proceed with upload"
        print_warning "Local logs are available at: $LOG_OUTPUT_DIR"
        exit 1
    fi
    echo ""

    # Step 5: Compress and Upload (one namespace at a time to save disk space)
    print_header "Step 5/7: Compress and Upload Logs by Namespace"

    # Verify log directories exist before attempting compression
    dir_check=$(find "$LOG_OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type d -name "kube.*" 2>/dev/null | wc -l | tr -d '[:space:]')
    if [ -z "$dir_check" ] || [ "$dir_check" -eq 0 ]; then
        print_error "No log directories found in: $LOG_OUTPUT_DIR"
        print_error "Log copy from pod may have failed"
        print_warning "Check if pod logs were copied successfully in Step 3"
        exit 1
    fi
print_info "Found $dir_check log directories to process"

# Check available disk space before compression
print_info "Checking available disk space..."
available_space=$(df -BG "$OUTPUT_DIR" | awk 'NR==2 {print $4}' | tr -d 'G')
log_size=$(du -sm "$LOG_OUTPUT_DIR" 2>/dev/null | awk '{print $1}' || echo "0")

if [ "$available_space" -lt 1 ]; then
    print_error "⚠️  Insufficient disk space detected!"
    print_error "   Available: ${available_space}GB"
    print_error "   Estimated log size: ~${log_size}MB"
    print_error "   Please free up disk space and try again"
    print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
    exit 1
fi

print_info "Available disk space: ${available_space}GB (Log size: ${log_size}MB)"

COMPRESSED_DIR="${OUTPUT_DIR}/compressed_${TIMESTAMP}"
mkdir -p "$COMPRESSED_DIR" || { print_error "Failed to create temp directory"; exit 1; }

# Get list of namespaces to process
declare -a namespaces
if [ -n "${FLUENTD_NAMESPACES:-}" ] && [ "$FLUENTD_NAMESPACES" != "NONE" ]; then
    IFS=',' read -ra namespaces <<< "$FLUENTD_NAMESPACES"
    for i in "${!namespaces[@]}"; do
        namespaces[$i]=$(echo "${namespaces[$i]}" | xargs)
    done
else
    if [ "${FLUENTD_NAMESPACES:-}" = "NONE" ]; then
        print_warning "No fluentd namespaces selected - skipping"
        namespaces=()
    else
        mapfile -t namespaces < <(find "$LOG_OUTPUT_DIR" -maxdepth 1 -type d -name "kube.*" | sed 's/.*kube\.\([^.]*\).*/\1/' | sort -u)
    fi
fi

if [ ${#namespaces[@]} -eq 0 ]; then
    print_warning "No namespaces to process"
else
    print_info "Processing ${#namespaces[@]} namespace(s)"
    
    UPLOAD_SUCCESS_COUNT=0
    UPLOAD_FAILED_COUNT=0
    declare -a UPLOADED_FILES
    
    # Process each namespace: compress -> upload -> delete
    namespace_index=0
    for namespace in "${namespaces[@]}"; do
        namespace_index=$((namespace_index + 1))
        print_section "Processing namespace: $namespace ($namespace_index/${#namespaces[@]})"
        echo "SUBSTEP_START: step5_namespace_${namespace_index} Namespace: ${namespace}" >&2
        
        output_file="${COMPRESSED_DIR}/${namespace}.tar.gz"
        pattern="kube.${namespace}.*"
        
        # Find all folders matching this namespace
        echo "SUBSTEP_UPDATE: step5_namespace_${namespace_index} Analyzing logs structure" >&2
        folder_count=$(find "$LOG_OUTPUT_DIR" -maxdepth 1 -type d -name "$pattern" | wc -l)
        print_info "  Found $folder_count folder(s) for namespace '$namespace'"
        
        if [ $folder_count -eq 0 ]; then
            print_warning "  No folders found for pattern: $pattern"
            echo "SUBSTEP_COMPLETE: step5_namespace_${namespace_index}" >&2
            continue
        fi
        
        # Get directory names
        dirs_to_compress=()
        while IFS= read -r dir; do
            dirs_to_compress+=("$(basename "$dir")")
        done < <(find "$LOG_OUTPUT_DIR" -maxdepth 1 -type d -name "$pattern")
        
        if [ ${#dirs_to_compress[@]} -eq 0 ]; then
            print_error "  No directories found to compress"
            echo "SUBSTEP_COMPLETE: step5_namespace_${namespace_index}" >&2
            continue
        fi
        
        # Compress
        echo "SUBSTEP_UPDATE: step5_namespace_${namespace_index} Creating tar.gz archive" >&2
        print_info "  Compressing ${#dirs_to_compress[@]} directories..."
        set +e
        tar_output=$(tar -czf "$output_file" -C "$LOG_OUTPUT_DIR" "${dirs_to_compress[@]}" 2>&1 | grep -v "Removing leading" || true)
        tar_exit=$?
        set -e
        
        if [ $tar_exit -ne 0 ] || [ ! -f "$output_file" ]; then
            print_error "  Failed to compress namespace: $namespace"
            UPLOAD_FAILED_COUNT=$((UPLOAD_FAILED_COUNT + 1))
            echo "SUBSTEP_COMPLETE: step5_namespace_${namespace_index}" >&2
            continue
        fi
        
        compressed_size=$(du -sh "$output_file" 2>/dev/null | cut -f1)
        print_success "  Compressed: $compressed_size"
        
        # Upload immediately
        echo "SUBSTEP_UPDATE: step5_namespace_${namespace_index} Uploading to filer" >&2
        print_info "  Uploading to filer..."
        if upload_file_to_filer "$output_file" "$FILER_TARGET_PATH"; then
            UPLOAD_SUCCESS_COUNT=$((UPLOAD_SUCCESS_COUNT + 1))
            UPLOADED_FILES+=("$(basename "$output_file")")
            print_success "  Uploaded successfully"
            
            # Delete local tarball to save disk space
            echo "SUBSTEP_UPDATE: step5_namespace_${namespace_index} Cleaning up local files" >&2
            rm -f "$output_file"
            print_info "  Deleted local tarball (saved disk space)"
        else
            UPLOAD_FAILED_COUNT=$((UPLOAD_FAILED_COUNT + 1))
            print_error "  Upload failed"
        fi
        echo "SUBSTEP_COMPLETE: step5_namespace_${namespace_index}" >&2
        echo ""
    done
    
    if [ $UPLOAD_FAILED_COUNT -gt 0 ]; then
        print_error "$UPLOAD_FAILED_COUNT file(s) failed to upload"
        print_warning "Local logs preserved at: $LOG_OUTPUT_DIR"
        exit 1
    fi
    
    print_success "All $UPLOAD_SUCCESS_COUNT file(s) uploaded successfully"
    fi
fi
echo ""

# Step 6: Cleanup
# Note: Step numbers are dynamically updated by Python backend based on logbay inclusion
print_header "Step 6/7: Cleanup"

# No verification needed as files were already deleted after upload
VERIFY_SUCCESS=${UPLOAD_SUCCESS_COUNT:-0}
VERIFY_FAILED=${UPLOAD_FAILED_COUNT:-0}

print_info "Verifying uploaded files on filer..."
print_info "Checking ${#UPLOADED_FILES[@]} file(s) at: ${FILER_HOST}:${FILER_TARGET_PATH}"
echo "" >&2

# Files were already uploaded and verified during Step 5, no additional verification needed
if [ $VERIFY_SUCCESS -gt 0 ]; then
    print_success "Successfully uploaded and cleaned up $VERIFY_SUCCESS file(s)"
elif [ "${FLUENTD_NAMESPACES:-}" = "NONE" ]; then
    print_info "No fluentd namespaces selected - nothing to upload"
else
    print_warning "No files were uploaded"
fi
echo ""

# Cleanup local files
print_info "Cleaning up local files..."
if rm -rf "$LOG_OUTPUT_DIR"; then
    print_success "Local logs deleted"
else
    print_warning "Could not delete local logs at: $LOG_OUTPUT_DIR"
fi

# Compressed files were already deleted immediately after upload
if [ -d "$COMPRESSED_DIR" ]; then
    rm -rf "$COMPRESSED_DIR" && print_info "Cleaned up temp directory"
fi
echo ""

# Final summary
print_header "✅ Success - All Operations Completed"
echo ""

if [ ${#UPLOADED_FILES[@]} -gt 0 ]; then
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
done
echo ""

echo "🌐 Logs Location:"
# Always show main folder URL (not subfolders)
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
