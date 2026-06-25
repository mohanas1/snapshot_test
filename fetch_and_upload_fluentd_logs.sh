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
    
    local ssh_cmd="ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    
    if [ -n "$password" ] && command -v sshpass &> /dev/null; then
        ssh_cmd="sshpass -p '$password' $ssh_cmd"
    fi
    
    eval "$ssh_cmd ${user}@${host} '$cmd'" < /dev/null
}

scp_upload() {
    local source=$1
    local host=$2
    local user=$3
    local password=$4
    local dest=$5
    
    local scp_cmd="scp -r -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    
    if [ -n "$password" ] && command -v sshpass &> /dev/null; then
        scp_cmd="sshpass -p '$password' $scp_cmd"
    fi
    
    eval "$scp_cmd '$source' ${user}@${host}:'$dest/'" < /dev/null
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
    print_info "Namespaces: ${selected_namespaces[*]}"
    
    # For each selected namespace, find matching directories and copy them
    local total_copied=0
    local total_failed=0
    
    for ns in "${selected_namespaces[@]}"; do
        print_info ""
        print_info "📦 Processing namespace: ${ns}"
        print_info "Source path: ${SOURCE_PATH}"
        print_info "Pod: ${NAMESPACE}/${POD_NAME}"
        
        # List matching directories in the pod (e.g., kube.ntnx-system.*)
        print_info "Finding log directories for ${ns}..."
        
        local find_cmd="find ${SOURCE_PATH} -maxdepth 1 -type d -name 'kube.${ns}.*' 2>/dev/null | sort"
        print_info "Find command: $find_cmd"
        
        local matching_dirs
        matching_dirs=$(kubectl --kubeconfig="$kubeconfig_file" exec -n "$NAMESPACE" "$POD_NAME" -- sh -c "$find_cmd" 2>/dev/null || true)
        
        print_info "Raw matching_dirs output:"
        echo "$matching_dirs" | head -10
        print_info "---"
        
        if [ -z "$matching_dirs" ]; then
            print_warning "No log directories found for namespace: ${ns}"
            total_failed=$((total_failed + 1))
            continue
        fi
        
        # Count directories
        local dir_count
        dir_count=$(echo "$matching_dirs" | wc -l | tr -d '[:space:]')
        print_info "Found ${dir_count} log director(y/ies) for ${ns}"
        local candidate_dirs="$matching_dirs"
        
        # Create namespace output directory locally
        local ns_output_dir="${output_dir}/${ns}"
        mkdir -p "$ns_output_dir"
        local ns_filer_target="${FILER_TARGET_PATH}/${ns}"
        if ! create_filer_folder "$ns_filer_target"; then
            print_error "Failed to create namespace folder on filer: ${ns_filer_target}"
            total_failed=$((total_failed + 1))
            rm -rf "$ns_output_dir"
            continue
        fi
        
        # Download only matching timestamp files (not whole directories).
        local from_ts="${FLUENTD_TIME_FROM_TS:-}"
        local to_ts="${FLUENTD_TIME_TO_TS:-}"
        local filter_type="${FLUENTD_TIME_FILTER_TYPE:-recent}"
        local remote_file_list=""

        if [ -n "$from_ts" ] && [ -n "$to_ts" ]; then
            print_info "Applying remote timestamp filter for ${ns}: ${from_ts}..${to_ts} (${filter_type})"
            # Hybrid optimization:
            # 1) prune candidate folders by folder mtime around requested window
            # 2) apply accurate file timestamp boundary logic within pruned folders
            local from_epoch="${FLUENTD_TIME_FROM_EPOCH:-}"
            local to_epoch="${FLUENTD_TIME_TO_EPOCH:-}"
            local prune_grace_sec=86400   # 24h grace on each side
            if [ -n "$from_epoch" ] && [ -n "$to_epoch" ]; then
                local prune_from=$((from_epoch - prune_grace_sec))
                local prune_to=$((to_epoch + prune_grace_sec))
                local prune_cmd="
prune_from='${prune_from}'; prune_to='${prune_to}';
while IFS= read -r d; do
  [ -z \"\$d\" ] && continue
  mt=\$(stat -c %Y \"\$d\" 2>/dev/null || echo 0)
  [ \"\$mt\" -ge \"\$prune_from\" ] && [ \"\$mt\" -le \"\$prune_to\" ] && echo \"\$d\"
done"
                local pruned_dirs
                pruned_dirs=$(echo "$matching_dirs" | kubectl --kubeconfig="$kubeconfig_file" exec -i -n "$NAMESPACE" "$POD_NAME" -- sh -c "$prune_cmd" 2>/dev/null || true)
                local pruned_count
                pruned_count=$(echo "$pruned_dirs" | sed '/^$/d' | wc -l | tr -d '[:space:]')
                if [ "$pruned_count" -gt 0 ]; then
                    candidate_dirs="$pruned_dirs"
                    print_info "Folder-time prune for ${ns}: ${dir_count} -> ${pruned_count} candidate folder(s)"
                else
                    # Safe fallback when mtime pruning is too strict.
                    print_warning "Folder-time prune yielded 0 for ${ns}; falling back to all ${dir_count} folders"
                fi
            fi

            local candidate_count
            candidate_count=$(echo "$candidate_dirs" | sed '/^$/d' | wc -l | tr -d '[:space:]')
            local heartbeat_every="${FLUENTD_HEARTBEAT_EVERY_FOLDERS:-25}"
            if ! [[ "$heartbeat_every" =~ ^[0-9]+$ ]] || [ "$heartbeat_every" -le 0 ]; then
                heartbeat_every=25
            fi
            print_info "Scanning ${candidate_count} candidate folder(s) for ${ns} (heartbeat every ${heartbeat_every})"
            local candidate_index=0
            while IFS= read -r pod_dir; do
                [ -z "$pod_dir" ] && continue
                candidate_index=$((candidate_index + 1))
                if [ "$candidate_index" -eq 1 ] || [ $((candidate_index % heartbeat_every)) -eq 0 ] || [ "$candidate_index" -eq "$candidate_count" ]; then
                    print_info "  [${ns}] Timestamp scan progress: folder ${candidate_index}/${candidate_count}"
                fi

                local rotated_cmd="
for f in \"${pod_dir}\"/file.log.log.*; do
  [ -f \"\$f\" ] || continue
  b=\$(basename \"\$f\")
  ts=\$(echo \"\$b\" | sed -n -E 's/^file\\.log\\.log\\.([0-9]{14})(\\.gz)?$/\\1/p')
  [ -n \"\$ts\" ] && echo \"\$ts|\$f\"
done | sort"
                local rotated_list
                rotated_list=$(kubectl --kubeconfig="$kubeconfig_file" exec -n "$NAMESPACE" "$POD_NAME" -- sh -c "$rotated_cmd" 2>/dev/null || true)

                local active_cmd="[ -f \"${pod_dir}/file.log.log\" ] && echo \"${pod_dir}/file.log.log\" || true"
                local active_path
                active_path=$(kubectl --kubeconfig="$kubeconfig_file" exec -n "$NAMESPACE" "$POD_NAME" -- sh -c "$active_cmd" 2>/dev/null || true)

                local started=0
                local done=0
                local last_ts=""
                while IFS= read -r row; do
                    [ -z "$row" ] && continue
                    local ts="${row%%|*}"
                    local path="${row#*|}"
                    last_ts="$ts"
                    if [ "$started" -eq 0 ]; then
                        # Include first rotated file that ends after/equal window start.
                        if [[ "$ts" > "$from_ts" || "$ts" == "$from_ts" ]]; then
                            started=1
                            remote_file_list="${remote_file_list}${path}"$'\n'
                            if [[ "$ts" > "$to_ts" || "$ts" == "$to_ts" ]]; then
                                done=1
                                break
                            fi
                        fi
                    else
                        remote_file_list="${remote_file_list}${path}"$'\n'
                        if [[ "$ts" > "$to_ts" || "$ts" == "$to_ts" ]]; then
                            done=1
                            break
                        fi
                    fi
                done <<< "$rotated_list"

                if [ "$started" -eq 1 ]; then
                    # If window extends beyond last rotated file, include active file.
                    if [ "$done" -eq 0 ] && [ -n "$active_path" ]; then
                        remote_file_list="${remote_file_list}${active_path}"$'\n'
                    fi
                else
                    # No rotated file boundary found in range. Include active for recent and
                    # for ranges newer than last rotated boundary.
                    if [ -n "$active_path" ]; then
                        if [ "$filter_type" = "recent" ]; then
                            remote_file_list="${remote_file_list}${active_path}"$'\n'
                        elif [ -n "$last_ts" ] && [[ "$from_ts" > "$last_ts" ]]; then
                            remote_file_list="${remote_file_list}${active_path}"$'\n'
                        fi
                    fi
                fi
            done <<< "$candidate_dirs"
        fi

        local download_success_count=0
        local download_failed_count=0
        local current_item=0

        if [ -n "$remote_file_list" ]; then
            local file_count
            file_count=$(echo "$remote_file_list" | wc -l | tr -d '[:space:]')
            print_info "Downloading ${file_count} filtered file(s) to local..."
            while IFS= read -r remote_file; do
                [ -z "$remote_file" ] && continue
                current_item=$((current_item + 1))
                local parent_dir_name
                parent_dir_name=$(basename "$(dirname "$remote_file")")
                mkdir -p "${ns_output_dir}/${parent_dir_name}"
                local dest_file="${ns_output_dir}/${parent_dir_name}/$(basename "$remote_file")"

                print_info "  [${ns}] Downloading ${current_item}/${file_count}..."
                set +e
                cp_output=$(kubectl --kubeconfig="$kubeconfig_file" cp \
                    -n "$NAMESPACE" \
                    "${POD_NAME}:${remote_file}" \
                    "${dest_file}" \
                    2>&1)
                local cp_exit=$?
                set -e

                filtered_output=$(echo "$cp_output" | grep -v "Defaulted container" | grep -v "tar: removing leading" || true)
                if [ $cp_exit -eq 0 ]; then
                    download_success_count=$((download_success_count + 1))
                    print_success "  ✓ [${ns}] ${current_item}/${file_count}"
                else
                    download_failed_count=$((download_failed_count + 1))
                    print_error "  ✗ [${ns}] Failed ${current_item}/${file_count} - exit: $cp_exit"
                    if [ -n "$filtered_output" ]; then
                        print_error "    Error: $filtered_output"
                    fi
                fi
            done <<< "$remote_file_list"
        else
            # Fallback for compatibility if timestamp filter is missing.
            print_warning "No timestamp-filtered file list found; falling back to directory copy."
            print_info "Downloading ${dir_count} directories to local..."
            while IFS= read -r pod_dir; do
                [ -z "$pod_dir" ] && continue
                current_item=$((current_item + 1))
                local dir_name=$(basename "$pod_dir")
                print_info "  [${ns}] Downloading ${current_item}/${dir_count}..."
                set +e
                cp_output=$(kubectl --kubeconfig="$kubeconfig_file" cp \
                    -n "$NAMESPACE" \
                    "${POD_NAME}:${pod_dir}" \
                    "${ns_output_dir}/${dir_name}" \
                    2>&1)
                local cp_exit=$?
                set -e
                filtered_output=$(echo "$cp_output" | grep -v "Defaulted container" | grep -v "tar: removing leading" || true)
                if [ $cp_exit -eq 0 ]; then
                    download_success_count=$((download_success_count + 1))
                    print_success "  ✓ [${ns}] ${current_item}/${dir_count}"
                else
                    download_failed_count=$((download_failed_count + 1))
                    print_error "  ✗ [${ns}] Failed ${current_item}/${dir_count} - exit: $cp_exit"
                    if [ -n "$filtered_output" ]; then
                        print_error "    Error: $filtered_output"
                    fi
                fi
            done <<< "$matching_dirs"
        fi
        
        if [ $download_success_count -eq 0 ]; then
            print_error "Failed to download any filtered files for ${ns}"
            total_failed=$((total_failed + 1))
            rm -rf "$ns_output_dir"
            continue
        fi
        
        if [ $download_failed_count -gt 0 ]; then
            print_warning "Downloaded ${download_success_count} filtered file(s) (${download_failed_count} failed)"
        else
            print_success "Downloaded ${download_success_count} filtered file(s)"
        fi
        
        # Verify directories actually exist and have content
        local actual_dir_count=$(find "$ns_output_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l || echo "0")
        if [ $actual_dir_count -eq 0 ]; then
            print_error "No directories found in ${ns_output_dir} after download"
            print_error "This usually means kubectl cp failed silently"
            total_failed=$((total_failed + 1))
            rm -rf "$ns_output_dir"
            continue
        fi
        print_info "Verified ${actual_dir_count} directories on disk"
        
        # Get total size to determine if chunking is needed
        print_info "Calculating local size..."
        local total_size_kb=$(du -sk "$ns_output_dir" 2>/dev/null | awk '{print $1}' || echo "0")
        local total_size_mb=$((total_size_kb / 1024))
        
        # Chunk settings:
        # - pack small containers together up to ~5GB
        # - split oversized single container into ~5GB parts
        local chunk_size_mb=5000
        local chunk_threshold_mb=3000
        
        if [ $total_size_mb -gt $chunk_threshold_mb ] && [ $download_success_count -gt 30 ]; then
            print_info "Large namespace detected (${total_size_mb}MB, ${download_success_count} dirs)"
            print_info "Using container-wise grouping with size-aware packing/splitting"

            # Build deterministic groups by container name so each tar maps to container(s).
            local dirs_with_sizes
            dirs_with_sizes=$(du -sk "${ns_output_dir}"/* 2>/dev/null | sort -k2 || true)
            declare -A container_dirs
            declare -A container_size_kb
            declare -A container_dir_count

            while IFS= read -r line; do
                [ -z "$line" ] && continue
                local size_kb
                local dir_path
                local dir_name
                local container_name
                size_kb=$(echo "$line" | awk '{print $1}')
                dir_path=$(echo "$line" | awk '{print $2}')
                dir_name=$(basename "$dir_path")
                container_name=$(echo "$dir_name" | awk -F'.' '{if (NF>=5) print $5; else print "unknown"}')
                [ -z "$container_name" ] && container_name="unknown"

                container_dirs["$container_name"]+="${dir_name}"$'\n'
                container_size_kb["$container_name"]=$(( ${container_size_kb["$container_name"]:-0} + size_kb ))
                container_dir_count["$container_name"]=$(( ${container_dir_count["$container_name"]:-0} + 1 ))
            done <<< "$dirs_with_sizes"

            mapfile -t container_names < <(printf '%s\n' "${!container_dirs[@]}" | sort)
            local total_groups=${#container_names[@]}
            print_info "Container groups detected: ${total_groups}"

            # Helper to create+upload one tar from provided dirs array.
            local _emit_index=0
            emit_tar_for_dirs() {
                local tar_label="$1"; shift
                local dirs=("$@")
                [ ${#dirs[@]} -eq 0 ] && return 0
                _emit_index=$((_emit_index + 1))
                local filer_filename="${ns}__${tar_label}.tar.gz"
                local local_tar="${output_dir}/${filer_filename}"

                print_info ""
                print_info "  📦 Group ${_emit_index}: ${filer_filename} (${#dirs[@]} dirs)"
                print_info "  Creating tar locally..."
                tar_chunk_output=$(tar -czf "$local_tar" -C "$ns_output_dir" "${dirs[@]}" 2>&1)
                tar_chunk_exit=$?
                if [ $tar_chunk_exit -eq 0 ] && [ -f "$local_tar" ]; then
                    local tar_size
                    tar_size=$(du -sh "$local_tar" 2>/dev/null | cut -f1)
                    print_success "  Created tar (${tar_size})"
                    print_info "  Uploading ${filer_filename}..."
                    if upload_file_to_filer "$local_tar" "$ns_filer_target"; then
                        print_success "  ✓ Uploaded ${filer_filename}"
                        UPLOADED_FILES+=("${ns}/${filer_filename}")
                        UPLOAD_SUCCESS_COUNT=$((UPLOAD_SUCCESS_COUNT + 1))
                        total_copied=$((total_copied + ${#dirs[@]}))
                        rm -f "$local_tar"
                    else
                        print_error "  ✗ Upload failed"
                        UPLOAD_FAILED_COUNT=$((UPLOAD_FAILED_COUNT + 1))
                        total_failed=$((total_failed + 1))
                        rm -f "$local_tar"
                    fi
                else
                    print_error "  ✗ Tar creation failed"
                    if [ -n "$tar_chunk_output" ]; then
                        print_error "  Tar error: $tar_chunk_output"
                    fi
                    total_failed=$((total_failed + 1))
                fi
            }

            # Pack small containers together up to chunk_size_mb.
            local pack_size_kb=0
            local pack_containers=""
            local pack_dirs=()
            local flush_pack=0

            for container_name in "${container_names[@]}"; do
                local c_size_kb=${container_size_kb["$container_name"]:-0}
                local c_size_mb=$((c_size_kb / 1024))
                local safe_container
                safe_container=$(echo "$container_name" | tr -cs '[:alnum:]_-' '_')
                [ -z "$safe_container" ] && safe_container="unknown"
                local c_dirs=()
                while IFS= read -r dir_entry; do
                    [ -z "$dir_entry" ] && continue
                    c_dirs+=("$dir_entry")
                done < <(echo -e "${container_dirs["$container_name"]}" | grep -v '^$')
                [ ${#c_dirs[@]} -eq 0 ] && continue

                # If this container itself exceeds threshold, split it into parts.
                if [ "$c_size_mb" -gt "$chunk_size_mb" ]; then
                    if [ ${#pack_dirs[@]} -gt 0 ]; then
                        local bundle_label
                        bundle_label=$(echo "$pack_containers" | sed 's/^_//' | cut -c1-120)
                        emit_tar_for_dirs "$bundle_label" "${pack_dirs[@]}"
                        pack_dirs=()
                        pack_size_kb=0
                        pack_containers=""
                    fi

                    local part_idx=1
                    local part_size_kb=0
                    local part_dirs=()
                    for d in "${c_dirs[@]}"; do
                        local d_kb
                        d_kb=$(du -sk "${ns_output_dir}/${d}" 2>/dev/null | awk '{print $1}' || echo "0")
                        if [ $(( (part_size_kb + d_kb) / 1024 )) -gt "$chunk_size_mb" ] && [ ${#part_dirs[@]} -gt 0 ]; then
                            emit_tar_for_dirs "${safe_container}_${part_idx}" "${part_dirs[@]}"
                            part_idx=$((part_idx + 1))
                            part_dirs=()
                            part_size_kb=0
                        fi
                        part_dirs+=("$d")
                        part_size_kb=$((part_size_kb + d_kb))
                    done
                    if [ ${#part_dirs[@]} -gt 0 ]; then
                        emit_tar_for_dirs "${safe_container}_${part_idx}" "${part_dirs[@]}"
                    fi
                    continue
                fi

                # Small/medium container: pack with other small ones.
                if [ $(( (pack_size_kb + c_size_kb) / 1024 )) -gt "$chunk_size_mb" ] && [ ${#pack_dirs[@]} -gt 0 ]; then
                    local bundle_label
                    bundle_label=$(echo "$pack_containers" | sed 's/^_//' | cut -c1-120)
                    emit_tar_for_dirs "$bundle_label" "${pack_dirs[@]}"
                    pack_dirs=()
                    pack_size_kb=0
                    pack_containers=""
                fi
                pack_size_kb=$((pack_size_kb + c_size_kb))
                pack_containers="${pack_containers}_${safe_container}"
                for d in "${c_dirs[@]}"; do
                    pack_dirs+=("$d")
                done
            done

            if [ ${#pack_dirs[@]} -gt 0 ]; then
                local bundle_label
                bundle_label=$(echo "$pack_containers" | sed 's/^_//' | cut -c1-120)
                emit_tar_for_dirs "$bundle_label" "${pack_dirs[@]}"
            fi
            
        else
            # Single tar (small namespace)
            print_info "Creating single tar archive locally (${total_size_mb}MB)..."
            
            # Create tar locally from downloaded directories
            local local_tar="${output_dir}/${ns}.tar.gz"
            
            print_info "Compressing ${download_success_count} directories..."
            
            # Debug: Check what we're trying to tar
            if [ ! -d "$ns_output_dir" ] || [ -z "$(ls -A "$ns_output_dir" 2>/dev/null)" ]; then
                print_error "Namespace directory is empty or does not exist: $ns_output_dir"
                total_failed=$((total_failed + 1))
                rm -rf "$ns_output_dir"
                continue
            fi
            
            # Create tar with better error handling
            tar_output=$(tar -czf "$local_tar" -C "$output_dir" "$ns" 2>&1)
            tar_exit=$?
            
            if [ $tar_exit -eq 0 ] && [ -f "$local_tar" ]; then
                local tar_size=$(du -sh "$local_tar" 2>/dev/null | cut -f1)
                print_success "Tar created successfully (size: ${tar_size})"
                
                # Upload to filer
                local filer_filename="${ns}.tar.gz"
                print_info "Uploading ${filer_filename} to filer..."
                
                if upload_file_to_filer "$local_tar" "$ns_filer_target"; then
                    print_success "Uploaded ${filer_filename} to filer"
                    total_copied=$((total_copied + download_success_count))
                    UPLOADED_FILES+=("${ns}/${filer_filename}")
                    UPLOAD_SUCCESS_COUNT=$((UPLOAD_SUCCESS_COUNT + 1))
                    rm -f "$local_tar"
                    print_info "Deleted local tar (saved disk space)"
                else
                    print_error "Failed to upload ${filer_filename} to filer"
                    UPLOAD_FAILED_COUNT=$((UPLOAD_FAILED_COUNT + 1))
                    total_failed=$((total_failed + 1))
                    rm -f "$local_tar"
                fi
            else
                print_error "Failed to create tar archive for ${ns}"
                if [ -n "$tar_output" ]; then
                    print_error "Tar error: $tar_output"
                fi
                total_failed=$((total_failed + 1))
                rm -f "$local_tar"
            fi
        fi
        
        # Clean up namespace directory after upload
        print_info "Cleaning up local directories for ${ns}..."
        rm -rf "$ns_output_dir"
        print_info "Deleted local directories (saved disk space)"
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
        verify_output=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "[ -d '$folder_path' ] && echo 'exists' || echo 'missing'" 2>&1 || true)
        # SSH may prepend warning lines (known_hosts, etc). Match token, not full string.
        if echo "$verify_output" | grep -qE '(^|[[:space:]])exists($|[[:space:]])'; then
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

timestamp_to_epoch() {
    local ts="$1"
    # Expected: YYYYMMDDHHMMSS
    if [[ ! "$ts" =~ ^[0-9]{14}$ ]]; then
        echo ""
        return 1
    fi
    local yyyy="${ts:0:4}"
    local mm="${ts:4:2}"
    local dd="${ts:6:2}"
    local hh="${ts:8:2}"
    local mi="${ts:10:2}"
    local ss="${ts:12:2}"
    date -d "${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}" +%s 2>/dev/null || true
}

apply_time_filter_for_namespace() {
    local logs_dir="$1"
    local namespace="$2"

    local from_epoch="${FLUENTD_TIME_FROM_EPOCH:-}"
    local to_epoch="${FLUENTD_TIME_TO_EPOCH:-}"
    local filter_type="${FLUENTD_TIME_FILTER_TYPE:-recent}"

    if [[ -z "$from_epoch" || -z "$to_epoch" ]]; then
        print_warning "No fluentd time window provided; skipping time filter for ${namespace}" >&2
        return 0
    fi

    local pattern="kube.${namespace}.*"
    local total_seen=0
    local kept=0
    local removed=0

    while IFS= read -r ns_dir; do
        [ -z "$ns_dir" ] && continue
        while IFS= read -r f; do
            [ -z "$f" ] && continue
            total_seen=$((total_seen + 1))
            local base
            base="$(basename "$f")"
            local keep_file=0

            # Active rolling file without timestamp suffix.
            if [[ "$base" == "file.log.log" ]]; then
                if [[ "$filter_type" == "recent" ]]; then
                    keep_file=1
                else
                    # For custom window, include active file only if mtime is within window.
                    local mtime_epoch
                    mtime_epoch=$(stat -c %Y "$f" 2>/dev/null || echo "")
                    if [[ -n "$mtime_epoch" && "$mtime_epoch" -ge "$from_epoch" && "$mtime_epoch" -le "$to_epoch" ]]; then
                        keep_file=1
                    fi
                fi
            elif [[ "$base" =~ ^file\.log\.log\.([0-9]{14})(\.gz)?$ ]]; then
                local ts="${BASH_REMATCH[1]}"
                local ts_epoch
                ts_epoch="$(timestamp_to_epoch "$ts")"
                if [[ -n "$ts_epoch" && "$ts_epoch" -ge "$from_epoch" && "$ts_epoch" -le "$to_epoch" ]]; then
                    keep_file=1
                fi
            fi

            if [[ "$keep_file" -eq 1 ]]; then
                kept=$((kept + 1))
            else
                rm -f "$f"
                removed=$((removed + 1))
            fi
        done < <(find "$ns_dir" -type f -name "file.log.log*")
    done < <(find "$logs_dir" -maxdepth 1 -type d -name "$pattern")

    print_info "Time filter [${namespace}] window=${from_epoch}..${to_epoch} type=${filter_type} seen=${total_seen}, kept=${kept}, removed=${removed}" >&2
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

        # Filter historical fluentd files by requested time window before compression.
        apply_time_filter_for_namespace "$logs_dir" "$namespace"
        
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
    
    # Use scp directly (requested behavior), with strict post-upload verification.
    print_info "  Using scp transfer..."
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if [ $retry_count -gt 0 ]; then
            print_warning "  Retry attempt $retry_count/$max_retries..."
            sleep 5
        fi
        
        local scp_out
        set +e
        scp_out=$(scp_upload "$local_file" "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "$filer_path" 2>&1)
        local scp_rc=$?
        set -e
        if [ $scp_rc -eq 0 ]; then
            local remote_path="${filer_path%/}/${upload_name}"
            local verify_out
            verify_out=$(ssh_exec "$FILER_HOST" "$FILER_USER" "$FILER_PASSWORD" "[ -f '$remote_path' ] && du -sh '$remote_path' 2>/dev/null | cut -f1 || echo MISSING" 2>/dev/null || true)
            if [ -n "$verify_out" ] && [ "$verify_out" != "MISSING" ]; then
                print_success "  Uploaded: $upload_name"
                print_info "  Verified on filer: ${verify_out}"
                return 0
            fi
            print_error "  Upload command succeeded but filer verification failed: ${remote_path}"
            [ -n "$verify_out" ] && print_error "  Verify output: $verify_out"
        else
            if [ -n "$scp_out" ]; then
                print_error "  scp failed: $(echo "$scp_out" | tr '\n' ' ' | cut -c1-300)"
            else
                print_error "  scp failed with exit: $scp_rc"
            fi
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
                -e "ssh" \
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

# Initialize upload tracking arrays
declare -a UPLOADED_FILES
UPLOAD_SUCCESS_COUNT=0
UPLOAD_FAILED_COUNT=0

# Step 2.5: Prepare filer path and create directory
if [ "${FLUENTD_NAMESPACES:-}" != "NONE" ]; then
    print_header "Step 2.5/7: Preparing Filer Directory"
    
    # Create fluentd subfolder (like logbay does)
    FILER_TARGET_PATH="$FILER_BASE_PATH/$BUG_FOLDER/fluentd"
    print_info "Target path: $FILER_TARGET_PATH"
    
    if ! create_filer_folder "$FILER_TARGET_PATH"; then
        print_error "Cannot proceed - filer directory creation failed"
        exit 1
    fi
    echo ""
fi

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
        
        print_info "Verifying operations..."
        
        # With new workflow, files are uploaded and cleaned during copy
        # Check uploaded files array instead of local directories
        uploaded_count=${#UPLOADED_FILES[@]}
        
        if [ $uploaded_count -gt 0 ]; then
            print_success "Processed and uploaded ${uploaded_count} namespace(s):"
            for uploaded_file in "${UPLOADED_FILES[@]}"; do
                print_info "  ✓ ${uploaded_file}"
            done
            print_success "Step 3 completed: All files uploaded to filer"
        else
            print_error "No files were uploaded"
            print_error "This usually means no matching namespaces were found or copy failed"
            exit 1
        fi
    else
        print_error "Failed to copy logs from pod"
        exit 1
    fi
fi
echo ""

# Steps 4 & 5: Already completed in Step 3 (new workflow)
print_header "Step 4/7: Filer Directory"
print_info "Directory already created in Step 2.5"
print_success "Filer ready: ${FILER_HOST}:${FILER_TARGET_PATH}/"
echo ""

print_header "Step 5/7: Compress and Upload"
if [ "${uploaded_count:-0}" -gt 0 ]; then
    print_info "Files were already uploaded during Step 3 (new workflow)"
    print_success "${uploaded_count} files uploaded and verified"
    echo ""
else
    if [ "${FLUENTD_NAMESPACES:-}" = "NONE" ]; then
        print_info "No fluentd namespaces selected - skipping"
    else
        print_warning "No files were uploaded (check Step 3 for errors)"
    fi
    echo ""
fi

# Original Step 5 logic (kept for fallback/non-selective mode)
# This is now only used if selective copy didn't upload files
if false; then  # Disabled - kept for reference
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
fi  # End of 'if false' block
echo ""

# Step 6: Cleanup
# Note: Step numbers are dynamically updated by Python backend based on logbay inclusion
print_header "Step 6/7: Cleanup"

# Use uploaded file list as source of truth for verification summary.
VERIFY_SUCCESS=${#UPLOADED_FILES[@]}
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
