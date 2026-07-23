#!/bin/bash

set -o pipefail  # Exit on pipe failures

# ============================================
# Configuration
# ============================================
NAMESPACE="ncm-cg"
DATASTORE_NAMESPACE="ntnx-ncm-datastore"
PG_POD="cg-pg-1"
DATABASE="cg_nx"
CRONJOB_NAME="cron-nx-cg-data-loader"
DATA_LOADER_DEPLOYMENT="nx-cg-data-loader"
ITERATIONS=20
SLEEP_DURATION=10800  # 180 minutes (3 hours)
BACKFILL_HOURS=8      # Hours to backfill
WAIT_AFTER_COMPLETION=300  # 5 minutes after job completion
LOG_CHECK_INTERVAL=15  # Check logs every 15 seconds for more responsive updates

# Kubeconfig setup
KUBECONFIG_FILE="/tmp/cg_kubeconfig_$(date +%s).yaml"
MSPCTL_PATH="/usr/local/nutanix/cluster/bin/mspctl"

# Completion patterns to monitor (all must be present for job to be considered complete)
# These patterns indicate the final stages of each job type
COMPLETION_PATTERNS=(
    # Job completion messages
    "CLUSTER_CONFIG completed successfully"
    "CATEGORIES_CONFIG completed successfully"
    "VM_CONFIG completed successfully"
    "CLUSTER_HARDWARE_CONFIG completed successfully"
    # Backfill status updates (indicates data persisted)
    "Updating service backfill status for Service :\[CLUSTER_CONFIG\]"
    "Updating service backfill status for Service :\[NX_ROUTINE_WORKFLOW\]"
    "Updating service backfill status for Service :\[CATEGORIES_CONFIG\]"
    "Updating service backfill status for Service :\[VM_CONFIG\]"
    "Updating service backfill status for Service :\[CLUSTER_HARDWARE_CONFIG\]"
)

# Global variable to store job start timestamp (ISO 8601 format for --since-time)
JOB_START_TIMESTAMP=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ============================================
# Functions
# ============================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${BLUE}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_section() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Setup kubeconfig
setup_kubeconfig() {
    log_info "Setting up kubeconfig..."
    
    if [ ! -f "$MSPCTL_PATH" ]; then
        log_error "mspctl not found at $MSPCTL_PATH"
        exit 1
    fi
    
    # Get kubeconfig from mspctl
    log_info "Fetching kubeconfig using mspctl..."
    if ! $MSPCTL_PATH cls kubeconfig nc > "$KUBECONFIG_FILE" 2>&1; then
        log_error "Failed to get kubeconfig from mspctl"
        exit 1
    fi
    
    if [ ! -s "$KUBECONFIG_FILE" ]; then
        log_error "Kubeconfig file is empty"
        exit 1
    fi
    
    log_success "Kubeconfig saved to $KUBECONFIG_FILE"
    export KUBECONFIG="$KUBECONFIG_FILE"
}

# Cleanup function for kubeconfig
cleanup() {
    if [ -f "$KUBECONFIG_FILE" ]; then
        rm -f "$KUBECONFIG_FILE"
        log_info "Cleaned up kubeconfig file"
    fi
}

# Trap EXIT to cleanup
trap cleanup EXIT

# Check if required commands exist
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v kubectl &>/dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi
    
    if ! kubectl --kubeconfig="$KUBECONFIG_FILE" get namespace "$NAMESPACE" &>/dev/null; then
        echo $(kubectl --kubeconfig="$KUBECONFIG_FILE" get namespace "$NAMESPACE" &>/dev/null)
        log_error "Namespace '$NAMESPACE' does not exist."
        exit 1
    fi
    
    if ! kubectl --kubeconfig="$KUBECONFIG_FILE" get namespace "$DATASTORE_NAMESPACE" &>/dev/null; then
        echo $(kubectl --kubeconfig="$KUBECONFIG_FILE" get namespace "$DATASTORE_NAMESPACE" &>/dev/null)
        log_error "Namespace '$DATASTORE_NAMESPACE' does not exist."
        exit 1
    fi
    
    if ! kubectl --kubeconfig="$KUBECONFIG_FILE" get pod -n "$DATASTORE_NAMESPACE" "$PG_POD" &>/dev/null; then
        echo $(kubectl --kubeconfig="$KUBECONFIG_FILE" get pod -n "$DATASTORE_NAMESPACE" "$PG_POD" &>/dev/null)
        log_error "PostgreSQL pod '$PG_POD' not found in namespace '$DATASTORE_NAMESPACE'."
        exit 1
    fi
    
    if ! kubectl --kubeconfig="$KUBECONFIG_FILE" get cronjob -n "$NAMESPACE" "$CRONJOB_NAME" &>/dev/null; then
        echo $(kubectl --kubeconfig="$KUBECONFIG_FILE" get cronjob -n "$NAMESPACE" "$CRONJOB_NAME" &>/dev/null)
        log_error "CronJob '$CRONJOB_NAME' not found in namespace '$NAMESPACE'."
        exit 1
    fi
    
    log_success "All prerequisites met."
}

# Execute SQL query
execute_sql() {
    local query="$1"
    local description="$2"
    
    if [ -n "$description" ]; then
        log_info "$description"
    fi
    
    kubectl --kubeconfig="$KUBECONFIG_FILE" exec -n "$DATASTORE_NAMESPACE" "$PG_POD" -- \
        psql -d "$DATABASE" -c "$query" 2>&1
    
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_error "SQL query failed with exit code $exit_code"
        return 1
    fi
    return 0
}

# Query backfill status
query_backfill_status() {
    execute_sql "SELECT * FROM nutanix_backfill_job_run_status;" "Querying backfill status..."
}

# Update backfill status
update_backfill_status() {
    local epoch_ms=$1
    
    log_info "Updating backfill status to epoch: $epoch_ms"
    
    execute_sql "UPDATE nutanix_backfill_job_run_status SET is_completed = false, last_persisted_epoch = $epoch_ms, last_completed_date = $epoch_ms;" \
        "Updating nutanix_backfill_job_run_status..."
    
    if [ $? -eq 0 ]; then
        log_success "Backfill status updated successfully."
        return 0
    else
        log_error "Failed to update backfill status."
        return 1
    fi
}

# Create and monitor job
create_data_loader_job() {
    local iteration=$1
    local job_name="cron-nx-cg-data-loader-manual-trigger-${iteration}-$(date +%Y-%m-%d-%H-%M-%S)"
    
    log_info "Creating job: $job_name"
    
    if kubectl --kubeconfig="$KUBECONFIG_FILE" create job --from=cronjob/"$CRONJOB_NAME" "$job_name" -n "$NAMESPACE" 2>&1; then
        log_success "Job '$job_name' created successfully."
        
        # Wait a few seconds for job to start
        sleep 5
        
        # Check job status
        local job_status=$(kubectl --kubeconfig="$KUBECONFIG_FILE" get job -n "$NAMESPACE" "$job_name" -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)
        log_info "Job status: ${job_status:-Pending}"
        
        # Get pod name for the job
        local pod_name=$(kubectl --kubeconfig="$KUBECONFIG_FILE" get pods -n "$NAMESPACE" -l job-name="$job_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        if [ -n "$pod_name" ]; then
            log_info "Job pod: $pod_name"
        fi
        
        return 0
    else
        log_error "Failed to create job '$job_name'."
        return 1
    fi
}

# Get the data loader pod name
get_data_loader_pod() {
    local pod_name=""
    
    pod_name=$(kubectl --kubeconfig="$KUBECONFIG_FILE" get pods -n "$NAMESPACE" --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | grep "^nx-cg-data-loader-" | head -1)
    
    if [ -z "$pod_name" ]; then
        pod_name=$(kubectl --kubeconfig="$KUBECONFIG_FILE" get pods -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | grep "^nx-cg-data-loader-" | head -1)
    fi
    
    echo "$pod_name"
}

# Track which milestones have been logged
declare -A LOGGED_MILESTONES

# Parse and display job progress milestones
display_job_progress() {
    local pod_name=$1
    
    if [ -z "$pod_name" ] || [ -z "$JOB_START_TIMESTAMP" ]; then
        return
    fi
    
    local logs
    logs=$(kubectl --kubeconfig="$KUBECONFIG_FILE" logs -n "$NAMESPACE" "$pod_name" --since-time="$JOB_START_TIMESTAMP" 2>/dev/null)
    
    if [ -z "$logs" ]; then
        return
    fi
    
    local job_types=("CATEGORIES_CONFIG" "VM_CONFIG" "CLUSTER_HARDWARE_CONFIG" "CLUSTER_CONFIG")
    
    for job_type in "${job_types[@]}"; do
        local processing_key="${job_type}_processing"
        if [ -z "${LOGGED_MILESTONES[$processing_key]}" ]; then
            if echo "$logs" | grep -q "Processing job: $job_type"; then
                echo -e "  ${CYAN}▶${NC} Started processing: ${YELLOW}$job_type${NC}"
                LOGGED_MILESTONES[$processing_key]=1
            fi
        fi
        
        local writing_key="${job_type}_writing"
        if [ -z "${LOGGED_MILESTONES[$writing_key]}" ]; then
            local write_pattern=""
            case "$job_type" in
                "CATEGORIES_CONFIG") write_pattern="Start writing to Category Metrics Tables" ;;
                "VM_CONFIG") write_pattern="Start writing to Vm Metrics Tables" ;;
                "CLUSTER_HARDWARE_CONFIG") write_pattern="Start writing to Cluster Hardware Metrics Tables" ;;
                "CLUSTER_CONFIG") write_pattern="Start writing to Cluster Metrics Tables" ;;
            esac
            if echo "$logs" | grep -q "$write_pattern"; then
                echo -e "  ${CYAN}✍${NC}  $job_type: Writing to ClickHouse tables"
                LOGGED_MILESTONES[$writing_key]=1
            fi
        fi
        
        local written_key="${job_type}_written"
        if [ -z "${LOGGED_MILESTONES[$written_key]}" ] && [ -n "${LOGGED_MILESTONES[${job_type}_writing]}" ]; then
            local write_pattern=""
            case "$job_type" in
                "CATEGORIES_CONFIG") write_pattern="Start writing to Category Metrics Tables" ;;
                "VM_CONFIG") write_pattern="Start writing to Vm Metrics Tables" ;;
                "CLUSTER_HARDWARE_CONFIG") write_pattern="Start writing to Cluster Hardware Metrics Tables" ;;
                "CLUSTER_CONFIG") write_pattern="Start writing to Cluster Metrics Tables" ;;
            esac
            local written_count=$(echo "$logs" | grep -A10 "$write_pattern" | grep "Written.*to clickhouse" | head -1 | sed 's/.*Written \([0-9]*\) to clickhouse.*/\1/')
            if [ -n "$written_count" ] && [ "$written_count" -gt 0 ] 2>/dev/null; then
                echo -e "  ${CYAN}💾${NC} $job_type: Written ${GREEN}$(printf "%'d" $written_count)${NC} records to ClickHouse"
                LOGGED_MILESTONES[$written_key]=1
            fi
        fi
        
        local completed_key="${job_type}_pg_completed"
        if [ -z "${LOGGED_MILESTONES[$completed_key]}" ]; then
            if echo "$logs" | grep -q "Updating service backfill status for Service :\[$job_type\]"; then
                echo -e "  ${CYAN}✅${NC} $job_type: Backfill status updated in PG"
                LOGGED_MILESTONES[$completed_key]=1
            fi
        fi
        
        local marked_key="${job_type}_marked"
        if [ -z "${LOGGED_MILESTONES[$marked_key]}" ] && [ -n "${LOGGED_MILESTONES[${job_type}_pg_completed]}" ]; then
            if echo "$logs" | grep -q "marked as completed updated in PG"; then
                echo -e "  ${CYAN}📝${NC} $job_type: Marked as completed in PG"
                LOGGED_MILESTONES[$marked_key]=1
            fi
        fi
    done
    
    local backfill_jobs_key="backfill_jobs_count"
    if [ -z "${LOGGED_MILESTONES[$backfill_jobs_key]}" ]; then
        local job_count=$(echo "$logs" | grep "Number of backfill jobs to process in workflow:" | sed 's/.*workflow: \([0-9]*\).*/\1/' | head -1)
        if [ -n "$job_count" ] && [ "$job_count" != "0" ]; then
            echo -e "  ${CYAN}📋${NC} Backfill jobs to process: ${YELLOW}$job_count${NC}"
            LOGGED_MILESTONES[$backfill_jobs_key]=1
        fi
    fi
}

# Check if all completion patterns are found
check_job_completion() {
    local pod_name=$1
    
    if [ -z "$pod_name" ] || [ -z "$JOB_START_TIMESTAMP" ]; then
        return 1
    fi
    
    local logs
    logs=$(kubectl --kubeconfig="$KUBECONFIG_FILE" logs -n "$NAMESPACE" "$pod_name" --since-time="$JOB_START_TIMESTAMP" 2>/dev/null)
    
    if [ -z "$logs" ]; then
        return 1
    fi
    
    local job_types=("CLUSTER_CONFIG" "CATEGORIES_CONFIG" "VM_CONFIG" "CLUSTER_HARDWARE_CONFIG" "NX_ROUTINE_WORKFLOW")
    local completed_jobs=()
    local pending_jobs=()
    
    for job_type in "${job_types[@]}"; do
        if echo "$logs" | grep -q "$job_type completed successfully" || \
           ([ "$job_type" = "NX_ROUTINE_WORKFLOW" ] && echo "$logs" | grep -q "Updating service backfill status for Service :\[NX_ROUTINE_WORKFLOW\]"); then
            completed_jobs+=("$job_type")
            
            local completion_msg_key="${job_type}_completion_msg"
            if [ -z "${LOGGED_MILESTONES[$completion_msg_key]}" ]; then
                echo -e "  ${GREEN}✓ $job_type is completed${NC}"
                LOGGED_MILESTONES[$completion_msg_key]=1
            fi
        else
            pending_jobs+=("$job_type")
        fi
    done
    
    local completed_count=${#completed_jobs[@]}
    local total_jobs=${#job_types[@]}
    
    if [ $completed_count -gt 0 ]; then
        log_info "Job Progress: $completed_count/$total_jobs jobs completed"
    fi
    
    if [ ${#pending_jobs[@]} -gt 0 ] && [ ${#pending_jobs[@]} -lt $total_jobs ]; then
        echo -e "  ${YELLOW}⏳ Pending:${NC} ${pending_jobs[*]}"
    fi
    
    if [ ${#pending_jobs[@]} -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

# Monitor logs and wait for completion
wait_for_job_completion() {
    local start_time=$(date +%s)
    local elapsed=0
    local pod_name
    local last_progress_time=0
    
    log_section "Monitoring Data Loader Job"
    
    unset LOGGED_MILESTONES
    declare -gA LOGGED_MILESTONES
    
    JOB_START_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    log_info "Job start timestamp: $JOB_START_TIMESTAMP"
    
    log_info "Waiting for data loader pod to be ready..."
    sleep 10
    
    pod_name=$(get_data_loader_pod)
    
    if [ -z "$pod_name" ]; then
        log_warn "Could not find data loader pod. Using timeout-based waiting."
        sleep $WAIT_AFTER_COMPLETION
        return 0
    fi
    
    log_info "Monitoring pod: $pod_name"
    log_info "Looking for ${#COMPLETION_PATTERNS[@]} completion patterns"
    log_info "Checking every $LOG_CHECK_INTERVAL seconds"
    log_info "Max wait time: $SLEEP_DURATION seconds ($((SLEEP_DURATION / 60)) minutes)"
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  JOB PROGRESS${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    
    while [ $elapsed -lt $SLEEP_DURATION ]; do
        local current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        local remaining=$((SLEEP_DURATION - elapsed))
        
        pod_name=$(get_data_loader_pod)
        
        if [ -z "$pod_name" ]; then
            log_warn "Data loader pod not found. Waiting..."
            sleep $LOG_CHECK_INTERVAL
            continue
        fi
        
        display_job_progress "$pod_name"
        
        if check_job_completion "$pod_name"; then
            echo ""
            echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
            log_success "All data loader jobs completed successfully!"
            echo ""
            log_info "Waiting $WAIT_AFTER_COMPLETION seconds ($(( WAIT_AFTER_COMPLETION / 60 )) min) before next iteration..."
            sleep $WAIT_AFTER_COMPLETION
            return 0
        fi
        
        if [ $((elapsed - last_progress_time)) -ge 60 ]; then
            echo ""
            printf "  ${CYAN}⏱️  Elapsed:${NC} %dm%ds | ${CYAN}Remaining:${NC} %dm%ds\n" \
                $((elapsed / 60)) $((elapsed % 60)) \
                $((remaining / 60)) $((remaining % 60))
            last_progress_time=$elapsed
        fi
        
        sleep $LOG_CHECK_INTERVAL
    done
    
    echo ""
    log_warn "Timeout reached ($SLEEP_DURATION seconds). Proceeding to next iteration."
    return 0
}

# ============================================
# Main Script
# ============================================

LOG_FILE="data_loader_$(date +%Y-%m-%d_%H-%M-%S).log"

log_section "Starting Data Loader Script"
log_info "Configuration:"
echo "  - Namespace: $NAMESPACE"
echo "  - Datastore Namespace: $DATASTORE_NAMESPACE"
echo "  - PostgreSQL Pod: $PG_POD"
echo "  - Database: $DATABASE"
echo "  - CronJob: $CRONJOB_NAME"
echo "  - Data Loader Deployment: $DATA_LOADER_DEPLOYMENT"
echo "  - Iterations: $ITERATIONS"
echo "  - Max Wait Duration: $SLEEP_DURATION seconds ($((SLEEP_DURATION / 60)) minutes)"
echo "  - Wait After Completion: $WAIT_AFTER_COMPLETION seconds ($((WAIT_AFTER_COMPLETION / 60)) minutes)"
echo "  - Log Check Interval: $LOG_CHECK_INTERVAL seconds"
echo "  - Backfill Hours: $BACKFILL_HOURS"

# Setup kubeconfig first
setup_kubeconfig

# Check prerequisites
check_prerequisites

# Statistics
SUCCESS_COUNT=0
FAILURE_COUNT=0
START_TIME=$(date +%s)

# Main loop
for i in $(seq 1 $ITERATIONS); do
    log_section "Iteration $i of $ITERATIONS"
    ITERATION_START=$(date +%s)
    
    if ! query_backfill_status; then
        log_error "Failed to query initial backfill status."
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
        continue
    fi
    
    BACKFILL_MS=$((BACKFILL_HOURS * 3600 * 1000))
    EPOCH_MS=$(($(date +%s%3N) - BACKFILL_MS))
    log_info "Calculated backfill epoch: $EPOCH_MS ($(date -d @$((EPOCH_MS / 1000)) '+%Y-%m-%d %H:%M:%S'))"
    
    log_info "Checking if backfill status already needs updating..."
    CURRENT_LAST_PERSISTED=$(kubectl --kubeconfig="$KUBECONFIG_FILE" exec -n "$DATASTORE_NAMESPACE" "$PG_POD" -- \
        psql -d "$DATABASE" -t -c "SELECT last_persisted_epoch FROM nutanix_backfill_job_run_status LIMIT 1;" 2>&1 | tr -d ' ')
    
    if [[ "$CURRENT_LAST_PERSISTED" =~ ^[0-9]+$ ]]; then
        if [ "$CURRENT_LAST_PERSISTED" -le "$EPOCH_MS" ]; then
            log_warn "Backfill status is already older than $BACKFILL_HOURS hours. Skipping update."
        else
            if ! update_backfill_status "$EPOCH_MS"; then
                log_error "Failed to update backfill status. Skipping this iteration."
                FAILURE_COUNT=$((FAILURE_COUNT + 1))
                continue
            fi
        fi
    else
        log_warn "Could not retrieve valid current backfill values. Proceeding with update."
        if ! update_backfill_status "$EPOCH_MS"; then
            log_error "Failed to update backfill status. Skipping this iteration."
            FAILURE_COUNT=$((FAILURE_COUNT + 1))
            continue
        fi
    fi
    
    if ! create_data_loader_job "$i"; then
        log_error "Failed to create data loader job. Skipping this iteration."
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
        continue
    fi
    
    log_info "Checking backfill status after job creation..."
    query_backfill_status
    
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    
    ITERATION_END=$(date +%s)
    ITERATION_DURATION=$((ITERATION_END - ITERATION_START))
    log_success "Iteration $i job triggered in $ITERATION_DURATION seconds."
    
    if [ $i -lt $ITERATIONS ]; then
        wait_for_job_completion
    fi
done

# ============================================
# Final Summary
# ============================================

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

log_section "Data Loader Script Completed"
log_info "Summary:"
echo "  - Total Iterations: $ITERATIONS"
echo "  - Successful: $SUCCESS_COUNT"
echo "  - Failed: $FAILURE_COUNT"
echo "  - Total Duration: $((TOTAL_DURATION / 60)) minutes ($TOTAL_DURATION seconds)"
echo "  - Average per Iteration: $((TOTAL_DURATION / ITERATIONS)) seconds"

if [ $FAILURE_COUNT -eq 0 ]; then
    log_success "All iterations completed successfully!"
    exit 0
else
    log_warn "$FAILURE_COUNT iteration(s) failed. Check logs for details."
    exit 1
fi
