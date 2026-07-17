#!/usr/bin/env bash
set -euo pipefail

# Live-stream microservice *.log files from pods for a fixed duration (default 30m) or until abort.
#
# For each selected service:
# 1) Resolve pod dynamically by prefix
# 2) Discover candidate program names from supervisorctl status
# 3) Select matching *.log files from service log dir
# 4) Stream each file with "tail -F" via kubectl exec into local output files
#
# Stop conditions:
# - duration expires (default 30m)
# - user presses Ctrl+C (abort)

KUBECONFIG_PATH="${HOME}/kube/ss_kube"
KUBECONFIG_EXPLICIT=0
SELECTED_SERVICES="epsilon,calm,policy,scheduler"
DURATION_SEC=$((30 * 60))
OUT_BASE="microservice-live-logs"
KUBECTL_REQUEST_TIMEOUT="20s"
KUBECTL_RETRIES=3
KUBECTL_RETRY_SLEEP_SEC=2

usage() {
  cat <<'EOF'
Usage: collect_microservice_live_logs.sh [options]

Options:
  -k <kubeconfig>   Kubeconfig path (optional; auto-falls back to latest known kubeconfig)
  -s <services>     Comma-separated services (default: epsilon,calm,policy,scheduler)
  -d <seconds>      Stream duration in seconds (default: 1800)
  -o <output_base>  Output base directory prefix (default: microservice-live-logs)
  -r <retries>      kubectl retries on transient errors (default: 3)
  -w <timeout>      kubectl request timeout (default: 20s)
  -h                Show help

Examples:
  ./collect_microservice_live_logs.sh
  ./collect_microservice_live_logs.sh -s epsilon -d 1200
  ./collect_microservice_live_logs.sh -k nc_kubecconfig -s epsilon,policy
EOF
}

while getopts ":k:s:d:o:r:w:h" opt; do
  case "${opt}" in
    k)
      KUBECONFIG_PATH="${OPTARG}"
      KUBECONFIG_EXPLICIT=1
      ;;
    s) SELECTED_SERVICES="${OPTARG}" ;;
    d) DURATION_SEC="${OPTARG}" ;;
    o) OUT_BASE="${OPTARG}" ;;
    r) KUBECTL_RETRIES="${OPTARG}" ;;
    w) KUBECTL_REQUEST_TIMEOUT="${OPTARG}" ;;
    h)
      usage
      exit 0
      ;;
    \?)
      echo "Invalid option: -${OPTARG}" >&2
      usage
      exit 2
      ;;
  esac
done

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[ERROR] kubectl not found in PATH" >&2
  exit 1
fi

if ! [[ "${DURATION_SEC}" =~ ^[0-9]+$ ]] || [[ "${DURATION_SEC}" -le 0 ]]; then
  echo "[ERROR] -d duration must be a positive integer (seconds)" >&2
  exit 1
fi

if ! [[ "${KUBECTL_RETRIES}" =~ ^[0-9]+$ ]] || [[ "${KUBECTL_RETRIES}" -lt 1 ]]; then
  echo "[ERROR] -r retries must be >= 1" >&2
  exit 1
fi

declare -A NS_BY_SERVICE=(
  ["epsilon"]="ntnx-ncm-common"
  ["calm"]="ntnx-ncm-self-service"
  ["policy"]="ntnx-ncm-self-service"
  ["scheduler"]="ntnx-ncm-self-service"
)
declare -A POD_PREFIX_BY_SERVICE=(
  ["epsilon"]="ncm-epsilon"
  ["calm"]="ncm-calm"
  ["policy"]="ncm-policy"
  ["scheduler"]="ncm-scheduler"
)
declare -A LOG_DIR_BY_SERVICE=(
  ["epsilon"]="/home/epsilon/log"
  ["calm"]="/home/calm/log"
  ["policy"]="/home/policy/log"
  ["scheduler"]="/home/epsilon/log"
)
declare -A SUPERVISOR_CONF_BY_SERVICE=(
  ["epsilon"]="/home/epsilon/conf/supervisor/supervisord.conf"
  ["calm"]="/home/calm/conf/supervisor/supervisord.conf"
  ["policy"]="/home/policy/conf/supervisor/supervisord.conf"
  ["scheduler"]="/home/epsilon/conf/supervisor/supervisord.conf"
)
declare -A ACTIVATE_BY_SERVICE=(
  ["epsilon"]="/home/epsilon/venv/bin/activate"
  ["calm"]="/home/calm/venv/bin/activate"
  ["policy"]="/home/policy/venv/bin/activate"
  ["scheduler"]="/home/epsilon/venv/bin/activate"
)

run_kubectl_retry() {
  local attempt=1
  local rc=0
  local output=""
  while true; do
    if output="$("$@" 2>&1)"; then
      printf "%s" "${output}"
      return 0
    fi
    rc=$?
    if [[ "${attempt}" -ge "${KUBECTL_RETRIES}" ]]; then
      printf "%s" "${output}" >&2
      return "${rc}"
    fi
    echo "[WARN] kubectl attempt ${attempt}/${KUBECTL_RETRIES} failed; retrying in ${KUBECTL_RETRY_SLEEP_SEC}s..." >&2
    echo "[WARN] ${output}" >&2
    attempt=$((attempt + 1))
    sleep "${KUBECTL_RETRY_SLEEP_SEC}"
  done
}

resolve_pod() {
  local namespace="$1"
  local prefix="$2"
  run_kubectl_retry kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" get pods -n "${namespace}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
    | awk -v p="^${prefix}" '$0 ~ p {print; exit}'
}

run_kexec() {
  local ns="$1"
  local pod="$2"
  local cmd="$3"
  run_kubectl_retry kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" exec -n "${ns}" "${pod}" -- bash -lc "${cmd}"
}

resolve_kubeconfig_path() {
  # 1) explicit/user-provided or default path if present
  if [[ -n "${KUBECONFIG_PATH:-}" && -f "${KUBECONFIG_PATH}" ]]; then
    echo "${KUBECONFIG_PATH}"
    return 0
  fi

  # If user explicitly passed -k and file is missing, fail fast.
  if [[ "${KUBECONFIG_EXPLICIT}" -eq 1 ]]; then
    echo "[ERROR] kubeconfig not found at explicit path: ${KUBECONFIG_PATH}" >&2
    return 1
  fi

  # 2) Try project kubeconfigs latest symlink/files
  local candidate=""
  local d
  for d in "${PWD}/kubeconfigs" "${HOME}/mohan_helpers/bulk_snapshots_ui/kubeconfigs" "${HOME}/kubeconfigs"; do
    [[ -d "${d}" ]] || continue
    candidate="$(ls -1t "${d}"/*_kubeconfig_latest 2>/dev/null | head -n 1 || true)"
    if [[ -n "${candidate}" && -e "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
    candidate="$(ls -1t "${d}"/*_kubeconfig_* 2>/dev/null | head -n 1 || true)"
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done

  echo "[ERROR] No kubeconfig found. Checked: ${KUBECONFIG_PATH}, ./kubeconfigs, ~/mohan_helpers/bulk_snapshots_ui/kubeconfigs, ~/kubeconfigs" >&2
  return 1
}

KUBECONFIG_PATH="$(resolve_kubeconfig_path)"
echo "[INFO] Using kubeconfig: ${KUBECONFIG_PATH}"

echo "[INFO] Preflight: checking cluster connectivity..."
if ! run_kubectl_retry kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" cluster-info >/dev/null; then
  echo "[ERROR] Kubernetes API not reachable. Check kubeconfig/context/network."
  exit 1
fi

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="${OUT_BASE}_${ts}"
mkdir -p "${out_dir}"

echo "[INFO] Output directory: ${out_dir}"
echo "[INFO] Services: ${SELECTED_SERVICES}"
echo "[INFO] Duration: ${DURATION_SEC}s"

declare -a PIDS=()
declare -a STREAM_META=()
STOP_REQUESTED=0

cleanup() {
  STOP_REQUESTED=1
  echo ""
  echo "[INFO] Stopping all live streams..."
  for pid in "${PIDS[@]}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
  # Allow children to exit, then force kill remaining.
  sleep 1
  for pid in "${PIDS[@]}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  done
}

trap 'echo "[WARN] Abort requested (Ctrl+C)"; cleanup' INT TERM

IFS=',' read -r -a services <<< "${SELECTED_SERVICES}"

for raw_service in "${services[@]}"; do
  service="$(echo "${raw_service}" | xargs)"
  [[ -z "${service}" ]] && continue
  if [[ -z "${NS_BY_SERVICE[${service}]:-}" ]]; then
    echo "[WARN] Unknown service '${service}', skipping"
    continue
  fi

  ns="${NS_BY_SERVICE[${service}]}"
  prefix="${POD_PREFIX_BY_SERVICE[${service}]}"
  log_dir="${LOG_DIR_BY_SERVICE[${service}]}"
  activate="${ACTIVATE_BY_SERVICE[${service}]}"
  supervisor_conf="${SUPERVISOR_CONF_BY_SERVICE[${service}]}"

  echo "==================================================================="
  echo "[INFO] Service: ${service} (ns=${ns}, prefix=${prefix}, dir=${log_dir})"

  pod="$(resolve_pod "${ns}" "${prefix}" || true)"
  if [[ -z "${pod}" ]]; then
    echo "[ERROR] No pod found for ${service} (prefix ${prefix})"
    continue
  fi
  echo "[INFO] Resolved pod: ${pod}"

  service_dir="${out_dir}/${service}"
  mkdir -p "${service_dir}"

  status_file="${service_dir}/supervisor_status.txt"
  {
    echo "Service: ${service}"
    echo "Namespace: ${ns}"
    echo "Pod: ${pod}"
    echo "Collected at: $(date)"
    echo "------------------------------------------------------------"
    run_kexec "${ns}" "${pod}" "source '${activate}' >/dev/null 2>&1 || true; supervisorctl -c '${supervisor_conf}' status | head -n 200"
  } > "${status_file}" 2>&1 || true

  mapfile -t base_names < <(
    awk '
      NF > 0 {
        n=$1
        sub(/^.*:/, "", n)
        if (n != "") print n
      }' "${status_file}" | sort -u
  )

  listing_file="${service_dir}/pod_log_listing.txt"
  run_kexec "${ns}" "${pod}" "ls -1 '${log_dir}' 2>/dev/null || true" > "${listing_file}" || true
  mapfile -t all_log_files < <(awk '/\.log$/ {print $0}' "${listing_file}" | sort -u)

  started_for_service=0
  for base in "${base_names[@]}"; do
    [[ -z "${base}" ]] && continue
    for lf in "${all_log_files[@]}"; do
      [[ -z "${lf}" ]] && continue
      # Only plain *.log files (not .log.1 etc), per request.
      if [[ "${lf}" =~ ^${base}([._-].*)?\.log$ ]]; then
        local_out="${service_dir}/${lf}"
        echo "[INFO] Streaming ${service}/${lf} -> ${local_out}"
        # Run in background: tail -F inside pod, redirect to local file.
        kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
          exec -n "${ns}" "${pod}" -- bash -lc "tail -F '${log_dir}/${lf}'" > "${local_out}" 2>&1 &
        pid=$!
        PIDS+=("${pid}")
        STREAM_META+=("${service}:${pod}:${lf}:${pid}")
        started_for_service=$((started_for_service + 1))
      fi
    done
  done

  if [[ "${started_for_service}" -eq 0 ]]; then
    echo "[WARN] No matching *.log files found to stream for ${service}"
  else
    echo "[INFO] Started ${started_for_service} live stream(s) for ${service}"
  fi
done

if [[ "${#PIDS[@]}" -eq 0 ]]; then
  echo "[WARN] No live streams started."
  exit 0
fi

echo "==================================================================="
echo "[INFO] Live streaming started for ${#PIDS[@]} file(s)."
echo "[INFO] Press Ctrl+C to abort early, or wait ${DURATION_SEC}s."

elapsed=0
while [[ "${elapsed}" -lt "${DURATION_SEC}" ]]; do
  if [[ "${STOP_REQUESTED}" -eq 1 ]]; then
    break
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

cleanup

echo "==================================================================="
echo "[INFO] Stream session ended."
echo "[INFO] Output: ${out_dir}"
echo "[INFO] Streams attempted:"
for meta in "${STREAM_META[@]}"; do
  echo "  - ${meta}"
done
