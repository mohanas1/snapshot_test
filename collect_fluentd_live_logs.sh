#!/usr/bin/env bash
set -euo pipefail

# Stream live pod logs for selected namespaces during a fixed duration.
# Intended for "live fluentd" mode invocations from the UI/backend.

KUBECONFIG_PATH="${HOME}/kube/ss_kube"
KUBECONFIG_EXPLICIT=0
NAMESPACES="cpaas-system,default,domain-manager,kube-system,ncm-cg,nc-system,ntnx-ikat,ntnx-ncm-aiops,ntnx-ncm-common,ntnx-ncm-datastore,ntnx-ncm-self-service,ntnx-system"
DURATION_SEC=$((30 * 60))
OUT_BASE="fluentd-live-logs"
KUBECTL_REQUEST_TIMEOUT="20s"

usage() {
  cat <<'EOF'
Usage: collect_fluentd_live_logs.sh [options]

Options:
  -k <kubeconfig>   Kubeconfig path (optional; auto-fallback to latest known kubeconfig)
  -n <namespaces>   Comma-separated namespaces
  -d <seconds>      Stream duration in seconds (default: 1800)
  -o <output_base>  Output base directory prefix (default: fluentd-live-logs)
  -w <timeout>      kubectl request timeout (default: 20s)
  -h                Show help
EOF
}

while getopts ":k:n:d:o:w:h" opt; do
  case "${opt}" in
    k) KUBECONFIG_PATH="${OPTARG}"; KUBECONFIG_EXPLICIT=1 ;;
    n) NAMESPACES="${OPTARG}" ;;
    d) DURATION_SEC="${OPTARG}" ;;
    o) OUT_BASE="${OPTARG}" ;;
    w) KUBECTL_REQUEST_TIMEOUT="${OPTARG}" ;;
    h) usage; exit 0 ;;
    \?) echo "Invalid option: -${OPTARG}" >&2; usage; exit 2 ;;
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

resolve_kubeconfig_path() {
  if [[ -n "${KUBECONFIG_PATH:-}" && -f "${KUBECONFIG_PATH}" ]]; then
    echo "${KUBECONFIG_PATH}"
    return 0
  fi
  if [[ "${KUBECONFIG_EXPLICIT}" -eq 1 ]]; then
    echo "[ERROR] kubeconfig not found at explicit path: ${KUBECONFIG_PATH}" >&2
    return 1
  fi

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
  echo "[ERROR] No kubeconfig found. Checked explicit/default and known kubeconfig dirs." >&2
  return 1
}

KUBECONFIG_PATH="$(resolve_kubeconfig_path)"
echo "[INFO] Using kubeconfig: ${KUBECONFIG_PATH}"

echo "[INFO] Preflight: checking cluster connectivity..."
if ! kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" cluster-info >/dev/null 2>&1; then
  echo "[ERROR] Kubernetes API not reachable. Check kubeconfig/context/network."
  exit 1
fi

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="${OUT_BASE}_${ts}"
mkdir -p "${out_dir}"

echo "[INFO] Output directory: ${out_dir}"
echo "[INFO] Namespaces: ${NAMESPACES}"
echo "[INFO] Duration: ${DURATION_SEC}s"

declare -a PIDS=()
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
  sleep 1
  for pid in "${PIDS[@]}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  done
}

trap 'echo "[WARN] Abort requested (Ctrl+C)"; cleanup' INT TERM

IFS=',' read -r -a ns_list <<< "${NAMESPACES}"
total_streams=0
for raw_ns in "${ns_list[@]}"; do
  ns="$(echo "${raw_ns}" | xargs)"
  [[ -z "${ns}" ]] && continue
  echo "==================================================================="
  echo "[INFO] Namespace: ${ns}"

  mapfile -t pods < <(
    kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
      get pods -n "${ns}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
  )
  if [[ "${#pods[@]}" -eq 0 ]]; then
    echo "[WARN] No pods found for namespace: ${ns}"
    continue
  fi

  ns_dir="${out_dir}/${ns}"
  mkdir -p "${ns_dir}"
  for pod in "${pods[@]}"; do
    [[ -z "${pod}" ]] && continue
    out_file="${ns_dir}/${pod}.log"
    echo "[INFO] Streaming ${ns}/${pod} -> ${out_file}"
    kubectl --kubeconfig="${KUBECONFIG_PATH}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
      logs -n "${ns}" "${pod}" --all-containers=true -f --since=10s > "${out_file}" 2>&1 &
    PIDS+=("$!")
    total_streams=$((total_streams + 1))
  done
done

if [[ "${total_streams}" -eq 0 ]]; then
  echo "[WARN] No live streams started."
  exit 0
fi

echo "==================================================================="
echo "[INFO] Live streaming started for ${total_streams} pod log stream(s)."
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
