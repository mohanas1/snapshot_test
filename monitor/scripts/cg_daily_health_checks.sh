#!/usr/bin/env bash

set -euo pipefail

# Robust CG daily health check script.
# - Kubernetes health and events
# - Endpoint synthetic checks
# - Prometheus KPIs (if configured)
# - Severity scoring for day-2 operations

MONITOR_TZ="${MONITOR_TZ:-Asia/Kolkata}"
export TZ="${MONITOR_TZ}"

KUBECONFIG_PATH="${KUBECONFIG_PATH:-$HOME/.kube/nc_kubeconfig}"
CG_NAMESPACE="${CG_NAMESPACE:-ncm-cg}"
NCM_BASE_URL="${NCM_BASE_URL:-}"
NCM_TOKEN="${NCM_TOKEN:-}"

PROMETHEUS_BASE_URL="${PROMETHEUS_BASE_URL:-}"
PROMETHEUS_TOKEN="${PROMETHEUS_TOKEN:-}"
PROMETHEUS_USERNAME="${PROMETHEUS_USERNAME:-admin}"
PROMETHEUS_PASSWORD="${PROMETHEUS_PASSWORD:-Nutanix.123}"
PROM_QUERY_WINDOW="${PROM_QUERY_WINDOW:-15m}"
PROM_NS_LABEL="${PROM_NS_LABEL:-ncm-cg}"
CHECK_NODE_PRESSURE="${CHECK_NODE_PRESSURE:-1}"
PROM_TOP_PODS="${PROM_TOP_PODS:-8}"

CG_HEALTH_PATH="${CG_HEALTH_PATH:-/internal/status}"
CG_UI_PATH="${CG_UI_PATH:-/cg}"
CG_V1_PATH="${CG_V1_PATH:-/v1/cg}"
CG_V2_PATH="${CG_V2_PATH:-/v2/cg}"
CG_REPORTS_PATH="${CG_REPORTS_PATH:-/v1/reports}"

WARN_API_P95_SEC="${WARN_API_P95_SEC:-2}"
CRIT_API_P95_SEC="${CRIT_API_P95_SEC:-4}"
WARN_API_5XX_PCT="${WARN_API_5XX_PCT:-1}"
CRIT_API_5XX_PCT="${CRIT_API_5XX_PCT:-3}"
WARN_CPU_UTIL_PCT="${WARN_CPU_UTIL_PCT:-80}"
CRIT_CPU_UTIL_PCT="${CRIT_CPU_UTIL_PCT:-90}"
WARN_MEM_UTIL_PCT="${WARN_MEM_UTIL_PCT:-85}"
CRIT_MEM_UTIL_PCT="${CRIT_MEM_UTIL_PCT:-92}"
WARN_THROTTLE_PCT="${WARN_THROTTLE_PCT:-10}"
CRIT_THROTTLE_PCT="${CRIT_THROTTLE_PCT:-20}"
WARN_UI_LATENCY_SEC="${WARN_UI_LATENCY_SEC:-2.0}"
CHECK_EPHEMERAL_DISK="${CHECK_EPHEMERAL_DISK:-1}"

PASS_COUNT=0
WARN_COUNT=0
CRIT_COUNT=0
FAIL_COUNT=0

timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] $*"; }
print_header() {
  echo
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}
need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command '$1' not found"
    exit 1
  }
}
record_pass() { PASS_COUNT=$((PASS_COUNT + 1)); log "PASS: $*"; }
record_warn() { WARN_COUNT=$((WARN_COUNT + 1)); log "WARN: $*"; }
record_crit() { CRIT_COUNT=$((CRIT_COUNT + 1)); log "CRITICAL: $*"; }
record_fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); log "FAIL: $*"; }

float_gt() {
  python3.12 - <<'PY' "$1" "$2"
import sys
try:
    a=float(sys.argv[1]); b=float(sys.argv[2]); sys.exit(0 if a>b else 1)
except Exception:
    sys.exit(1)
PY
}

http_check() {
  local name="$1"; local url="$2"; local code
  if [[ -n "$NCM_TOKEN" ]]; then
    code="$(curl -k -L -sS -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${NCM_TOKEN}" "$url" || true)"
  else
    code="$(curl -k -L -sS -o /dev/null -w "%{http_code}" "$url" || true)"
  fi
  if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
    record_pass "${name} (${url}) => HTTP ${code}"
  elif [[ "$code" =~ ^3[0-9][0-9]$ ]]; then
    record_warn "${name} (${url}) redirected => HTTP ${code}"
  else
    record_fail "${name} (${url}) => HTTP ${code}"
  fi
}

prom_query_value() {
  local query="$1"
  local auth=()
  [[ -n "$PROMETHEUS_TOKEN" ]] && auth=(-H "Authorization: Bearer ${PROMETHEUS_TOKEN}")
  local basic=()
  if [[ -z "$PROMETHEUS_TOKEN" && -n "$PROMETHEUS_USERNAME" && -n "$PROMETHEUS_PASSWORD" ]]; then
    basic=(-u "${PROMETHEUS_USERNAME}:${PROMETHEUS_PASSWORD}")
  fi
  local resp
  resp="$(curl -k -sS "${auth[@]}" "${basic[@]}" -G --data-urlencode "query=${query}" "${PROMETHEUS_BASE_URL%/}/api/v1/query" || true)"
  python3.12 - <<'PY' "$resp"
import json,sys
raw=sys.argv[1]
try:
    j=json.loads(raw)
    if j.get("status")!="success":
        print("")
    else:
        res=j.get("data",{}).get("result",[])
        if not res:
            print("")
        else:
            print(str(res[0].get("value",[None,""])[1]).strip())
except Exception:
    print("")
PY
}

prom_query_rows() {
  local query="$1"
  local max_rows="${2:-8}"
  local auth=()
  [[ -n "$PROMETHEUS_TOKEN" ]] && auth=(-H "Authorization: Bearer ${PROMETHEUS_TOKEN}")
  local basic=()
  if [[ -z "$PROMETHEUS_TOKEN" && -n "$PROMETHEUS_USERNAME" && -n "$PROMETHEUS_PASSWORD" ]]; then
    basic=(-u "${PROMETHEUS_USERNAME}:${PROMETHEUS_PASSWORD}")
  fi
  local resp
  resp="$(curl -k -sS "${auth[@]}" "${basic[@]}" -G --data-urlencode "query=${query}" "${PROMETHEUS_BASE_URL%/}/api/v1/query" || true)"
  python3.12 - <<'PY' "$resp" "$max_rows"
import json,sys
raw=sys.argv[1]
max_rows=int(sys.argv[2] or "8")
try:
    j=json.loads(raw)
    if j.get("status")!="success":
        print("")
    else:
        rows=[]
        for r in j.get("data",{}).get("result",[]):
            metric=r.get("metric",{})
            pod=metric.get("pod") or metric.get("pod_name") or metric.get("name") or "unknown"
            val=r.get("value",[None,""])[1]
            try:
                f=float(val)
            except Exception:
                continue
            rows.append((pod,f))
        rows.sort(key=lambda x:x[1], reverse=True)
        for pod,val in rows[:max_rows]:
            print(f"{pod}\t{val}")
except Exception:
    print("")
PY
}

as_num() {
  python3.12 - <<'PY' "$1"
import sys
v=str(sys.argv[1]).strip()
try:
    print(float(v))
except Exception:
    print("")
PY
}

need_cmd kubectl
need_cmd curl
need_cmd python3.12

print_header "CG DAILY HEALTH CHECKS"
log "Using kubeconfig: ${KUBECONFIG_PATH}"
log "Using namespace: ${CG_NAMESPACE}"

print_header "KUBERNETES: POD AND DEPLOYMENT HEALTH"
kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" -o wide || true
kubectl --kubeconfig "${KUBECONFIG_PATH}" get deploy -n "${CG_NAMESPACE}" || true
kubectl --kubeconfig "${KUBECONFIG_PATH}" get svc -n "${CG_NAMESPACE}" || true

total_pods="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
not_ready_pods="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" --no-headers 2>/dev/null | awk '$2 !~ /^([0-9]+)\/\1$/ && $3!="Completed" && $3!="Succeeded" {c++} END{print c+0}')"
crashloop_pods="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" --no-headers 2>/dev/null | grep -c CrashLoopBackOff || true)"
oom_count="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get events -n "${CG_NAMESPACE}" --sort-by=.lastTimestamp 2>/dev/null | grep -ci OOMKilled || true)"

if [[ "${not_ready_pods:-0}" -eq 0 ]]; then record_pass "All running pods Ready (total=${total_pods:-0})"; else record_warn "NotReady running pods: ${not_ready_pods}"; fi
if [[ "${crashloop_pods:-0}" -ge 2 ]]; then record_crit "CrashLoopBackOff pods >= 2 (${crashloop_pods})"; elif [[ "${crashloop_pods:-0}" -eq 1 ]]; then record_warn "CrashLoopBackOff pods: 1"; else record_pass "No CrashLoopBackOff pods"; fi
if [[ "${oom_count:-0}" -ge 2 ]]; then record_crit "OOMKilled events >= 2 in recent events (${oom_count})"; elif [[ "${oom_count:-0}" -eq 1 ]]; then record_warn "OOMKilled event observed (${oom_count})"; else record_pass "No OOMKilled events in recent list"; fi

print_header "KUBERNETES: RECENT EVENTS"
kubectl --kubeconfig "${KUBECONFIG_PATH}" get events -n "${CG_NAMESPACE}" --sort-by=.lastTimestamp || true

print_header "KUBERNETES: RESTART SUMMARY"
kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" \
  -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[*].ready,RESTARTS:.status.containerStatuses[*].restartCount,PHASE:.status.phase' || true

restart_heavy="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[*].restartCount}{"\n"}{end}' 2>/dev/null | awk '{s=0;for(i=2;i<=NF;i++)s+=$i; if(s>3) c++} END{print c+0}')"
if [[ "${restart_heavy:-0}" -gt 0 ]]; then record_warn "Pods with restartCount > 3: ${restart_heavy}"; else record_pass "No pods with restartCount > 3"; fi

if [[ "${CHECK_NODE_PRESSURE}" == "1" ]]; then
  print_header "NODE HEALTH (PRESSURE SIGNALS)"
  node_pressure="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get nodes 2>/dev/null | grep -Ec 'NotReady|MemoryPressure|DiskPressure|PIDPressure' || true)"
  kubectl --kubeconfig "${KUBECONFIG_PATH}" get nodes -o wide || true
  if [[ "${node_pressure:-0}" -gt 0 ]]; then record_warn "Node pressure/NotReady signals found (${node_pressure})"; else record_pass "No node pressure signals"; fi
fi

print_header "HTTP SYNTHETIC CHECKS"
if [[ -n "$NCM_BASE_URL" ]]; then
  http_check "CG Health Endpoint" "${NCM_BASE_URL}${CG_HEALTH_PATH}"
  http_check "CG UI" "${NCM_BASE_URL}${CG_UI_PATH}"
  http_check "CG API v1" "${NCM_BASE_URL}${CG_V1_PATH}"
  http_check "CG API v2" "${NCM_BASE_URL}${CG_V2_PATH}"
  http_check "CG Reports API" "${NCM_BASE_URL}${CG_REPORTS_PATH}"
else
  record_warn "NCM_BASE_URL not set; skipped endpoint checks"
fi

print_header "LATENCY & RESPONSE TIME CHECKS"
if [[ -n "$NCM_BASE_URL" ]]; then
  ui_url="${NCM_BASE_URL%/}${CG_UI_PATH}"
  ui_latency="$(curl -k -L -sS -o /dev/null -w "%{time_total}" --max-time 8 "$ui_url" || true)"
  if [[ -n "$ui_latency" ]]; then
    log "CG UI Latency: ${ui_latency}s (${ui_url})"
    if float_gt "$ui_latency" "$WARN_UI_LATENCY_SEC"; then
      record_warn "CG UI latency high (${ui_latency}s > ${WARN_UI_LATENCY_SEC}s)"
    else
      record_pass "CG UI latency within threshold (${ui_latency}s)"
    fi
  else
    record_warn "CG UI latency could not be measured"
  fi
else
  record_warn "NCM_BASE_URL not set; skipped latency checks"
fi

print_header "STUCK / TERMINATING / ORPHAN POD CHECKS"
stuck_pods="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get pods -n "${CG_NAMESPACE}" --no-headers 2>/dev/null | grep -E 'Terminating|Unknown|Pending|ContainerCreating' || true)"
if [[ -n "$stuck_pods" ]]; then
  record_warn "Pods in non-running/transient states detected"
  echo "$stuck_pods"
else
  record_pass "No stuck/terminating/pending CG pods"
fi

print_header "CRONJOB & SCHEDULED TASK STATUS"
cron_json="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get cronjobs -n "${CG_NAMESPACE}" -o json 2>/dev/null || true)"
if [[ -z "$cron_json" ]]; then
  record_warn "Unable to fetch cronjobs from namespace ${CG_NAMESPACE}"
else
  failed_crons="$(python3.12 - <<'PY' "$cron_json"
import json,sys
raw=sys.argv[1]
try:
    j=json.loads(raw)
    bad=[]
    for item in j.get("items",[]):
        name=((item or {}).get("metadata") or {}).get("name","")
        st=(item or {}).get("status") or {}
        if not st.get("lastSuccessfulTime"):
            bad.append(name)
    print("\n".join([x for x in bad if x]))
except Exception:
    print("")
PY
)"
  if [[ -n "$failed_crons" ]]; then
    record_warn "CronJobs with no successful run detected"
    echo "$failed_crons"
  else
    record_pass "All CronJobs have at least one successful execution"
  fi
fi

if [[ "${CHECK_EPHEMERAL_DISK}" == "1" ]]; then
  print_header "EPHEMERAL STORAGE / DISK LEAK CHECKS"
  # Best-effort disk check on a representative deployment if present.
  if kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${CG_NAMESPACE}" get deploy nx-cg-inventory-manager-worker >/dev/null 2>&1; then
    disk_line="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${CG_NAMESPACE}" exec deploy/nx-cg-inventory-manager-worker -- df -h / 2>/dev/null | awk 'NR==2 {print $5}' || true)"
    if [[ -n "$disk_line" ]]; then
      log "Worker root disk used: ${disk_line}"
      disk_pct="$(echo "$disk_line" | tr -d '%' | tr -dc '0-9')"
      if [[ -n "$disk_pct" && "$disk_pct" -ge 90 ]]; then
        record_crit "High root disk usage on inventory manager worker (${disk_line})"
      elif [[ -n "$disk_pct" && "$disk_pct" -ge 80 ]]; then
        record_warn "Elevated root disk usage on inventory manager worker (${disk_line})"
      else
        record_pass "Root disk usage acceptable on inventory manager worker (${disk_line})"
      fi
    else
      record_warn "Could not read root disk usage from inventory manager worker"
    fi
  else
    record_warn "nx-cg-inventory-manager-worker deployment not found; skipped disk leak check"
  fi
fi

print_header "PROMETHEUS KPIS"
if [[ -n "$PROMETHEUS_BASE_URL" ]]; then
  log "Using Prometheus: ${PROMETHEUS_BASE_URL}"
  log "Window: ${PROM_QUERY_WINDOW} | Namespace label: ${PROM_NS_LABEL}"

  q_cpu="100 * sum(rate(container_cpu_usage_seconds_total{namespace=\"${PROM_NS_LABEL}\",container!=\"\",image!=\"\"}[${PROM_QUERY_WINDOW}])) / sum(kube_pod_container_resource_limits{namespace=\"${PROM_NS_LABEL}\",resource=\"cpu\",unit=\"core\"})"
  q_mem="100 * sum(container_memory_working_set_bytes{namespace=\"${PROM_NS_LABEL}\",container!=\"\",image!=\"\"}) / sum(kube_pod_container_resource_limits{namespace=\"${PROM_NS_LABEL}\",resource=\"memory\",unit=\"byte\"})"
  q_thr="100 * sum(rate(container_cpu_cfs_throttled_seconds_total{namespace=\"${PROM_NS_LABEL}\",container!=\"\",image!=\"\"}[${PROM_QUERY_WINDOW}])) / sum(rate(container_cpu_cfs_periods_total{namespace=\"${PROM_NS_LABEL}\",container!=\"\",image!=\"\"}[${PROM_QUERY_WINDOW}]))"
  q_5xx="100 * sum(rate(nginx_ingress_controller_requests{namespace=\"${PROM_NS_LABEL}\",status=~\"5..\"}[${PROM_QUERY_WINDOW}])) / sum(rate(nginx_ingress_controller_requests{namespace=\"${PROM_NS_LABEL}\"}[${PROM_QUERY_WINDOW}]))"
  q_p95="histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket{namespace=\"${PROM_NS_LABEL}\"}[${PROM_QUERY_WINDOW}])) by (le))"

  cpu_v="$(as_num "$(prom_query_value "$q_cpu")")"
  mem_v="$(as_num "$(prom_query_value "$q_mem")")"
  thr_v="$(as_num "$(prom_query_value "$q_thr")")"
  e5xx_v="$(as_num "$(prom_query_value "$q_5xx")")"
  p95_v="$(as_num "$(prom_query_value "$q_p95")")"

  for kv in "CPU Utilization %|$cpu_v|$WARN_CPU_UTIL_PCT|$CRIT_CPU_UTIL_PCT" \
            "Memory Utilization %|$mem_v|$WARN_MEM_UTIL_PCT|$CRIT_MEM_UTIL_PCT" \
            "CPU Throttling %|$thr_v|$WARN_THROTTLE_PCT|$CRIT_THROTTLE_PCT" \
            "API 5xx Rate %|$e5xx_v|$WARN_API_5XX_PCT|$CRIT_API_5XX_PCT" \
            "API p95 Latency sec|$p95_v|$WARN_API_P95_SEC|$CRIT_API_P95_SEC"; do
    name="${kv%%|*}"; rest="${kv#*|}"
    val="${rest%%|*}"; rest2="${rest#*|}"
    warn="${rest2%%|*}"; crit="${rest2#*|}"
    if [[ -z "$val" ]]; then
      record_warn "${name}: metric unavailable"
      continue
    fi
    log "METRIC: ${name}=${val} (warn>${warn}, crit>${crit})"
    if python3.12 - <<'PY' "$val" "$crit"
import sys
v=float(sys.argv[1]); c=float(sys.argv[2]); sys.exit(0 if v>=c else 1)
PY
    then
      record_crit "${name} critical: ${val}"
    elif python3.12 - <<'PY' "$val" "$warn"
import sys
v=float(sys.argv[1]); w=float(sys.argv[2]); sys.exit(0 if v>=w else 1)
PY
    then
      record_warn "${name} warning: ${val}"
    else
      record_pass "${name} healthy: ${val}"
    fi
  done

  print_header "PROMETHEUS POD-LEVEL KPIS"
  q_pod_cpu="sort_desc(sum by (pod) (rate(container_cpu_usage_seconds_total{namespace=\"${PROM_NS_LABEL}\",container!=\"\",image!=\"\"}[${PROM_QUERY_WINDOW}])))"
  q_pod_mem="sort_desc(sum by (pod) (container_memory_working_set_bytes{namespace=\"${PROM_NS_LABEL}\",container!=\"\",image!=\"\"}))"
  q_pod_restart="sort_desc(sum by (pod) (increase(kube_pod_container_status_restarts_total{namespace=\"${PROM_NS_LABEL}\"}[${PROM_QUERY_WINDOW}])))"

  log "Top pods by CPU cores (rate over ${PROM_QUERY_WINDOW}):"
  cpu_rows="$(prom_query_rows "$q_pod_cpu" "$PROM_TOP_PODS")"
  if [[ -n "$cpu_rows" ]]; then
    while IFS=$'\t' read -r pod val; do
      [[ -n "$pod" ]] || continue
      log "POD_CPU: ${pod} => ${val} cores"
    done <<< "$cpu_rows"
  else
    record_warn "Pod CPU metrics unavailable"
  fi

  log "Top pods by Memory working set (bytes):"
  mem_rows="$(prom_query_rows "$q_pod_mem" "$PROM_TOP_PODS")"
  if [[ -n "$mem_rows" ]]; then
    while IFS=$'\t' read -r pod val; do
      [[ -n "$pod" ]] || continue
      log "POD_MEM: ${pod} => ${val} bytes"
    done <<< "$mem_rows"
  else
    record_warn "Pod memory metrics unavailable"
  fi

  log "Pod restart increase over ${PROM_QUERY_WINDOW}:"
  restart_rows="$(prom_query_rows "$q_pod_restart" "$PROM_TOP_PODS")"
  restart_warn=0
  if [[ -n "$restart_rows" ]]; then
    while IFS=$'\t' read -r pod val; do
      [[ -n "$pod" ]] || continue
      log "POD_RESTART_DELTA: ${pod} => ${val}"
      if python3.12 - <<'PY' "$val"
import sys
v=float(sys.argv[1]); sys.exit(0 if v>0 else 1)
PY
      then
        restart_warn=1
      fi
    done <<< "$restart_rows"
    if [[ "$restart_warn" -eq 1 ]]; then
      record_warn "One or more pods restarted within ${PROM_QUERY_WINDOW}"
    else
      record_pass "No pod restart increase within ${PROM_QUERY_WINDOW}"
    fi
  else
    record_warn "Pod restart delta metrics unavailable"
  fi
else
  record_warn "PROMETHEUS_BASE_URL not set; skipped Prometheus KPI checks"
fi

print_header "SUMMARY"
log "PASS=${PASS_COUNT} WARN=${WARN_COUNT} CRITICAL=${CRIT_COUNT} FAIL=${FAIL_COUNT}"
if [[ "${CRIT_COUNT}" -gt 0 || "${FAIL_COUNT}" -gt 0 ]]; then
  log "OVERALL: UNHEALTHY"
elif [[ "${WARN_COUNT}" -gt 0 ]]; then
  log "OVERALL: DEGRADED"
else
  log "OVERALL: HEALTHY"
fi

print_header "DONE"
log "CG daily checks completed"
