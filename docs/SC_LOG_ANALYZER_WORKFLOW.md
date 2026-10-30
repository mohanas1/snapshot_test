# SC Network Log Analyzer — Architecture & Workflow

> **Source of truth.** Update this file whenever scripts, routes, parsers, or the UI change.

---

## Table of Contents

1. [Overview](#overview)
2. [Pipeline Architecture](#pipeline-architecture)
3. [Repository Files](#repository-files)
4. [Fetch Fresh vs Local Mode](#fetch-fresh-vs-local-mode)
5. [Multi-Pod Log Collection](#multi-pod-log-collection)
6. [Core Analyzer: `analyze_network_log_receiver.py`](#core-analyzer-analyze_network_log_receiverpy)
7. [Pipeline Service Parsers](#pipeline-service-parsers)
8. [Analysis Report Structure](#analysis-report-structure)
9. [Flask Routes & API](#flask-routes--api)
10. [Job Lifecycle](#job-lifecycle)
11. [SC Security Reports Analyzer](#sc-security-reports-analyzer)
12. [Frontend (sc.html)](#frontend-schtml)
13. [Job UI (job.html)](#job-ui-jobhtml)
14. [History & Storage](#history--storage)
15. [Configuration Reference](#configuration-reference)
16. [Output Files](#output-files)
17. [Troubleshooting](#troubleshooting)

---

## Overview

The SC Network Log Analyzer fetches logs from the Prism Central Kubernetes pods in the Security Central pipeline and produces a structured end-to-end analysis report. It covers the full pipeline:

```
PE (AHV IPFIX exporter)
  → ncm-network-log-receiver   (gRPC listener, batch publisher)
    → NATS JetStream (IPFIX-COLLECTOR-1 stream)
      → ncm-network-log-data-processor   (NDP, nx-network-log-pipeline)
        → DIIS   (ClickHouse sink)
          → ClickHouse (ncm.nx_ipfix_network_logs_3d_raw)
```

Two analyzer types exist:

| Type | Script | Input | Output |
|------|--------|-------|--------|
| **Network Log Analyzer** | `analyze_network_log_receiver.py` | Pod logs fetched from fluentd | JSON, CSV, Markdown, pipeline health report |
| **Security Reports Analyzer** | Same script (different entry point) | Local SC JSON files | Audit/query/VM assignment analysis |

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PE / AHV Host                                                              │
│  N-Flow IPFIX exporter → gRPC → port 8081                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ gRPC FlowBatch stream
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ncm-network-log-receiver  (ntnx-ncm-common namespace)                     │
│  • 2 replicas                                                               │
│  • NxDecoderPool: 30 workers                                                │
│  • BatchingPublisher: batchSize=35, flushInterval=0.5s                      │
│  • Rate limiter: 90,000 flows / 10s                                         │
│  • gRPC TCP listener on :8081                                               │
│  • Publishes to NATS subject: ipfix.collector.nx                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ NATS publish
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  NATS JetStream  (ntnx-ncm-datastore namespace)                            │
│  • Cluster: cpaas-nats (3-node)                                             │
│  • Stream: IPFIX-COLLECTOR-1                                                │
│  • Subject filter: ipfix.collector.nx                                       │
│  • Prometheus exporter: :7777/metrics                                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ JetStream pull
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ncm-network-log-data-processor  (ntnx-ncm-common namespace)               │
│  • 2 replicas                                                               │
│  • Pipeline: nx-network-log-pipeline                                        │
│  • Consumer: IPFIXConsumer1                                                 │
│  • Pull batchSize: 20, pullTimeout: 500ms                                   │
│  • 5 fan-out workers                                                        │
│  • Sends processed data to DIIS via gRPC (:8088)                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ gRPC IngestMetrics
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  DIIS  (ntnx-ncm-common namespace)                                         │
│  • 2 replicas                                                               │
│  • OTLP → ClickHouse mapping: ncm.nx_ipfix_network_logs_3d_raw             │
│  • gRPC server: :8088                                                       │
│  • Rate limits: 150K/min (LatestDataPoints), 10M/min (CommonDataPoints)    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ CH INSERT
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ClickHouse  (ncm schema)                                                   │
│  • Table: ncm.nx_ipfix_network_logs_3d_raw                                 │
│  • Columns: batch_id, flow_start, flow_end, source_ip, destination_ip,     │
│             destination_port, protocol_identifier, cluster_id, host_uuid,  │
│             nx_connection_status, rule_uuid, action, octet_count, …        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Files

```
snapshot_test/
├── analyze_network_log_receiver.py   ← Core analyzer (both analyzer types)
├── app.py                            ← Flask routes, workers, history
├── templates/
│   ├── sc.html                       ← SC page (launch jobs, recent jobs table)
│   └── job.html                      ← Job detail (pipeline overview, KPIs, charts)
└── data/
    └── sc_jobs_history.jsonl         ← Completed SC job records (full summary)
```

---

## Fetch Fresh vs Local Mode

### Fetch Fresh (`--fetch-fresh`, default in UI)

When enabled, the analyzer:
1. SSH into Prism Central (`--pc-ip`, `--pc-user`, `--pc-pass`).
2. Runs `kubectl get secret pc-kubeconfig -n ntnx-system -o jsonpath=...` to download the MSP kubeconfig.
3. Saves kubeconfig to `--kubeconfig` path (default: `<out-dir>/pc_kube.txt`).
4. Runs `kubectl --kubeconfig <kc> exec -n ntnx-system fluentd-aggregator-0 -- find /fluentd/data/logs -name file.log.log -path '*ncm-network-log-receiver*'` to discover log paths.
5. **Fetches all replica logs** using `kubectl cp` and concatenates them into `file.log`.
6. Repeats for extra pod keywords (data-processor, diis, nats).

### Local Mode (`--no-fetch-fresh`)
Uses whatever log files already exist on disk. The analyzer will:
- Look for `--receiver <path>` (required if different from the default).
- Look for the extra service logs in `--out-dir` first, then in the receiver log's parent directory.

---

## Multi-Pod Log Collection

Every service has 2 Kubernetes replicas. The `discover_all_log_paths()` function finds **all** log files matching the keyword, not just one:

```python
def discover_all_log_paths(kubeconfig, namespace, pod, base, keyword, filename) -> list[str]:
    # kubectl exec fluentd-aggregator-0 -- find /fluentd/data/logs -name file.log.log -path '*keyword*'
    # Returns ALL matching paths (one per replica pod)
```

Then `fetch_and_merge_logs()` fetches each path via `kubectl cp` and concatenates the chunks into a single local file, ensuring a newline boundary between replica chunks.

### Pod Keywords (configurable via `--pod-keywords`)

Default: `ncm-network-log-receiver,ncm-network-log-data-processor,diis,nats`

- **First keyword** = primary receiver log → saved as `file.log` for analysis.
- **Remaining keywords** = extra service logs → saved as `<keyword>.log` in `--out-dir`.

---

## Core Analyzer: `analyze_network_log_receiver.py`

### High-Level Flow

```
main()
  │
  ├─ fetch kubeconfig (if --fetch-fresh)
  ├─ discover_all_log_paths()  ← finds all replica log paths
  ├─ fetch_and_merge_logs()    ← kubectl cp + concatenate
  │   (for receiver + each extra pod keyword)
  │
  ├─ build(receiver_path, processor_path, fetch_meta,
  │         nats_log, data_processor_log, diis_log)
  │     ├─ load_json_lines()      ← parse JSON + strip K8s timestamps / ANSI
  │     ├─ pair_batches()         ← correlate receive + publish events
  │     ├─ aggregate()            ← hourly / daily stats
  │     ├─ detect_regime()        ← volume change detection
  │     ├─ error_log_entries()    ← filter error/warn records
  │     ├─ rate_analysis()        ← flows/minute, span
  │     ├─ reconcile()            ← cross-reference with processor log
  │     ├─ parse_nats_log()       ← NATS stream states + errors
  │     ├─ parse_data_processor_log() ← pipeline subscription info
  │     ├─ parse_diis_log()       ← CH config + insert events
  │     └─ _build_pipeline_section()
  │
  ├─ build_pipeline_report()  ← post-process into stage health + root cause
  │
  └─ write outputs:
       network_receiver_analysis.json
       network_receiver_hourly.csv
       network_receiver_batches.csv
       network_receiver_analysis.md
```

### `load_json_lines(path)`
Reads every line of the log file:
- Strips ANSI escape codes.
- Strips Kubernetes pod-log timestamp prefix (`2026-10-17T12:47:57.042809330Z `).
- Attempts `json.loads()` on each line; non-JSON lines go to the `plain` list.

### `pair_batches(records)`
Correlates "received a FlowBatch" events with their corresponding "published to NATS" events:
- Receive event: `msg = "FlowBatch received"` → extracts `batchUUID`, `numFlows`, `host`, `cluster`.
- Publish event: `msg = "Message published"` → extracts `subject`, `size`, `ack`, `lag_ms`.
- Returns a list of batch dicts, each with both sides joined on `batchUUID`.

### `aggregate(batches, width)`
Groups batches by `"hour"` or `"day"` and computes:
- `flows`, `bytes`, `batches`, `acks`, `empty_cluster`
- Per-field summaries: min, max, mean, median, p90, p95, p99
- `lag_ms_summary` for publish latency

### `detect_regime(hourly, jump_ratio=1.5)`
Scans the hourly time series for a >1.5× jump in flows/hour, indicating a change in the data sending rate.

### `reconcile(batches, terminations)`
Cross-references published batch UUIDs with `"Message Terminated"` events from the optional processor log (`netwrk_los.txt`). Returns `None` if no processor log is provided.

---

## Pipeline Service Parsers

### `parse_nats_log(path) → dict`

Parses NATS server log format: `[<pid>] YYYY/MM/DD HH:MM:SS.ms [LEVEL] message`

**Extracts:**
- `line_counts`: counts per level (INF/WRN/ERR)
- `errors` / `warnings`: filtered message list
- `streams`: dict of stream name → `{msgs, bytes, last_seq, ts}` from `Filestore [name]` state lines
- `ipfix_stream`: the `IPFIX-COLLECTOR-1` entry specifically
- `ipfix_stream_msgs`: message count in the IPFIX stream
- `cluster_leader`: JetStream cluster metadata leader
- `consumer_leaders`: list of stream→consumer leadership events

### `parse_data_processor_log(path) → dict`

Parses NDP structured JSON log (with ANSI codes + optional K8s prefix stripped).

**Extracts:**
- `pipeline_name`: typically `nx-network-log-pipeline`
- `stream_name`: `IPFIX-COLLECTOR-1`
- `consumer_name`: `IPFIXConsumer1`
- `subject`: `ipfix.collector.nx`
- `batch_size`: pull batch size (20)
- `worker_count`: fan-out worker count (5)
- `pipeline_started` / `workers_started` / `subscription_added`: boolean flags
- `msg_terminated`: count of ZK/IDF "Message Terminated" lifecycle events
- `errors` / `warnings`

### `parse_diis_log(path) → dict`

Parses DIIS mixed-format log (glog `I1017 12:23:04 file.go:222] msg` + zap tab-delimited + logrus).

**Extracts:**
- `ch_table`: ClickHouse target table name (from config line)
- `ch_inserts`: count of bulk insert events
- `ch_errors`: count of ClickHouse errors
- `grpc_port`: detected gRPC server port
- `gauge_updates`: Prometheus gauge metric update count
- `service_started`: whether the HTTP/gRPC server started
- `errors` / `warnings`

### `build_pipeline_report(receiver_result, nats_result, dp_result, diis_result) → dict`

Combines all 4 service results into a single pipeline health object:

```json
{
  "overall_status": "healthy|warn|degraded",
  "root_cause": "Human-readable explanation of the first failure",
  "stages": [
    {"name": "PE → Receiver", "label": "PE / AHV Host", "icon": "🖥",
     "status": "ok|warn|error|idle", "metric": "629,061 flows received", "note": null},
    {"name": "Receiver", ...},
    {"name": "NATS", ...},
    {"name": "Data Processor", ...},
    {"name": "DIIS → ClickHouse", ...}
  ],
  "receiver":       { "flows", "batches_received", "batches_published", "errors" },
  "nats":           { "available", "stream_msgs", "errors", "warnings", "cluster_leader" },
  "data_processor": { "available", "pipeline", "stream", "consumer", "subject",
                       "workers", "subscription_active", "msg_terminated", ... },
  "diis":           { "available", "ch_table", "ch_inserts", "ch_errors",
                       "grpc_port", "gauge_updates", ... }
}
```

**Root cause heuristics (in order):**
1. `recv_flows == 0` → PE not sending IPFIX data
2. `recv_pub == 0` → Receiver has flows but failed to publish
3. `"payload exceeded" in receiver errors` → NATS max_payload hit; reduce batch_size
4. `nats_msgs == 0 && recv_pub > 0` → Stream subject filter mismatch
5. `not dp_subs` → Data processor has no subscription
6. `ch_inserts == 0 && nats_msgs > 0` → DIIS not consuming from NDP

---

## Analysis Report Structure

The full report is written to `network_receiver_analysis.json`:

```json
{
  "fetch": {
    "fetch_fresh": true,
    "pc_ip": "10.x.x.x",
    "analysed_at": "...",
    "remote_log_paths": ["..."],
    "replica_count": 2,
    "extra_pod_logs": [{"keyword": "nats", "path": "...", "size_bytes": 6..., "replicas": 1}]
  },
  "source": {
    "receiver_log": "...",
    "json_lines": 436,
    "plain_lines": [],
    "levels": {"info": 430, "error": 2},
    "messages": [["FlowBatch received", 218], ...],
    "log_start": "2026-10-17T12:47:57.113Z",
    "log_end": "2026-10-18T07:38:xx.xxxZ"
  },
  "startup": {
    "pipeline_name": "NUTANIX_PIPELINE",
    "batch_size": 35,
    "flush_interval_sec": 0.5,
    "ingress_buffer": 20480,
    "decoder_workers": 30,
    "nats_url": "nats://cpaas-nats.ntnx-ncm-datastore:4222",
    "subject": "ipfix.collector.nx",
    "throughput_stats": [...]
  },
  "identity": {
    "hosts": [{"host": "...", "count": N}],
    "clusters": [...],
    "subjects": [...],
    "unique_batch_uuids": 436,
    "duplicate_batch_uuids": 0,
    "num_flows_mismatch_flow_record_count": 0
  },
  "totals": {
    "batches_received": 436,
    "batches_published": 435,
    "flows": 629061,
    "bytes": 520832105,
    "gibibytes": 0.485,
    "flows_summary": {"count": 436, "mean": 1443.0, "median": ...},
    "bytes_summary": {...},
    "lag_ms_summary": {...},
    "publish_lag_ms_summary": {...}
  },
  "rate_analysis": {
    "avg_flows_per_minute": 723.06,
    "span_minutes": 870.0
  },
  "hourly": [ { "hour": "2026-10-17T13", "batches": 12, "flows": 5000, ... } ],
  "daily":  [ { "day": "2026-10-17", "batches": 436, "flows": 629061, ... } ],
  "flows_histogram": [ {"range": "0–200", "count": 5} ],
  "error_log_entries": [
    { "line": 40, "level": "error", "msg": "Error publishing message to subject",
      "error": "nats: maximum payload exceeded", "subject": "ipfix.collector.nx" }
  ],
  "volume_regime": { "jump": {"from_hour": "...", "to_hour": "...", "ratio": 3.2} },
  "receive_gaps_outside_100_to_130_seconds": [...],
  "per_host": [...],
  "per_cluster": [...],
  "processor_reconciliation": null,
  "samples": { "first": {...}, "last": {...}, "largest_by_flows": {...}, "largest_by_bytes": {...} },
  "pipeline": {
    "overall_status": "warn",
    "root_cause": "...",
    "stages": [...],
    "receiver": {...},
    "nats": {...},
    "data_processor": {...},
    "diis": {...}
  }
}
```

---

## Flask Routes & API

### SC page

| Method | Path | Function | Description |
|--------|------|----------|-------------|
| `GET` | `/sc` | `sc_page()` | Render SC page with recent jobs table |
| `POST` | `/api/sc/test_connection` | `api_sc_test_connection()` | SSH test to PC |
| `POST` | `/api/sc/network_log_analyzer/start` | `api_sc_network_log_analyzer_start()` | Start network log analysis job |
| `POST` | `/api/sc/security_reports/start` | `api_sc_security_reports_start()` | Start security reports analysis |
| `GET` | `/api/sc/jobs/recent` | `api_sc_jobs_recent()` | JSON list of recent SC jobs |
| `GET` | `/download/sc_analysis/<run_id>/<filename>` | `download_sc_analysis_file()` | Download output files |
| `GET` | `/job/<run_id>` | `job_status()` | Job detail page |
| `GET` | `/api/job/<run_id>` | `api_job()` | Poll job status + log |

### `api_sc_network_log_analyzer_start` payload

```json
{
  "pc_ip": "10.x.x.x",
  "username": "nutanix",
  "password": "nutanix/4u",
  "fetch_fresh": true,
  "log_file": "",           // path if fetch_fresh=false
  "processor_file": "",     // optional netwrk_los.txt path
  "kubeconfig": "",         // optional: reuse existing kubeconfig
  "pod_keywords": "ncm-network-log-receiver,ncm-network-log-data-processor,diis,nats"
}
```

---

## Job Lifecycle

```
POST /api/sc/network_log_analyzer/start
  │
  ├─ _enqueue_sc_network_analyzer_run(payload)
  │     ├─ _allocate_run_id_and_path("sc_na")
  │     │     → run_id = "sc-na_YYYYMMDDTHHMMSS_<random>"
  │     │     → log_path = logs/runs/run_sc-na_<run_id>.log
  │     ├─ out_dir = RUN_LOG_DIR / run_id   ← per-job output directory
  │     ├─ runs[run_id] = {status:"queued", job_kind:"sc_network_analyzer", ...}
  │     └─ threading.Thread(target=_worker).start()
  │
  └─ returns {ok: true, run_id, job_url}

_worker():
  ├─ status = "running"
  ├─ Build cmd:
  │     python3 analyze_network_log_receiver.py
  │       --fetch-fresh / --no-fetch-fresh
  │       --pc-ip --pc-user --pc-pass
  │       --kubeconfig <out_dir>/pc_kube.txt
  │       --receiver  <out_dir>/file.log
  │       --pod-keywords ncm-network-log-receiver,...
  │       --out-dir  <out_dir>
  ├─ subprocess.Popen(cmd, stdout=log_path, stderr=log_path)
  ├─ proc.wait()
  ├─ if exit_code == 0:
  │     parse <out_dir>/network_receiver_analysis.json
  │     runs[run_id]["summary"] = {
  │       "pc_ip", "fetch_fresh", "analysis_result": <full JSON>,
  │       "output_dir", "json_output", "hourly_csv", "batches_csv", "markdown_report"
  │     }
  ├─ append_record(SC_JOBS_HISTORY_FILE, {run_id, status, summary, ...})
  └─ append_record(HISTORY_FILE, {run_id, job_kind:"sc_network_analyzer", ...})
```

### Output Directory Layout

```
logs/runs/sc-na_20261018T073844_411716/
├── file.log                          ← merged receiver log (all replicas)
├── nats.log                          ← merged NATS log
├── ncm-network-log-data-processor.log
├── diis.log
├── pc_kube.txt                       ← kubeconfig (reused on next run)
├── network_receiver_analysis.json    ← full analysis report
├── network_receiver_hourly.csv       ← one row per hour
├── network_receiver_batches.csv      ← one row per batch
└── network_receiver_analysis.md      ← human-readable summary
```

### Run ID Format
```
sc-na_20261018T073844_411716
└─── prefix
         └─── UTC timestamp
                       └─── 6-digit random suffix
```

---

## SC Security Reports Analyzer

A second analyzer mode reads local Security Central JSON files:

### Input Files (from `SC_SECURITY_DATA_DIR`)
```
security_central/
├── sc_categories_and_vms.json    ← VM→category assignments
├── audits_list.json              ← audit rules
└── query_results.json            ← query coverage results
```

### What it analyses
- **VM Assignment**: total unique VMs, assignment counts per category
- **Audits**: total audit rules, pass/fail counts, rule coverage
- **Queries**: query success rates, query types

### Route
```
POST /api/sc/security_reports/start
  { "data_dir": "/path/to/security_central" }
```

### Job Kind
`job_kind = "sc_security_analyzer"` — distinguished from network log analysis in the Recent SC Jobs table via a "Security Reports" badge.

---

## Frontend (sc.html)

### PC Connection Card
- Fields: PC IP, SSH Username, SSH Password
- **Test Connection** button → `POST /api/sc/test_connection`
  - Returns `connection_time` and `remote_date` on success.

### Network Log Analyzer Card
- **Fetch Fresh toggle**: when on, kubeconfig + logs are pulled from PC; when off, uses local paths.
- Local Log Path row: hidden when Fetch Fresh is enabled.
- Optional fields: Processor Log path, Kubeconfig path (for reuse).
- **Run Analysis** → `POST /api/sc/network_log_analyzer/start`; on success, redirects to job page.

### SC Security Reports Analyzer Card
- Data Directory field (path to `security_central/` folder).
- **Analyse SC Reports** → `POST /api/sc/security_reports/start`.

### Recent SC Jobs Table
- **Data source**: `sc_page()` reads directly from `SC_JOBS_HISTORY_FILE` (`sc_jobs_history.jsonl`) for full summary data, plus in-memory `runs` dict for active jobs.
- Shows last 30 jobs, newest first.
- **Details column**:
  - Network Log: `Batches: <batches_received>`
  - Security Reports: `VMs: N · Audits: N · Queries: N`
- **Fallback**: amber "no report data" text when `analysis_result` is empty.
- **Scroll container**: `max-height: 450px` with sticky headers.

---

## Job UI (job.html)

The job detail page at `/job/<run_id>` uses `info.summary.analysis_result` to render all SC sections.

### Template Variable Chain
```jinja
{% set sc_summary = info.get('summary', {}) %}
{% set sc_result  = sc_summary.get('analysis_result', {}) %}
{% set pipeline   = sc_result.get('pipeline') %}
{% set totals     = sc_result.get('totals', {}) %}
{% set hourly     = sc_result.get('hourly', []) %}
```

### Section 0: Pipeline Overview
Rendered when `sc_result.pipeline` is present.

**Horizontal pipeline flow bar**: Each stage shown as a tile:
```
🖥 PE/AHV     →   📥 Receiver   →   📡 NATS   →   ⚙️ NDP   →   🗄 DIIS→CH
✅ 629K flows     ⚠ 435 pub         ✅ 1 msg       ✅ subscribed  ⚠ 0 inserts
                  ↳ payload limit                               ↳ stalled upstream
```

**Root cause banner**: Yellow highlight with detected failure explanation.

**Per-service detail cards**:
- Receiver: flows from PE, batches received/published, errors
- NATS: stream message count, cluster leader, errors/warnings
- Data Processor: pipeline name, stream, consumer, subject, worker count, subscription status
- DIIS: CH table name, insert count, error count, gRPC port, gauge updates

### Section 1: KPI Overview
Large metric cards: Batches Received, Batches Published, Total Flows, Total Bytes, Avg Lag, Avg Flows/Batch, Unique Hosts, Clusters.

### Section 2: Hourly Flow Rate Chart
Canvas-based chart of flows/hour and publish lag over time. Uses Chart.js-like custom canvas rendering.

### Section 3: Daily Aggregated Table
One row per calendar day.

### Section 4: Publish Lag Percentiles
Grouped bar chart: median, p90, p95, p99 for each day.

### Section 5: Batch-Size Distribution
Histogram of flows-per-batch.

### Section 6: Volume Regime Change
Alert box when `detect_regime()` finds a >1.5× jump in hourly volume.

### Section 7: Error Entries / Identity
Table of error/warn log lines. Identity section: host/cluster/subject distribution, UUID stats.

### Section 8: Notable Batches
First, last, largest-by-flows, largest-by-bytes.

### Section 9: Hourly Breakdown (accordion)
Expandable table with 5-minute sub-intervals (fetched on-demand via API).
- Wrapped in `.table-scroll-container` for max-height 450px vertical scroll.
- Sticky `<thead>` via CSS.
- When `hourly` is empty but `sc_result` exists: shows "⚠ No hourly data available" message.

### Export Bar
Download links for: JSON, Hourly CSV, Batches CSV, Markdown.

---

## History & Storage

| File | Contents | Written by |
|------|----------|-----------|
| `data/sc_jobs_history.jsonl` | Full records: run_id, status, summary (with full analysis_result), log_path | `_enqueue_sc_network_analyzer_run` worker |
| `data/run_history.jsonl` | Lightweight index: run_id, job_kind, at, duration_sec | Same worker (minimal, no summary) |
| `logs/runs/run_sc-na_<id>.log` | stdout+stderr of analyzer script | subprocess.Popen |
| `logs/runs/sc-na_<id>/` | Per-job output dir with all log + output files | analyzer script |

### Why Two History Files?
`sc_jobs_history.jsonl` stores the **full** `analysis_result` (needed by `job_status()` to rebuild the UI after server restart). `run_history.jsonl` stores only minimal fields for the lightweight dashboard on the index page.

### `sc_page()` Data Loading
```python
# 1. Active jobs (queued/running) from in-memory runs dict
# 2. Completed jobs from sc_jobs_history.jsonl (has full summary)
# Sort newest first, cap at 30
```

### `_get_run_info()` for SC Jobs
When the job page is accessed after a server restart, the in-memory `runs` dict is empty. `_get_run_info()` checks `sc_jobs_history.jsonl` and returns the complete record including `summary.analysis_result`.

---

## Configuration Reference

### Script-Level Defaults (`analyze_network_log_receiver.py`)

```python
PC_IP              = "10.120.10.9"
PC_SSH_USER        = "nutanix"
PC_SSH_PASS        = "nutanix/4u"
FETCH_FRESH        = True
KUBECONFIG_FILE    = Path("pc_kube.txt")
RECEIVER_LOG_FILE  = Path("file.log")
PROCESSOR_LOG_FILE = Path("netwrk_los.txt")

# Fluentd pod
FLUENTD_NAMESPACE   = "ntnx-system"
FLUENTD_POD         = "fluentd-aggregator-0"
FLUENTD_LOG_BASE    = "/fluentd/data/logs"
LOG_RECEIVER_FILENAME = "file.log.log"

# Primary pod keyword
LOG_RECEIVER_KEYWORD  = "ncm-network-log-receiver"

# Extra pod keywords
EXTRA_POD_KEYWORDS = [
    "ncm-network-log-data-processor",
    "diis",
    "nats",
]
MSP_CLUSTER_NAME   = "nc"
```

### app.py Constants

```python
SC_ANALYZER_SCRIPT_PATH = Path(__file__).parent / "analyze_network_log_receiver.py"
SC_JOBS_HISTORY_FILE    = PROJECT_DIR / "data" / "sc_jobs_history.jsonl"
SC_SECURITY_DATA_DIR    = Path("/home/mohan.as1/mohan_helpers/security_central")
```

### CLI Arguments (most commonly used)

| Argument | Default | Description |
|----------|---------|-------------|
| `--fetch-fresh` | on | Pull kubeconfig + logs from PC before analysing |
| `--no-fetch-fresh` | — | Use local files only |
| `--pc-ip` | `10.120.10.9` | Prism Central IP |
| `--pc-user` | `nutanix` | SSH username |
| `--pc-pass` | `nutanix/4u` | SSH password |
| `--kubeconfig` | `pc_kube.txt` | Path to save/reuse kubeconfig |
| `--receiver` | `file.log` | Local receiver log path |
| `--processor` | `netwrk_los.txt` | Optional processor log |
| `--pod-keywords` | `ncm-network-log-receiver,...` | Comma-separated pod keyword list |
| `--out-dir` | `.` | Output directory for all generated files |

---

## Output Files

### `network_receiver_analysis.json`
Full analysis report (see [Analysis Report Structure](#analysis-report-structure)).

### `network_receiver_hourly.csv`
```csv
hour,batches,flows,flows_min,flows_mean,flows_max,bytes,bytes_per_flow_mean,lag_ms_median,lag_ms_p95,lag_ms_max,acks,empty_cluster
2026-10-17T13,12,5400,400,450.0,510,...
```

### `network_receiver_batches.csv`
One row per received batch:
```csv
received_at,batch_uuid,num_flows,flow_record_count,host,cluster,
published,subject,size_bytes,lag_ms,ack,
publish_error
```

### `network_receiver_analysis.md`
Human-readable Markdown report with tables for:
- Source info, startup config
- Totals, rate analysis
- Hourly breakdown
- Batch-size histogram
- Identity (hosts, clusters, subjects)
- Error entries
- Processor reconciliation (if available)

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `hourly: 0, batches: 0` in recent jobs | Log file has no valid JSON records | Check receiver log — PE may not be sending IPFIX; or K8s prefix not stripped |
| `No SC jobs yet.` on SC page | `sc_jobs_history.jsonl` is empty or jobs haven't been run yet | Run the analyzer from the SC page |
| "Recent SC Jobs" shows "no report data" | Analysis ran but found 0 flows | Expected when receiver has no data; check pipeline |
| Hourly Breakdown section missing | `hourly` array is empty | Analyzer ran but log had no batch records; see error_log_entries for clues |
| Job page 404 after server restart | run_id uses `sc_na_` instead of `sc-na_` prefix | Fixed by canonical redirect in `job_status()` |
| `Could not find 'file.log.log'` | Pod keyword doesn't match any fluentd path | Verify pod name in cluster with `kubectl get pods -n ntnx-ncm-common` |
| `nats: maximum payload exceeded` | One receiver batch exceeds NATS max_payload | Reduce `batchSize` in the receiver's pipeline config |
| NATS stream shows 0 messages despite published batches | Subject filter mismatch | Verify `IPFIX-COLLECTOR-1` stream subject filter includes `ipfix.collector.nx` |
| DIIS shows 0 CH inserts | NDP not consuming from NATS | Check NDP subscription status; `subscription_active` should be true |
| Kubeconfig fetch hangs | SSH key exchange timeout | Increase timeout; verify PC SSH is accessible |
| Extra pod logs missing after fetch | Pod keyword not found in fluentd path | Confirm pod is running: `kubectl get pods -n ntnx-ncm-common \| grep <keyword>` |
| Pipeline section absent in job.html | Extra logs not present when analysis ran | Re-run with `--fetch-fresh`; ensure all 4 pod keywords are fetched |
