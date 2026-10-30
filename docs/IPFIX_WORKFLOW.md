# IPFIX Traffic Injection — Architecture & Workflow

> **Source of truth.** Update this file whenever the scripts, routes, or UI change.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Core Scripts](#core-scripts)
4. [Traffic Modes](#traffic-modes)
5. [Parallel Modes & Flow Patterns](#parallel-modes--flow-patterns)
6. [Persistence Modes](#persistence-modes)
7. [VM Selection & IP File Management](#vm-selection--ip-file-management)
8. [Port & Protocol Configuration](#port--protocol-configuration)
9. [Flask API Routes](#flask-api-routes)
10. [Job Lifecycle](#job-lifecycle)
11. [Data Flow: UI → API → Worker → Script](#data-flow-ui--api--worker--script)
12. [ipfix\_live.py Internals](#ipfix_livepy-internals)
13. [ipfix\_test.py Internals](#ipfix_testpy-internals)
14. [Signal Handling & Graceful Teardown](#signal-handling--graceful-teardown)
15. [History & Persistence Storage](#history--persistence-storage)
16. [Job UI (job.html)](#job-ui-jobhtml)
17. [Frontend (ipfix.html)](#frontend-ipfixhtml)
18. [Configuration Reference](#configuration-reference)
19. [Troubleshooting](#troubleshooting)

---

## Overview

The IPFIX Traffic Injection system creates real or mock network flows between VMs in a Nutanix cluster so that Security Central can detect and analyze them via IPFIX telemetry.

Two traffic modes exist:

| Mode | Script | Mechanism | Use case |
|------|--------|-----------|----------|
| **Live** | `ipfix_live.py` | Real TCP/UDP sockets via SSH into VMs | Actual IPFIX records in SC |
| **Mock** | `ipfix_test.py` | `conntrack -I` entries injected on AHV host | Fast testing without real traffic |

The Flask app (`app.py`) provides the web UI, REST API, and background worker that runs either script as a subprocess.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│  Browser (ipfix.html)                           │
│  • Fetch VMs  (POST /api/ipfix/fetch_vms)       │
│  • Start Job  (POST /api/ipfix/start)           │
│  • Monitor    (GET  /api/job/<run_id>)          │
└────────────────────┬────────────────────────────┘
                     │ JSON payload
                     ▼
┌─────────────────────────────────────────────────┐
│  app.py  Flask routes                           │
│  _build_ipfix_command()  → cmd list             │
│  _enqueue_ipfix_run()    → run_id               │
│  background thread → subprocess.Popen()        │
└────────────┬────────────────────────────────────┘
             │
     ┌───────┴────────┐
     │                │
     ▼                ▼
ipfix_live.py    ipfix_test.py
(live mode)      (mock mode)
     │                │
     │ SSH             │ SSH
     ▼                ▼
  VM sockets       AHV host
  (TCP/UDP)        conntrack -I / -U
     │                │
     └───────┬─────────┘
             │ IPFIX export
             ▼
      Security Central
      (ncm-network-log-receiver → NATS → NDP → DIIS → ClickHouse)
```

---

## Core Scripts

### `ipfix_live.py`
- Opens **real TCP/UDP sockets** between pairs of VMs via SSH (paramiko).
- Each pair runs a Python socket server+client for `--duration` seconds.
- Falls back to iperf3 if available on both VMs.
- Supports keep-alive (persistent worker threads) and recurring injection loops.

### `ipfix_test.py`
- Injects **mock conntrack entries** on the AHV hypervisor using the `conntrack` CLI.
- Does not require actual network traffic — creates kernel flow table entries directly.
- Requires SSH access to the AHV host (either directly as root or via CVM → root jump).
- Supports `conntrack -U` to refresh existing entries (keep-alive).

### `app.py` (relevant sections)
- Constants: `IPFIX_SCRIPT_PATH` (mock), `IPFIX_LIVE_SCRIPT_PATH` (live)
- Command builder: `_build_ipfix_command(payload)`
- Enqueuer/worker: `_enqueue_ipfix_run(cmd, summary)`
- Routes: `ipfix_traffic()`, `api_ipfix_fetch_vms()`, `api_ipfix_start()`, `api_job_cancel()`

---

## Traffic Modes

### Live Mode (`traffic_mode: "live"`)

Script: `ipfix_live.py`

**What happens:**
1. Reads VM IPs from the saved IP file.
2. SSH into each VM (paramiko) to open a server socket.
3. SSH into source VMs to launch a client that connects to the server.
4. Real TCP/UDP data is transferred for `--duration` seconds.
5. AHV's N-Flow / IPFIX exporter records the flow and sends it to the receiver.

**Key arguments built by `_build_ipfix_command`:**

```
python3 ipfix_live.py
  --proto          tcp|udp|both
  --parallel-mode  <mode>
  --parallel / --no-parallel
  --timeout        <ssh_timeout_seconds>
  --pattern        <flow_pattern>
  --duration       <seconds_per_flow>
  --ssh-user       <user>
  --ssh-password   <password>
  --ports          <dest_ports>        (or --tcp-ports / --udp-ports)
  --sports         <source_ports>
  --thread-count   <N>
  --ip-file        <path>
  # Persistence:
  --keep-alive                         (keepalive / duration modes)
  --keepalive-interval <seconds>
  --duration-limit     <seconds>
  --loop-count     <N>                 (recurring mode)
  --loop-interval  <seconds>
```

### Mock Mode (`traffic_mode: "mock"`)

Script: `ipfix_test.py`

**What happens:**
1. Optionally fetches VM IPs from Prism Element API (`/vms`).
2. SSH into CVM → `ssh root@<ahv-host>` → run `conntrack -I` for each flow.
3. Entries appear in kernel's conntrack table; IPFIX exporter treats them as real flows.
4. Keep-alive uses `conntrack -U` to refresh entries before they expire.

**Key arguments built by `_build_ipfix_command`:**

```
python3 ipfix_test.py
  --pc_ip      <prism_ip>
  --username   <admin>
  --password   <pass>
  --proto      tcp|udp|both
  --sport      <source_port>
  --sports     <port_range>
  --dports     <dest_ports>
  --tcp-ports / --udp-ports
  --tcp_state  ESTABLISHED
  --status     SEEN_REPLY,ASSURED
  --parallel-mode <mode>
  --parallel / --no-parallel
  --thread-count  <N>
  --cvm_user   <nutanix>
  --cvm_password  <pass>
  --host_ip    <ahv_host>       (optional override)
  --update-existing / --no-update-existing
  --inspect_only               (skip inject, only inspect)
  --ips_file   <path>
  --skip_fetch                 (use file, skip Prism API)
  # Persistence:
  --keep-alive
  --keepalive-interval <seconds>
  --duration-limit     <seconds>
  --loop-count     <N>
  --loop-interval  <seconds>
  --flush-mock                 (delete all injected flows)
```

---

## Parallel Modes & Flow Patterns

### Parallel Modes

Parallel modes control **how VMs are paired** into flows and how many run concurrently. The mode drives default `parallel` flag and `thread_count`. Both scripts share the same mode names.

| Mode | Parallel | Default Threads | Description |
|------|----------|-----------------|-------------|
| `sequential` | No | 1 | One flow at a time |
| `same_vm` | Yes | 5 | One source, one dest, multiple ports at once |
| `unique_vms` | Yes | 20 (live) / 5 (mock) | Each running flow uses a pair of VMs not used by any other |
| `fan_out` | Yes | 5 | One source → many destinations concurrently |
| `fan_in` | Yes | 10 | Many sources → one destination concurrently |
| `spread` | Yes | 5 | Any flows concurrently; VMs may appear in multiple flows |
| `first_to_all` | Yes | 10–50 | First IP sends to every other VM (locks pattern to `1:all`) |
| `all_to_first` | Yes | 10–50 | Every VM sends to the first IP (locks pattern to `all:1`) |
| `randomized_mesh` | Yes | 10 | All flows built then shuffled, run randomly |
| `client_server_pair` | Yes | 10 | First half → second half of the IP list |
| `port_scan_burst` | Yes | 50 | One src/dst pair, all ports spread across threads |
| `nn` | Yes | 50 | All srcs × all dsts, all ports (cross-product) |

> **Pattern locking**: `first_to_all` forces `1:all`, `all_to_first` forces `all:1`, `nn` forces `n:n`. All other modes use the user-selected pattern.

### Flow Patterns

The pattern determines how VMs are assigned to src/dst roles within a batch:

| Pattern | Pairs generated | Formula |
|---------|----------------|---------|
| `1:1` | ⌊n/2⌋ | VM[0]→VM[1], VM[2]→VM[3], … |
| `1:n` | min(N, n−1) | VM[0] → up to N others |
| `n:1` | min(N, n−1) | Up to N others → VM[last] |
| `1:all` | n−1 | VM[0] → every other VM |
| `all:1` | n−1 | Every other VM → VM[0] |
| `n:n` | ⌊n/2⌋ × ⌈n/2⌉ | First half srcs × second half dsts |
| `mesh` | ⌊n/2⌋ | (live only) Same as 1:1 in practice |

### Wave Batching

Both scripts use `build_waves(mode, flows/jobs, width)` to split the full list into batches that respect the parallelism constraint (e.g., `unique_vms` ensures no IP appears twice in the same wave).

---

## Persistence Modes

| Mode | Flag | Behaviour |
|------|------|-----------|
| `oneshot` | (none) | Inject once and exit |
| `keepalive` | `--keep-alive` | Repeat indefinitely; refresh with `conntrack -U` (mock) or re-open sockets (live); stop via SIGUSR1/SIGTERM |
| `duration` | `--keep-alive --duration-limit N` | Keep-alive that auto-stops after N seconds |
| `recurring` | `--loop-count N --loop-interval S` | Full re-injection from scratch, N times, with S-second gaps between loops |

### Keep-Alive (live mode)
In `ipfix_live.py`, keep-alive uses **autonomous persistent worker threads** — one thread per flow. Each worker:
1. Acquires `active_flow_semaphore` (limits concurrency to `thread_count`).
2. Opens a real TCP/UDP connection for `--duration` seconds.
3. Releases the semaphore, waits `--keepalive-interval` seconds.
4. Loops until `_TEARDOWN_REQUESTED` is set.

### Keep-Alive (mock mode)
In `ipfix_test.py`, keep-alive:
1. Runs `execute_keep_alive_mode()` which refreshes all entries via `conntrack -U`.
2. Waits `keepalive_interval` seconds between full refresh passes.
3. Respects `SIGUSR1` for graceful teardown.

### Recurring Mode (both modes)
After the first full injection cycle:
1. Waits `loop_interval` seconds (interruptible second-by-second).
2. Re-runs `execute_one_shot_mode()` (live) or the injection batch loop (mock).
3. Repeats until `loop_count` is reached or `_TEARDOWN_REQUESTED` fires.

---

## VM Selection & IP File Management

### Fetching VMs
Route: `POST /api/ipfix/fetch_vms`

The UI sends PC IP + credentials. `app.py` calls `ipfix_test.ipfix.fetch_all_vm_ips()` which:
1. `GET /vms` on Prism Element (paged by `page_size=50`, filter `is_control_domain!=1;is_cvm==0`).
2. Returns IPs with VM names and powered-on status.

### Saving the IP File
When the user clicks "Start Injection", `app.py` calls `_write_ipfix_ip_file(selected_ips)`:
- Writes IPs to `data/ipfix_vm_lists/selected_<hash>.txt`.
- Hash is the MD5 of the sorted IPs, so identical sets reuse the same file.
- Both scripts read this file at startup.

---

## Port & Protocol Configuration

### Source Ports
- Default: auto-assign (OS picks ephemeral port).
- Fixed: `--sport N` (mock) / `--sports N` (live).
- Range / list: `--sports 8000-8010` or `--sports 80,443,8080`.

### Destination Ports
- Live mode defaults: `80,443,53,123,5201` (via `--ports`).
- Mock mode defaults: `80,443,22,3306,53,123,161` (via `--dports`).
- Override per-protocol: `--tcp-ports` / `--udp-ports` (both modes).

### UI Port Presets
The UI (`ipfix.html`) offers presets:

| Preset label | Ports |
|---|---|
| Default common | 80, 443, 53, 123, 5201 |
| HTTP/HTTPS | 80, 443 |
| Database | 3306, 5432, 1433 |
| Custom | User-entered |

The Flow Projection Banner calculates estimated flows in real time as settings change.

---

## Flask API Routes

| Method | Path | Function | Description |
|--------|------|----------|-------------|
| `GET` | `/ipfix` | `ipfix_traffic()` | Render the IPFIX UI page |
| `POST` | `/api/ipfix/fetch_vms` | `api_ipfix_fetch_vms()` | Fetch VM IPs from Prism |
| `POST` | `/api/ipfix/start` | `api_ipfix_start()` | Build command + start background job |
| `GET` | `/api/job/<run_id>` | `api_job()` | Poll job status, log tail |
| `POST` | `/api/job/<run_id>/cancel` | `api_job_cancel()` | Send SIGTERM to worker process |
| `POST` | `/api/ipfix/teardown/<run_id>` | `api_ipfix_teardown()` | Send SIGUSR1 for graceful stop |
| `POST` | `/api/ipfix/flush/<run_id>` | `api_ipfix_flush()` | Send SIGUSR1 in flush mode (mock) |

---

## Job Lifecycle

```
POST /api/ipfix/start
  │
  ├─ _build_ipfix_command(payload)   ← validates + builds argv
  ├─ _enqueue_ipfix_run(cmd, summary)
  │     ├─ _allocate_run_id_and_path("ipfix")
  │     │     → run_id = "ipfix_YYYYMMDDTHHMMSS_<random>"
  │     │     → log_path = logs/runs/run_<run_id>.log
  │     ├─ runs[run_id] = {status:"queued", ...}  ← in-memory dict
  │     └─ threading.Thread(target=_worker).start()
  │
  └─ returns {ok: true, run_id, job_url}

_worker():
  ├─ runs[run_id]["status"] = "running"
  ├─ open log_path for writing
  ├─ write "IPFIX live/mock traffic job: <cmd>" to log
  ├─ subprocess.Popen(cmd, stdout=log_fh, stderr=log_fh)
  ├─ proc.wait()   ← blocks until script exits
  ├─ exit_code = 0 → status="complete"
  │   exit_code ≠ 0 → status="error"
  │   cancel_event  → status="aborted"
  ├─ append_record(IPFIX_JOBS_HISTORY_FILE, {...})
  └─ append_record(HISTORY_FILE, {...})

GET /api/job/<run_id>
  → returns {status, log (last 200 KB), summary, skipped_vms_summary, ...}

POST /api/job/<run_id>/cancel
  → proc.send_signal(SIGTERM)
  → cancel_event.set()
```

### Run ID Format
```
ipfix_20261018T042441_137922
└──── prefix
            └──── UTC timestamp (YYYYMMDDTHHmmss)
                             └──── 6-digit random suffix
```

---

## Data Flow: UI → API → Worker → Script

```
ipfix.html
  ├─ collectPayload()          ← reads all form fields
  │     {traffic_mode, pc_ip, proto, pattern, parallel_mode,
  │      selected_ips, ports, persistence_mode, ...}
  │
  ├─ POST /api/ipfix/start
  │
app.py _build_ipfix_command(payload)
  ├─ Determines script (live vs mock)
  ├─ Validates all fields with _ipfix_token(), _ipfix_positive_int(), etc.
  ├─ Writes selected IPs to data/ipfix_vm_lists/selected_<hash>.txt
  ├─ Builds argv list (cmd)
  └─ Returns (cmd, summary dict)

_enqueue_ipfix_run(cmd, summary)
  ├─ Starts background thread
  └─ Thread runs: subprocess.Popen(cmd) → reads log → polls status

ipfix_live.py / ipfix_test.py
  ├─ parse_vm_ips() / load IP file
  ├─ build_directions() / build_flows()  ← pairs VMs
  ├─ build_waves()                       ← batches by concurrency
  ├─ execute_one_shot_mode()             ← runs flows
  │     or execute_keep_alive_mode()
  └─ print_summary(results)             ← written to log file
```

---

## ipfix\_live.py Internals

### Main Execution Path

```python
main()
  │
  ├─ parse_vm_ips()           # load IPs from --ip-file
  ├─ connect() × n            # SSH into every VM, check python/iperf3
  ├─ build_directions()       # create (src, dst, proto, port, sport) tuples
  ├─ build_waves()            # split into batches respecting parallelism mode
  │
  ├─ if args.keep_alive:
  │     execute_keep_alive_mode()
  │       ├─ autonomous_worker() × num_flows  (each is a thread)
  │       │     while not _TEARDOWN_REQUESTED:
  │       │       acquire semaphore
  │       │       flow_once(src, dst, proto, port, duration, …)
  │       │       release semaphore
  │       │       sleep keepalive_interval
  │       └─ join all threads
  │
  └─ else:
        execute_one_shot_mode()
          for wave in waves:
            ThreadPoolExecutor(max_workers=thread_count)
              └─ run_job(job) for each job in wave
        if args.loop_count > 1:
          for loop_num in 2..loop_count:
            sleep loop_interval (interruptible)
            execute_one_shot_mode()
```

### `flow_once()` (single TCP/UDP flow)
1. SSH into source VM, start Python socket server on `(proto, port)`.
2. SSH into destination VM, launch client connecting to server.
3. Wait `duration` seconds for traffic to flow.
4. Collect result `{src, dst, proto, port, success, bytes_sent, elapsed, error}`.

### `build_directions(sessions, pattern, n)`
Converts the flat session list into `(src_session, dst_session, proto, port, sport)` tuples by applying the chosen pattern. Called once per job submission.

### `build_waves(mode, jobs, width)`
Splits the job list into batches using mode-specific grouping:
- `unique_vms`: IP-disjoint groups (no IP appears twice per wave).
- `fan_out/fan_in`: Groups by shared source/dest IP.
- Others: Round-robin chunking of width `thread_count`.

---

## ipfix\_test.py Internals

### Main Execution Path

```python
main()
  │
  ├─ fetch_all_vm_ips()       # optional: GET /vms from Prism
  ├─ build_flows()            # create conntrack flow dicts
  ├─ build_waves()            # batch by parallelism mode
  │
  ├─ for wave in waves:
  │     inject_flows_batch(flows, host, cvm_slot, paramiko)
  │       └─ packs N conntrack -I commands into one SSH round-trip
  │
  ├─ inspect_target_flows()   # verify injected flows exist
  │
  ├─ if args.keep_alive:
  │     while not _TEARDOWN_REQUESTED:
  │       for wave in waves:
  │         run_on_hypervisor(host, update_argv(flow))  ← conntrack -U
  │       sleep keepalive_interval
  │
  └─ if args.loop_count > 1:
        for loop_num in 2..loop_count:
          sleep loop_interval
          re-inject all waves
          re-inspect
```

### Conntrack Injection
```bash
conntrack -I \
  --protonum tcp \
  --src <src_ip> --dst <dst_ip> \
  --sport <sport> --dport <dport> \
  --state ESTABLISHED \
  --status SEEN_REPLY,ASSURED \
  --timeout 120
```

### CVM SSH Slot Architecture
To avoid creating a new SSH connection per command (which exhausts CVM channels):
- A pool of `CVM_SSH_SLOTS` persistent CVM sessions is pre-allocated.
- Each thread borrows a slot, executes commands through it, then returns it.
- Batching: up to `CONNTRACK_BATCH_SIZE` (default 50) commands are packed into one SSH channel write.

---

## Signal Handling & Graceful Teardown

Both scripts install signal handlers early in `main()`:

```python
signal.signal(signal.SIGUSR1, _request_teardown)
signal.signal(signal.SIGTERM, _request_teardown)
```

`_request_teardown()` sets the global `_TEARDOWN_REQUESTED` threading.Event, which all loops check on each iteration.

From `app.py`:
- **Cancel**: sends `SIGTERM` → worker marks status as "aborted".
- **Teardown/Flush**: sends `SIGUSR1` → script completes current wave and exits cleanly.

---

## History & Persistence Storage

| File | Contents |
|------|----------|
| `data/ipfix_jobs_history.jsonl` | One JSON line per completed job with full summary |
| `data/run_history.jsonl` | Lightweight index entry (used by index page recent runs) |
| `data/ipfix_vm_lists/selected_<hash>.txt` | VM IP list for this job |
| `logs/runs/run_ipfix_<run_id>.log` | Full stdout+stderr of the script |

### Job Summary Fields (stored in `ipfix_jobs_history.jsonl`)

```json
{
  "run_id": "ipfix_20261018T042441_137922",
  "status": "complete|error|aborted",
  "job_kind": "ipfix",
  "traffic_mode": "live|mock",
  "pc_ip": "10.x.x.x",
  "proto": "both",
  "pattern": "n:n",
  "parallel_mode": "nn",
  "persistence_mode": "recurring",
  "loop_count": 5,
  "loop_interval": 60,
  "vm_count": 52,
  "thread_count": 50,
  "duration": 30,
  "ips_file": "data/ipfix_vm_lists/selected_abc.txt",
  "queued_at": "...",
  "running_started_at": "...",
  "finished_at": "...",
  "duration_sec": 187.4,
  "error": ""
}
```

---

## Job UI (job.html)

The job detail page (`/job/<run_id>`) renders for all job types. The IPFIX section shows:

- **Summary cards**: traffic mode, protocol, pattern, parallel mode, persistence mode, VM count, thread count, duration
- **Recurring/Keep-alive details**: loop count, loop interval, keepalive interval
- **Skipped VMs**: parsed from log lines `"  - N VMs due to <reason>"` by `_parse_skipped_vms_from_log()`
- **Live log tail**: auto-refreshed every 3 seconds while status is running
- **Action buttons**: Cancel, Teardown (SIGUSR1), Flush Mock

### Log Parsing for Skipped VMs
```python
pattern = re.compile(r"""  - (\d+) VM(?:s)? due to (.*)""")
```
Groups by reason and displays aggregated counts.

---

## Frontend (ipfix.html)

### Key JavaScript Functions

| Function | Purpose |
|----------|---------|
| `fetchVMs()` | POST to `/api/ipfix/fetch_vms`, render VM checkbox table |
| `renderVmTable()` | Paginated rendering of VM checkboxes (50 per page) |
| `collectPayload()` | Assembles the complete JSON body for `/api/ipfix/start` |
| `showRelatedFields()` | Show/hide form sections based on persistence mode, traffic mode |
| `previewCommand()` | Build a human-readable CLI preview string |
| `updateFlowProjection()` | Compute and display projected flow count, time, pairs |
| `computePairs(n)` | Map parallel mode + pattern to number of flow pairs |
| `countPortSpec(spec)` | Parse port string ("80,443,8000-8010") → count |
| `destPortCounts()` | Return `{tcpCount, udpCount}` for current proto + port config |

### Flow Projection Banner
Appears above the "Start Injection" button and updates in real time:

```
Total flows: 14,872 | Time estimate: ~4m 58s | Pairs: 26×28=728 | Ports/pair: ~10.2 | Threads: 50
```

Formula:
- `pairs = computePairs(selectedVmCount)` — uses the effective pattern per mode
- `portsPerPair = tcpCount + udpCount`
- `totalFlows = pairs × portsPerPair`
- `estSecs = ⌈totalFlows / threads⌉ × duration`
- For recurring mode: `totalFlows *= loopCount`, `estSecs = loopCount × oneLoopSecs + (loopCount-1) × loopInterval`

---

## Configuration Reference

### app.py Constants (top of file)

```python
IPFIX_SCRIPT_PATH       = PROJECT_DIR / "ipfix_test.py"
IPFIX_LIVE_SCRIPT_PATH  = PROJECT_DIR / "ipfix_live.py"
IPFIX_JOBS_HISTORY_FILE = PROJECT_DIR / "data" / "ipfix_jobs_history.jsonl"
```

### ipfix_live.py Constants

```python
PAYLOAD_MODE        = "random"         # or "fixed"
PAYLOAD_FIXED       = 1400             # bytes
PAYLOAD_RANDOM_MIN  = 1
PAYLOAD_RANDOM_MAX  = 1400
PARALLEL_MODE       = "unique_vms"     # default mode
```

### ipfix_test.py Constants

```python
PAGE_SIZE                   = 50
LOG_FILE                    = "injected_conntrack_flows.log"
TCP_PORTS                   = [80, 443, 22, 3306]
UDP_PORTS                   = [53, 123, 161]
CONNTRACK_BIN               = "/usr/sbin/conntrack"
UPDATE_EXISTING_CONNECTION  = True
CVM_SSH_SLOTS               = 1
VM_FILTER                   = "is_control_domain!=1;is_cvm==0"
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `unrecognized arguments: --loop-count` | Wrong script for traffic mode | Ensure `ipfix_live.py` is used for live mode |
| `nats: maximum payload exceeded` | Receiver batch too large for NATS | Reduce `batchSize` in receiver config |
| Job immediately fails with exit code 1 | SSH auth failure or VM unreachable | Check `--ssh-user`/`--ssh-password`; verify VMs are powered on |
| All flows show "Such conntrack exists" | `UPDATE_EXISTING_CONNECTION=True` triggered | Normal; entries already present, updated instead |
| Fetch VMs returns empty list | VM filter too strict or PE unreachable | Check `VM_FILTER`, verify Prism connectivity |
| Keep-alive job never stops | SIGUSR1/SIGTERM not reaching process | Use `/api/ipfix/teardown/<run_id>` endpoint |
| Banner shows wrong flow count | Pattern mismatch between UI and script | Verify `computePairs()` matches the Python `build_flows()` logic |
| `connection refused` during live flow | VM not running socket server | Increase `--timeout`; verify Python available on VMs |
