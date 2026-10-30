# Keep-Alive Persistence Architecture

## ⚠️ **Current Limitation: Connection Recreation Model**

### 🔴 The Reality

Despite the "persistent" naming, the current architecture **still recreates connections each cycle** due to fundamental design constraints:

**Each Cycle:**
1. ✅ Worker thread stays alive (permanent loop)
2. ✅ Worker owns specific flow assignment for job lifetime
3. ❌ **Sockets are created NEW each cycle**
4. ❌ **Sockets are closed after 30s traffic**
5. ⏸️ Sleep 60s (no connection exists)
6. ❌ **NEW sockets created next cycle (not reused)**

**Result:** Connections still "go dark" during the 60s sleep period.

---

## 🏗️ Architecture Analysis

### What We Have Now (Persistent Workers, Ephemeral Connections)

```python
def autonomous_worker(job_id, job):
    """Worker thread persists, connections do not."""
    while not _TEARDOWN_REQUESTED:  # ← Worker loop persists
        # Acquire semaphore
        send_traffic(30s)  # ← Creates NEW socket, sends data, closes socket
        # Release semaphore
        sleep(60s)         # ← No connection exists during this time
        # Loop back ← Creates NEW socket next iteration
```

**Execution Timeline:**
```
Worker Thread (permanent):
├─ 00:00 - Create socket A, send 30s, close socket A
├─ 00:30 - Sleep 60s (NO CONNECTION)
├─ 01:30 - Create socket B, send 30s, close socket B  
├─ 02:00 - Sleep 60s (NO CONNECTION)
└─ 03:00 - Create socket C, send 30s, close socket C...
```

### What TRUE Persistence Would Require (Not Implemented)

```python
def truly_persistent_worker(job_id, job):
    """Both worker AND connection persist."""
    # Create socket ONCE at start
    socket = create_persistent_socket()
    enable_tcp_keepalive(socket)
    
    while not _TEARDOWN_REQUESTED:
        # Use SAME socket every cycle
        send_traffic_on_existing_socket(socket, 30s)
        
        # Keep socket ALIVE during sleep
        # Send TCP keep-alive packets to prevent timeout
        sleep_with_keepalive(socket, 60s)
    
    # Close socket only when worker exits
    socket.close()
```

**Execution Timeline (Not Implemented):**
```
Worker Thread (permanent):
├─ 00:00 - Create socket A
├─ 00:00 - Send traffic 30s using socket A
├─ 00:30 - Sleep 60s (socket A stays OPEN with keep-alive packets)
├─ 01:30 - Send traffic 30s using socket A (SAME socket)
├─ 02:00 - Sleep 60s (socket A stays OPEN)
└─ 03:00 - Send traffic 30s using socket A (SAME socket)...
```

---

## 🚧 Why True Persistence Isn't Implemented

### Architectural Constraints

The traffic generation uses **remote Python scripts** executed via SSH:

**Current Architecture:**
```python
# Start remote server process
server_pid = start_background_server(dst_vm, port)

# Start remote client process  
client_pid = start_background_client(src_vm, dst_ip, port, duration=30)

# Wait for client to finish (30s)
wait_pid(client_pid)

# Kill both processes
stop_pid(server_pid)
stop_pid(client_pid)  # ← Sockets closed here
```

**The Problem:**
- Remote Python scripts are **ephemeral** (run once, exit, sockets close)
- Scripts don't have mechanism to **pause and resume**
- No way to tell running script to "send traffic for 30s, pause 60s, repeat"

### What Would Be Needed for True Persistence

**1. Long-Running Remote Servers**
```python
# Start server that stays alive for entire job
server_pid = start_persistent_server(dst_vm, port)  
# Server keeps socket open, accepts multiple connections

# At job end (not after each cycle)
stop_pid(server_pid)
```

**2. Long-Running Remote Clients with Control Protocol**
```python
# Start client that stays alive and accepts commands
client_pid = start_persistent_client(src_vm)

# Tell client to start traffic
send_command(client_pid, "START", dst_ip, port, 30s)

# Tell client to pause (keep socket alive)
send_command(client_pid, "PAUSE", 60s)

# Tell client to resume  
send_command(client_pid, "RESUME", 30s)

# At job end
send_command(client_pid, "EXIT")
```

**3. Socket Keep-Alive at OS Level**
```python
# In remote Python script
sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
```

**This requires:**
- Rewriting all embedded Python scripts (`PYTHON_SERVER`, `PYTHON_CLIENT`)
- Adding inter-process communication (signals, files, or sockets)
- Managing long-running background processes across SSH
- Complex error handling for stale connections

---

## ✅ What the Current "Persistent" Implementation Does

### 1. Persistent Worker Threads
```python
# Each worker has permanent while loop
# Worker owns its flow assignment for job lifetime
# Worker never pulls from shared queue and exits
```

### 2. Cycle Tracking
```python
# Logs include cycle number
log_line("TCP 10.1.1.1:40001 -> 10.1.1.2:80 cycle=5")

# Telemetry tracks total cycles
telemetry["completed_cycles"] = 1523
```

### 3. Connection State Awareness
```python
connection_established = False

try:
    result = _flow_once_persistent(...)
    connection_established = True
except:
    connection_established = False  # Reset on error
```

### 4. Improved Logging
```python
# Keep-alive mode logs show:
# - Cycle numbers
# - Bytes sent/received
# - Success/failure per cycle
```

---

## 📊 Connection Behavior: Current vs Ideal

### Current Behavior (What Happens Now)

```
Time    Connection Status          Traffic
──────────────────────────────────────────────
0:00    Create TCP connection      ████████
0:30    Close connection          
0:30    [NO CONNECTION]           
1:30    Create NEW TCP connection  ████████
2:00    Close connection
2:00    [NO CONNECTION]
3:00    Create NEW TCP connection  ████████
```

**Characteristics:**
- ❌ Connection drops every cycle
- ❌ 60s gaps with no connection
- ❌ New socket each time (different ports, seq numbers)
- ❌ TCP handshake overhead every cycle
- ❌ Firewall sees new connections, not persistent

### Ideal Persistent Behavior (Not Implemented)

```
Time    Connection Status          Traffic
──────────────────────────────────────────────
0:00    Create TCP connection      ████████
0:30    [SAME connection ALIVE]    ░░░░░░░░ (keep-alive packets)
1:30    [SAME connection]          ████████
2:00    [SAME connection ALIVE]    ░░░░░░░░ (keep-alive packets)
3:00    [SAME connection]          ████████
```

**Characteristics:**
- ✅ ONE connection for entire job
- ✅ No gaps, always connected
- ✅ Same socket (ports, seq numbers)
- ✅ No repeated handshakes
- ✅ Firewall sees single long-lived connection

---

## 🔧 Workarounds for Connection Dropout

### Option 1: Reduce Sleep Interval (Minimize Gaps)

**Set shorter keepalive interval:**
```bash
python3 ipfix_live.py \
  --keep-alive \
  --keepalive-interval 5 \   # ← Only 5s gap instead of 60s
  --duration 30 \
  [args...]
```

**Result:** Connections recreated every 35s instead of 90s (30s traffic + 5s gap)

### Option 2: Increase Traffic Duration (Minimize Idle %)

```bash
python3 ipfix_live.py \
  --keep-alive \
  --keepalive-interval 10 \   # Short gap
  --duration 300 \             # ← 5 minutes of traffic
  [args...]
```

**Result:** Traffic 96.8% of the time (300s active / 310s total)

### Option 3: Continuous Mode (Zero Gap)

```bash
python3 ipfix_live.py \
  --keep-alive \
  --keepalive-interval 0 \    # ← NO sleep between cycles
  --duration 30 \
  [args...]
```

**Result:** Connections recreated immediately with minimal gap (< 1s)

⚠️ **Note:** Even with zero interval, connections are still **recreated** (new sockets), not **reused**.

---

## 📝 Summary

### What "Persistent" Means in This Implementation

| Aspect | Status |
|--------|--------|
| **Worker threads persist** | ✅ Yes - permanent `while` loop |
| **Flow assignments persist** | ✅ Yes - each worker owns one flow |
| **Worker loop is autonomous** | ✅ Yes - independent refresh cycles |
| **TCP/UDP sockets persist** | ❌ No - recreated each cycle |
| **Connections stay alive** | ❌ No - closed during sleep |
| **Zero connection dropout** | ❌ No - 60s gaps with no connection |

### Architecture Type

**Current:** Persistent Workers + Ephemeral Connections  
**Needed for true persistence:** Persistent Workers + Persistent Connections

### Why This Still Helps

Even though sockets are recreated, the persistent worker architecture provides:

1. ✅ **Dedicated flow ownership** - no job queue contention
2. ✅ **Continuous cycling** - automatic refresh without orchestration
3. ✅ **Independent timing** - no batch synchronization
4. ✅ **Lifecycle management** - workers born and die with job
5. ✅ **Live telemetry** - cycle counting and success tracking

### When You Need True Persistent Sockets

If you absolutely need sockets that stay open for hours/days:

**Option A:** Use `--keepalive-interval 0` for continuous recreation (< 1s gap)

**Option B:** Implement true persistent architecture (major refactoring needed):
- Rewrite embedded Python scripts as long-running daemons
- Add control protocol for start/pause/resume/stop commands
- Implement TCP keep-alive at OS level
- Handle connection failures and recovery
- Manage SSH session stability across hours/days

---

## 🎯 Recommendation

**For most use cases:**
- Current implementation is sufficient
- Use `--keepalive-interval 5-10` to minimize gaps
- Connections recreate fast enough to appear continuous

**For true zero-dropout:**
- Requires significant architectural changes
- Consider if the use case truly needs it
- May be overkill for testing/monitoring purposes

---

**Current Status:** ✅ Persistent workers implemented, but connections still ephemeral by design due to remote script architecture.
