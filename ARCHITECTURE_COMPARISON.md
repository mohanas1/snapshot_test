# Keep-Alive Architecture: Before vs After

## BEFORE: Sequential Global Loop (Broken Keep-Alive)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Global Pass Loop                             │
│  (Repeats every keepalive-interval after all batches complete)  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │         Pass 1                      │
        │  (All 11 batches run sequentially)  │
        └─────────────────────────────────────┘
                              │
                              ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ Batch 1  │  │ Batch 2  │  │ Batch 11 │
    │ (2 min)  │→ │ (2 min)  │→...│ (1 min) │
    └──────────┘  └──────────┘  └──────────┘
         │             │              │
         ▼             ▼              ▼
    [50 flows]    [50 flows]    [9 flows]
    run 30s       run 30s        run 30s
    then DIE      then DIE       then DIE

                              │
            Wait 60 seconds   ▼
                              
        ┌─────────────────────────────────────┐
        │         Pass 2                      │
        │  (Connections from Pass 1 are DEAD) │
        └─────────────────────────────────────┘
                              │
                              ▼
                         (repeat...)
```

### Problems:
❌ Flows from Batch 1 sit idle for ~20 minutes during Batches 2-11  
❌ Connections timeout and die before Pass 2 starts  
❌ Keep-alive only refreshes after ALL batches complete  
❌ Not truly persistent - just periodic recreation  
❌ Sequential execution means low concurrency

---

## AFTER: Independent Persistent Workers (True Keep-Alive)

```
┌────────────────────────────────────────────────────────────────┐
│              ThreadPoolExecutor (50 workers max)                │
│     Each worker independently maintains its own flow            │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
    ┌─────────────────────────────────────────────────┐
    │          All 510 flows launched at once          │
    │      (up to 50 concurrent at any moment)         │
    └─────────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ Worker 0 │  │ Worker 1 │  │ Worker 2 │  │ Worker N │
    │ (thread) │  │ (thread) │  │ (thread) │  │ (thread) │
    └──────────┘  └──────────┘  └──────────┘  └──────────┘
         │             │              │              │
         ▼             ▼              ▼              ▼
    ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
    │ While   │   │ While   │   │ While   │   │ While   │
    │ True:   │   │ True:   │   │ True:   │   │ True:   │
    └─────────┘   └─────────┘   └─────────┘   └─────────┘
         │             │              │              │
         ▼             ▼              ▼              ▼
    Send 30s      Send 30s      Send 30s      Send 30s
         │             │              │              │
         ▼             ▼              ▼              ▼
    Wait 60s      Wait 60s      Wait 60s      Wait 60s
         │             │              │              │
         ▼             ▼              ▼              ▼
    Send 30s      Send 30s      Send 30s      Send 30s
         │             │              │              │
         ▼             ▼              ▼              ▼
    Wait 60s      Wait 60s      Wait 60s      Wait 60s
         │             │              │              │
         └─────────────┴──────────────┴──────────────┘
                          (continues until
                      duration_limit or signal)
```

### Benefits:
✅ Each flow continuously maintained by dedicated worker  
✅ Connections stay alive throughout entire job  
✅ True concurrent execution (up to thread_count)  
✅ Predictable refresh timing (every keepalive-interval)  
✅ No idle periods - constant activity  
✅ Graceful shutdown for all workers simultaneously

---

## Timeline Comparison

### BEFORE (Old Keep-Alive):
```
Time    Event
────────────────────────────────────────────────────
0:00    Start Pass 1
0:00    Batch 1 starts (50 flows, each 30s)
2:00    Batch 1 done
2:00    Batch 2 starts (50 flows, each 30s)
4:00    Batch 2 done
...
14:00   Batch 11 done
15:00   Wait keepalive-interval (60s)
16:00   Start Pass 2 
        ❌ Batch 1 connections from 0:00 are DEAD (14 min idle)
```

### AFTER (New Keep-Alive):
```
Time    Event
────────────────────────────────────────────────────
0:00    Launch all 510 workers (50 concurrent)
0:00    Worker 0: Flow 1 starts, runs 30s
0:30    Worker 0: Wait 60s
1:30    Worker 0: Flow 1 refresh 2, runs 30s  ✅ STILL ALIVE
2:00    Worker 0: Wait 60s
3:00    Worker 0: Flow 1 refresh 3, runs 30s  ✅ STILL ALIVE
...     (all workers independently maintain their flows)
10:00   Duration limit reached, all workers stop gracefully
```

---

## Code Flow Comparison

### BEFORE:
```python
# Simplified structure of old keep-alive
while True:  # Global pass loop
    pass_number += 1
    for batch in waves:  # Sequential batches
        run_batch(batch)  # Each flow runs once
    
    if not keep_alive:
        break
    
    sleep(keepalive_interval)  # ← Connections die during this wait
```

### AFTER:
```python
# Simplified structure of new keep-alive
if args.keep_alive:
    def persistent_worker(job):
        while True:  # Per-worker loop
            run_flow(job)  # Send traffic for duration
            if should_stop():
                break
            sleep(keepalive_interval)  # ← Connection stays alive
    
    # Launch all workers concurrently
    with ThreadPoolExecutor(max_workers=thread_count) as pool:
        for job in all_jobs:
            pool.submit(persistent_worker, job)

else:
    # One-shot mode: original sequential logic (unchanged)
    for batch in waves:
        run_batch(batch)
```

---

## Key Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Concurrent flows** | 50 per batch | 50 total | Same concurrency |
| **Connection lifetime** | 30s per pass | Continuous | ∞ better |
| **Idle time per flow** | Up to 20 min | 0 seconds | 100% active |
| **Refresh predictability** | After all batches | Every interval | Perfectly timed |
| **Total job time** | 14+ min/pass | Continuous | Faster results |
| **Memory overhead** | Low | Slightly higher | Negligible |
| **Code complexity** | High (nested loops) | Lower (flat workers) | Cleaner |

---

## Signal Handling

### BEFORE:
```
SIGUSR1 → Sets _TEARDOWN_REQUESTED
         → Breaks out of current batch
         → Skips remaining batches
         → Waits until end of sleep to check flag
```

### AFTER:
```
SIGUSR1 → Sets _TEARDOWN_REQUESTED
         → All workers check flag every second
         → Workers exit their while loop immediately
         → ThreadPoolExecutor waits for clean shutdown
         → All SSH connections close gracefully
```

---

## Compatibility Matrix

| Feature | One-Shot Mode | Old Keep-Alive | New Keep-Alive |
|---------|--------------|----------------|----------------|
| Sequential batch execution | ✅ Yes | ✅ Yes | ❌ No |
| Concurrent workers | ✅ Yes | ✅ Yes | ✅ Yes |
| Continuous refresh | ❌ No | ⚠️ Fake | ✅ True |
| Connection persistence | ❌ No | ⚠️ Broken | ✅ Yes |
| Duration limit | N/A | ✅ Yes | ✅ Yes |
| Signal handling | ✅ Yes | ✅ Yes | ✅ Yes |
| Per-batch SSH | ✅ Yes | ❌ No | ❌ No |
| Upfront SSH | ❌ No | ✅ Yes | ✅ Yes |

Legend:
- ✅ Works correctly
- ⚠️ Partially works or broken
- ❌ Not applicable / Not used
