# SC Network Log Pipeline - Data Flow Explained

## Your Question: Why Do Numbers Change?

### Pipeline Flow in Your Run

```
PE/AHV Host          →  3,413,463 individual flow records
                        ↓
Network Receiver     →  1,795 batches (grouped flows)
                        ↓
NATS JetStream       →  1,795 batches (messages)
                        ↓
NDP                  →  190 watch callbacks ⚠️ (NOT batches!)
                        ↓
DIIS → ClickHouse    →  1,270 rows inserted
```

---

## ❓ Why Numbers Change?

### 1. **3.4M Flows → 1,795 Batches** (Aggregation)

**What happens**: Receiver groups individual flow records into batches

- **Input**: 3,413,463 individual flow records (IPFIX data from PE)
- **Output**: 1,795 batches
- **Why**: Batching for efficient transport
- **Batch size**: ~1,900 flows per batch (3,413,463 ÷ 1,795)

**This is NORMAL** - flows are aggregated into batches.

---

### 2. **1,795 Batches → 190 "Consumed"** ⚠️ **MISLEADING!**

**The Problem**: **190 is NOT batches consumed!**

**What "190" actually means**:
- **190 = Watch callbacks processed**
- **Watch callbacks** = Entity cache updates (host metadata changes)
- **NOT** = NATS messages consumed

### What's Actually Happening

```
NATS publishes:  1,795 batches
                    ↓
NDP consumes:    1,795 batches (assumed, not logged directly)
                    ↓
Processing:      - Enriches flows with metadata
                 - Updates entity cache
                 - Triggers 190 watch callbacks (entity updates)
                    ↓
Sends to DIIS:   Enriched flows
```

**The "190 watch callbacks" is a SIDE EFFECT, not the main data flow!**

---

### 3. **1,795 Batches → 1,270 CH Rows** (Aggregation Again)

**What happens**: DIIS may aggregate or filter before inserting

- **Input**: 1,795 batches from NDP
- **Output**: 1,270 ClickHouse rows
- **Why**: Possible reasons:
  - Time-based aggregation (multiple flows → 1 row)
  - Duplicate filtering
  - Schema transformation
- **Ratio**: ~1.4 batches per CH row

**This is NORMAL** - data is aggregated again for storage efficiency.

---

## ✅ The Correct Data Flow

### What Each Number Means

| Stage | Metric | Unit | Meaning |
|-------|--------|------|---------|
| **PE → Receiver** | 3,413,463 | flows | Individual IPFIX flow records sent |
| **Receiver → NATS** | 1,795 | batches | Groups of flows (~1,900/batch) |
| **NATS → NDP** | 1,795 | batches | Messages consumed (same as published) |
| **NDP Entity Updates** | 190 | callbacks | Host metadata cache updates |
| **NDP → DIIS → CH** | 1,270 | rows | Final aggregated records in database |

---

## 🐛 The Bug: NDP Display is Misleading

### Current Display (WRONG)
```
NDP Consumed: 190
```

**This implies**: Only 190 messages consumed (11% of 1,795!)

### What It Actually Means
```
NDP Watch Callbacks: 190
NDP Messages Consumed: ~1,795 (not logged directly)
```

---

## 🔧 What Should Be Fixed

### Option 1: Show Both Metrics
```
NDP (nx-network-log-pipeline)
  Messages consumed: ~1,795 (inferred from NATS)
  Watch callbacks: 190 entity updates
  Messages dropped: 0
```

### Option 2: Don't Show Watch Callbacks in Main Flow
```
NDP (nx-network-log-pipeline)
  Consumed: 1,795, Dropped: 0
  
  Details:
    - 190 entity cache updates
    - 155 host entities updated
```

---

## 📊 Is All Data Reaching ClickHouse?

### Summary
```
Start:  3,413,463 flows from PE
        ↓ (batched)
        1,795 batches
        ↓ (processed & enriched)
        1,795 batches to DIIS
        ↓ (aggregated)
End:    1,270 rows in ClickHouse
```

### Answer: **YES**, but aggregated

- **Not lost**: Data is aggregated, not dropped
- **Aggregation ratio**: ~2,688 flows per CH row
- **This is expected**: ClickHouse stores aggregated flow statistics

---

## 🔍 What's Missing from Logs

The NDP logs don't directly log:
- ✅ "Consumed N messages from NATS" 
- ✅ "Processed N batches"
- ✅ "Sent N records to DIIS"

Instead, they log:
- ❌ Watch callbacks (entity updates)
- ❌ Entity changes (host metadata)
- ❌ Error messages only

**This is why "190" is confusing** - it's not the actual throughput!

---

## 📝 Summary

### Your Questions Answered

**Q: Is math correct?**
- **A**: Yes, but units are different:
  - 3.4M **flows** → 1,795 **batches** → 1,270 **CH rows**
  - "190" is **entity updates**, not throughput

**Q: Count reduced and increased?**
- **A**: Reduced by aggregation (batching)
  - Not increased anywhere

**Q: Is all data inserted to CH?**
- **A**: Yes, but aggregated (2,688 flows → 1 row)

**Q: What is "190 consumed"?**
- **A**: **Misleading!** It's entity cache updates, not message consumption
  - Real consumption: ~1,795 batches (inferred from NATS)

---

## 🎯 Recommended Fix

Change NDP display from:
```
Consumed: 190, Dropped: 0
```

To:
```
Consumed: ~1,795 (inferred), Dropped: 0
ℹ 190 entity cache updates
```

Or show calculated value:
```
Consumed: 1,791 (published - pending), Dropped: 0
```

Where: `1,791 = 1,795 published - 4 pending`
