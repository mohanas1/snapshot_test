# CG Alert Thresholds and Severity

Tune these values with production baselines after 1-2 weeks of observation.

## Availability and API SLO

| Metric | Warning | Critical | Window |
|---|---|---|---|
| CG health endpoint failure rate | > 1% | > 5% | 10 min |
| UI/API availability | < 99.9% | < 99.5% | daily |
| API p95 latency | > 2s | > 4s | 10 min |
| API 5xx error rate | > 1% | > 3% | 10 min |

## Kubernetes Health

| Metric | Warning | Critical | Window |
|---|---|---|---|
| Pod NotReady | > 1 pod for 5 min | > 1 pod for 10 min | rolling |
| CrashLoopBackOff | 1 pod | >= 2 pods | immediate |
| Restart count delta | > 3/hour per pod | > 10/hour per pod | hourly |
| OOMKilled events | 1 event | >= 2 events | 24h |

## Resource Saturation

| Metric | Warning | Critical | Window |
|---|---|---|---|
| CPU utilization | > 80% | > 90% | 15 min |
| Memory utilization | > 85% | > 92% | 15 min |
| CPU throttling ratio | > 10% | > 20% | 15 min |
| Ephemeral storage | > 80% | > 90% | 15 min |

## Functional and Data Quality

| Metric | Warning | Critical | Window |
|---|---|---|---|
| Request volume deviation vs 7-day baseline | > 30% | > 50% | 1 hour |
| Report failure rate | > 5% | > 10% | 1 hour |
| Ingestion freshness lag | > 60 min | > 120 min | rolling |
| Queue lag growth | sustained 15 min | sustained 30 min | rolling |

## Recommended Alert Routing

- `Critical`: page on-call immediately and open incident.
- `Warning`: send channel alert, assign owner, investigate in same shift.
- `Info`: include in end-of-day report and trend tracking.
