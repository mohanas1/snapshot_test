# Daily CG Monitoring Checklist

Use this checklist at least twice daily (start of shift and end of shift).

## 1) Service Availability

- [ ] CG health endpoint returns OK (`/internal/status`)
- [ ] CG UI route `/cg` is reachable and returns 200
- [ ] CG v1 APIs (`/v1/cg`) are reachable
- [ ] CG v2 APIs (`/v2/cg`) are reachable
- [ ] CG reports APIs (`/v1/reports`) are reachable
- [ ] p95 latency within target
- [ ] 5xx error rate within target

## 2) Kubernetes Workload Health

- [ ] All CG pods are `Running` and `Ready`
- [ ] No CrashLoopBackOff pods in namespace `ncm-cg`
- [ ] Restart count increase is within allowed threshold
- [ ] No OOMKilled events in last 24h
- [ ] Deployment desired replicas equal available replicas

## 3) Resource and Capacity

- [ ] CPU usage below warning threshold
- [ ] Memory usage below warning threshold
- [ ] CPU throttling not sustained
- [ ] No node pressure impacts on CG pods

## 4) Functional Activity Monitoring

- [ ] Request volume by endpoint is normal vs baseline
- [ ] CG success rate is normal
- [ ] Report generation jobs are completing
- [ ] Report execution p95 latency is normal

## 5) Data Freshness and Pipeline Integrity

- [ ] Cost/metering ingestion jobs completed successfully
- [ ] Data freshness lag is within target
- [ ] No backlog growth in processing queues
- [ ] No missing tenant/project/account artifacts

## 6) Logs and Error Patterns

- [ ] No new recurring exception signature
- [ ] Timeout rate is within baseline
- [ ] No auth failure spikes
- [ ] No dependency error spikes (DB, IAM, upstream services)

## 7) Security and Access

- [ ] No suspicious API burst patterns
- [ ] Service account token/certificate expiry not near threshold
- [ ] No RBAC or namespace policy drift detected

## 8) End-of-Day Reporting

- [ ] Fill and publish handoff template
- [ ] Link incidents, alerts, and remediation actions
- [ ] Add planned follow-ups for next shift/day
