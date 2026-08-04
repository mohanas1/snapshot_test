# Cost Governance Monitoring Kit

This folder contains a complete daily monitoring kit for NCM Cost Governance (CG).

## What this includes

- `checklists/daily-cg-monitoring-checklist.md`: shift checklist for daily CG monitoring
- `alerts/cg-alert-thresholds.md`: recommended thresholds and severity mapping
- `templates/cg-daily-handoff-template.md`: end-of-day handoff format
- `scripts/cg_daily_health_checks.sh`: operational script to run health and platform checks

## Scope Covered

- CG service availability (UI/API/reports/health endpoint)
- CG Kubernetes workload health
- CG resource and performance monitoring
- CG activity and ingestion sanity checks
- Alerting policy and incident escalation

## Quick Start

1. Ensure `kubectl` and `curl` are installed.
2. Ensure kubeconfig exists at `~/.kube/nc_kubeconfig` (or set your own path).
3. Export NCM base URL and authentication token:
   - `export NCM_BASE_URL="https://<ncm-fqdn-or-ip>"`
   - `export NCM_TOKEN="<bearer-token>"`
4. Run:
   - `bash scripts/cg_daily_health_checks.sh`
5. Fill `templates/cg-daily-handoff-template.md` and share with your team.

## Notes

- The script uses safe read/list endpoint probes only.
- Update endpoint paths in the script if your deployment uses different CG routes.
- Add these checks to cron or CI for scheduled execution.
