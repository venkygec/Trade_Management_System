# CIS Trade Hive — Support Guide

## 1. Purpose

This runbook helps support teams keep the application available, monitor scheduled processing, and investigate incidents.

---

## 2. Application Uptime Runbook

## 2.1 Basic Health Checks

From `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive`:

```bash
source ../.venv/bin/activate
python manage.py test_hive
python manage.py runserver 0.0.0.0:8000
```

Useful endpoints:
- `/core/health/` — application health check
- `/core/pool-stats/` — Impala connection pool stats

## 2.2 Environment/Connectivity Validation

- Verify `CIS_ENV` and Impala settings in environment variables.
- Confirm Impala host/port/auth are correct for environment.
- If local: ensure Kudu/Impala Docker is running.

---

## 3. Incident Triage Flow

1. Capture failing URL, user, timestamp, and error screenshot.
2. Check if issue is global (all users) or role/data specific.
3. Validate Impala connectivity (`python manage.py test_hive`).
4. Check queue/process health for impacted function:
   - Trade events: `/trade/api/event-queue/health/`
   - Position worker: `/trade/api/worker-health/`
5. Review audit trail from **System → Audit Logs**.
6. Re-run affected job safely in dry-run where possible.

---

## 4. EOD / Control-M Operational Runbook

Primary reference:
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/CONTROL_M_EOD_JOBS.md`
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/EOD_PROCESSING_GUIDE.md`

### Standard EOD Chain
1. `python manage.py test_hive`
2. `python manage.py sync_gmp_corporate_actions`
3. `python manage.py process_corporate_actions`
4. `python manage.py process_approved_cashflows --run-type EOD`
5. `python manage.py process_settlements`
6. `python manage.py refresh_positions --run-type EOD`
7. `python manage.py create_sod_snapshot`

### Recovery Options
- Stuck CA queue entries: `process_corporate_actions --reset-stuck`
- Retry failed CA queue entries: `process_corporate_actions --retry-failed`
- Non-destructive validation: use `--dry-run` first on supported commands.

---

## 5. Common Issue Patterns

### 5.1 Blank/Failing UI page
- Check role permission and menu visibility.
- Confirm backend API endpoints for that page are reachable.
- Review server logs for template/view exceptions.

### 5.2 Approval queues not moving
- Confirm maker/checker separation is respected.
- Check pending queues in UI.
- Confirm related processing command completed successfully.

### 5.3 Position or settlement mismatch
- Verify status in trade/cashflow tables.
- Verify EOD commands for settlement/revaluation were completed in order.
- Re-run targeted command with date/portfolio filters when available.

### 5.4 Upload ingestion failures
- Validate schema from upload preview.
- Check upload status endpoint.
- Re-run ETL process only after correcting source data.

---

## 6. Key Support Commands

- `python manage.py test_hive` — DB connectivity
- `python manage.py verify_rbac` — RBAC table health check
- `python manage.py process_settlements --dry-run` — preview settlement processing
- `python manage.py refresh_positions --dry-run --run-type EOD` — preview revaluation
- `python manage.py process_corporate_actions --status` — queue status summary

---

## 7. Escalation Guidance

Escalate to development when:
- Reproducible server-side exception persists after connectivity is healthy.
- Data inconsistency appears in Kudu tables after successful command completion.
- EOD chain fails repeatedly at same stage after retry/recovery steps.

Provide:
- exact command/API/page
- timestamp and environment
- command output/log excerpts
- affected portfolio/security/trade identifiers

