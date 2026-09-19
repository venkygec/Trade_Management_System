# CIS Trade Hive — Technical Guide

## 1. Purpose

This guide explains the technical implementation behind the UI, EOD processing, interfaces, and script responsibilities.

---

## 2. Application Architecture

## 2.1 Layering

Implementation follows:
- **Views**: HTTP handlers and request/response
- **Services**: business logic and workflow rules
- **Repositories**: Impala SQL access to Kudu tables

Main path: `views -> *_service.py -> *_kudu_repository.py -> Impala/Kudu`

Core connection manager:
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/core/repositories/impala_connection.py`

## 2.2 Workflow Engine

Maker-checker status transitions used across key entities:
`INITIAL -> MODIFIED -> PENDING_VALIDATION -> VALIDATED/CANCELLED -> SETTLED`

---

## 3. UI-to-Backend Functional Mapping

## 3.1 URL Entry Points

- `config/urls.py` wires module routes:
  - `/portfolio/`
  - `/trade/`
  - `/reference-data/`
  - `/market-data/`
  - `/security/`
  - `/udf/`
  - `/lookup/`
  - `/upload/`
  - `/query-builder/`
  - `/core/`

## 3.2 Menu/Feature Mapping

Sidebar menu source:
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/templates/components/sidebar.html`

Each menu item maps to module URL files:
- portfolio: `portfolio/urls.py`
- trade/cashflow/positions: `trade/urls.py`
- market data: `market_data/urls.py`
- reference data: `reference_data/urls.py`
- security: `security/urls.py`
- udf: `udf/urls.py`
- lookup: `lookup/urls.py`
- query builder: `query_builder/urls.py`
- system/rbac: `core/urls.py`

---

## 4. EOD and Batch Process Flows

Detailed references:
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/EOD_PROCESSING_GUIDE.md`
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/CONTROL_M_EOD_JOBS.md`

## 4.1 Standard EOD Sequence
1. GMP CA sync (`sync_gmp_corporate_actions`)
2. CA cash flow generation (`process_corporate_actions`)
3. Approved cash flow application (`process_approved_cashflows`)
4. Trade settlement (`process_settlements`)
5. Position revaluation (`refresh_positions`)
6. SOD snapshot (`create_sod_snapshot`)

## 4.2 Date Driving Logic

When not explicitly overridden, EOD/CORR date logic is inferred from:
- `gmp_cis_sta_dly_alldatesinfo`

---

## 5. Interface Landscape

## 5.1 Inbound Interfaces (Typical)
- GMP staging feeds (`gmp_cis_sta_dly_*`)
- Security/equity feeds for sync jobs
- Upload UI files (manual/business uploads)

## 5.2 Outbound/Derived Outputs
- Updated Kudu master/transaction tables
- Approval queues and audit records
- Position snapshots (`SOD`, `INT`, `EOD`, `CORR` contexts)

Reference:
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/GMP_DATA_SYNC_GUIDE.md`

---

## 6. Script and Command Inventory

## 6.1 Management Commands (Selected)

### Core
- `test_hive` — Impala connectivity
- `verify_rbac` — RBAC health check
- `export_ddl` — export DDL

### Reference Data
- `sync_gmp_corporate_actions` — GMP CA sync + queue
- `process_corporate_actions` — CA queue to cash flow generation

### Trade
- `process_approved_cashflows` — apply approved CF to positions
- `process_settlements` — settlement processing for pending trades
- `refresh_positions` — revaluation job
- `create_sod_snapshot` — next-day SOD copy
- `position_worker` — async queue worker controls
- maintenance/backfill commands:
  - `backfill_cancelled_trade_visibility`
  - `backfill_zero_price_positions`
  - `cleanup_corr_duplicate_positions`
  - `rename_security_labels`
  - `delete_security_labels`

### Other Modules
- `import_portfolios`, `setup_security_udf`, `setup_equity_price_udf`, `create_equity_price_table`, `load_udf_sample_data`, `recreate_udf_field_table`

## 6.2 PySpark / ETL Scripts (`sql/pyspark`)
- `eod_ams_position_etl.py` — AMS/GMP position ETL pipeline
- `eod_ca_cash_flow.py` — CA→cashflow EOD pipeline
- `merge_gmp_security.py` — GMP security merge with stable security ID registry logic
- `merge_gmp_equity_price.py` — GMP equity price merge
- `upload_equity_price_csv.py` — CIS equity price CSV load
- `merge_position_master.py` — merge multiple staging position sources
- `generic_file_ingest.py` — metadata-driven ingestion framework
- `ingest_trade_hive_to_kudu.py` / `ingest_trade_simple.py` — trade ingestion variants

## 6.3 Python 3.6 Edge Runtime Fork
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/edge_jobs_py36/`
- Contains Django-free forks for edge/Control-M environments where Python 3.6 is required.

---

## 7. Mapping Logic Highlights

## 7.1 Corporate Action Mapping
- GMP free-text CA types are normalized to CIS CA types using command-side mapping in `sync_gmp_corporate_actions`.
- Unknown types are skipped until mapping table is extended.

## 7.2 Security Identity Mapping
- `merge_gmp_security.py` uses natural-key registry mapping for stable `security_id` assignment.
- Priority typically favors ISIN, then fallback keys.

## 7.3 Position ETL Mapping
- `eod_ams_position_etl.py` runs staged mapping/validation:
  - standardization
  - portfolio validation
  - security matching
  - fallback matching flows

---

## 8. Where to Go Deeper

- Architecture: `docs/docs/technical/architecture.md`
- DB model: `docs/docs/technical/database-schema.md`
- EOD details: `docs/EOD_PROCESSING_GUIDE.md`
- Control-M operations: `docs/CONTROL_M_EOD_JOBS.md`
- GMP interfaces: `docs/GMP_DATA_SYNC_GUIDE.md`

