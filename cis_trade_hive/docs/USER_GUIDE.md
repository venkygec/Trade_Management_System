# CIS Trade Hive — User Guide

## 1. Purpose

This guide explains the main application menus and what end users can do in each area of the CIS Trade Hive UI.

---

## 2. Main Navigation

The left sidebar is role-based. Menus appear based on your permissions.

### Dashboard
- Entry page after login.
- Shows high-level operational and business summary widgets.

### Portfolio & Trade

#### Portfolios
- **All Portfolios**: view and search portfolios.
- **New Portfolio**: create a portfolio.
- **Pending Approval**: checker queue for pending portfolio validations.
- Portfolio workflow uses maker-checker states: `INITIAL → MODIFIED → PENDING_VALIDATION → VALIDATED/CANCELLED → SETTLED`.

#### Trades
- **All Trades**: list and filter trades.
- **New Trade**: create BUY/SELL trade.
- **Pending Validation**: checker validation queue.
- **Pending Settlement**: checker settlement queue.

#### Positions
- Read-only position list (`cis_position`) for monitoring holdings.

#### Cash Flows
- **All Cash Flows**: list cash flows.
- **New Cash Flow**: create cash flow entry.
- **Pending Approval**: approve/reject pending cash flows.

### Market Data
- **Market Data Dashboard**: overview for market data.
- **FX Rates**: view and maintain FX rates.
- **Equity Prices**: view/create/upload equity prices.

### Reference Data
- **Currencies**
- **Countries**
- **Calendars**
- **MAS Codes**
- **Parties**
- **Securities**
- **Corporate Actions**

### Configuration
- **Lookup Tables**: maintain configurable lookup values.
- **User Defined Fields (UDF)**: manage custom fields.
- **File Upload**: upload files and run ingestion/ETL processing.
- **Query Builder**: run predefined or ad-hoc query flows.
- **RBAC Admin**: manage users, groups, permissions, RBAC audit (admin roles only).

### System
- **Audit Logs**: activity and change audit trail.
- **Documentation**: opens docs portal.

### Logout
- Ends your session.

---

## 3. Typical User Journeys

### Portfolio Maker
1. Create portfolio.
2. Edit while in `INITIAL/MODIFIED`.
3. Submit for validation.

### Portfolio Checker
1. Open pending queue.
2. Validate portfolio.
3. Settle portfolio to activate it.

### Trade Maker/Checker
1. Maker creates trade.
2. Checker validates in pending validation queue.
3. Checker settles in pending settlement queue.

### Operations User (Business)
1. Upload source file in **File Upload**.
2. Review validation/preview.
3. Trigger ingestion and review result status/report.

---

## 4. Search and Filters

Most list screens support:
- Text search
- Status filters
- Date/range filtering (module dependent)
- Export actions (where enabled)

---

## 5. Role & Access Notes

- Menus are permission-controlled.
- Maker and checker responsibilities must be performed by different users for approval flows.
- If a menu is not visible, contact support/RBAC admin for access.

---

## 6. Related Detailed Guides

- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/docs/business/portfolio-management.md`
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/docs/business/four-eyes-workflow.md`
- `/home/runner/work/Trade_Management_System/Trade_Management_System/cis_trade_hive/docs/docs/business/file-upload-guide.md`

