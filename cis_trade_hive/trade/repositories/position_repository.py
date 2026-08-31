"""
Position Repository

Read-only data access for cis_position (Kudu) table.
Supports filtering by portfolio, security, src_system, position_basis,
position_date range, and position_type.
"""

import logging
from typing import Dict, List, Optional, Any

from core.repositories.impala_connection import impala_manager
from django.conf import settings

logger = logging.getLogger(__name__)

DATABASE = settings.IMPALA_CONFIG['DATABASE']
TABLE = 'cis_position'


class PositionRepository:

    @staticmethod
    def _escape(value: str) -> str:
        if value is None:
            return ''
        return str(value).replace("\\", "\\\\").replace("'", "\\'")

    # Columns allowed for ORDER BY (whitelist to prevent injection)
    SORTABLE_COLUMNS = {
        'account_group':     'p.account_group',
        'portfolio':         'pos.portfolio',
        'portfolio_currency': 'p.currency',
        'portfolio_investment_type': 'p.investment_type',
        'security_label':    'pos.security_label',
        'security_currency': 's.currency_code',
        'isin':              'pos.isin',
        'position_basis':    'pos.position_basis',
        'position_date':     'pos.position_date',
        'src_system':        'pos.src_system',
        'position_type':     'pos.position_type',
        'quantity':          'pos.quantity',
        'average_cost_fc':   'pos.average_cost_fc',
        'cost_fc':           'pos.cost_fc',
        'cost_lc':           'pos.cost_lc',
        'market_value_fc':   'pos.market_value_fc',
        'market_value_lc':   'pos.market_value_lc',
        'net_book_value_fc': 'pos.net_book_value_fc',
        'unrealized_pnl_fc': 'pos.unrealized_pnl_fc',
        'unrealized_pnl_lc': 'pos.unrealized_pnl_lc',
        'realized_pnl_fc':   'pos.realized_pnl_fc',
        'provision_fc':      'pos.provision_fc',
        'provision_lc':      'pos.provision_lc',
        'dividend_fc':       'pos.dividend_fc',
        'uncall_fc':         'pos.uncall_fc',
        'uncall_lc':         'pos.uncall_lc',
        'pipeline_fc':       'pos.pipeline_fc',
        'processing_date':      'pos.processing_date',
        'processing_timestamp': 'pos.processing_timestamp',
        'revaluation_status':   'revaluation_status',
    }

    def _build_position_conditions(
        self,
        portfolios=None, securities=None, src_system=None,
        position_basis=None, position_type=None,
        date_from=None, date_to=None,
        prefix='pos.'
    ):
        """Build shared WHERE conditions. position_type accepts str or list."""
        e = self._escape
        conditions = []
        # Position Viewer shows current state, not history -- without this,
        # superseded rows left behind by pre-fix duplicate position_ids (or
        # any other is_latest=false row) show up alongside the current one.
        conditions.append(f"({prefix}is_latest = true OR {prefix}is_latest IS NULL)")
        if portfolios and len(portfolios) == 1:
            conditions.append(f"UPPER({prefix}portfolio) LIKE '%{e(portfolios[0].upper())}%'")
        elif portfolios and len(portfolios) > 1:
            vals = "', '".join(e(p) for p in portfolios)
            conditions.append(f"{prefix}portfolio IN ('{vals}')")
        if securities and len(securities) == 1:
            conditions.append(f"UPPER({prefix}security_label) LIKE '%{e(securities[0].upper())}%'")
        elif securities and len(securities) > 1:
            vals = "', '".join(e(s) for s in securities)
            conditions.append(f"{prefix}security_label IN ('{vals}')")
        if src_system:
            src_list = [s for s in (src_system if isinstance(src_system, list) else [src_system]) if s]
            if len(src_list) == 1:
                conditions.append(f"{prefix}src_system = '{e(src_list[0])}'")
            elif len(src_list) > 1:
                vals = "', '".join(e(s) for s in src_list)
                conditions.append(f"{prefix}src_system IN ('{vals}')")
        if position_basis:
            conditions.append(f"{prefix}position_basis = '{e(position_basis)}'")
        if position_type:
            pt_list = [t for t in (position_type if isinstance(position_type, list) else [position_type]) if t]
            if len(pt_list) == 1:
                conditions.append(f"{prefix}position_type = '{e(pt_list[0])}'")
            elif len(pt_list) > 1:
                vals = "', '".join(e(t) for t in pt_list)
                conditions.append(f"{prefix}position_type IN ('{vals}')")
        if date_from:
            conditions.append(f"{prefix}position_date >= '{e(date_from)}'")
        if date_to:
            conditions.append(f"{prefix}position_date <= '{e(date_to)}'")
        return conditions

    def get_positions(
        self,
        portfolios: Optional[list] = None,
        securities: Optional[list] = None,
        src_system: Optional[Any] = None,
        position_basis: Optional[str] = None,
        position_type: Optional[Any] = None,   # str or list[str]
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Optional[str] = None,
        limit: int = 500,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch positions from cis_position with optional filters and sorting."""
        try:
            conditions = self._build_position_conditions(
                portfolios=portfolios, securities=securities, src_system=src_system,
                position_basis=position_basis, position_type=position_type,
                date_from=date_from, date_to=date_to, prefix='pos.'
            )
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # Build ORDER BY — whitelist column, default asc/desc
            order_col = self.SORTABLE_COLUMNS.get(sort_by, 'pos.position_date')
            order_dir = 'ASC' if str(sort_dir).upper() == 'ASC' else 'DESC'
            # Default sort: date DESC when no explicit sort chosen
            if not sort_by:
                order_dir = 'DESC'
            order_by = f"{order_col} {order_dir}, pos.portfolio, pos.security_label"

            query = f"""
                SELECT
                    pos.position_id, pos.version_id,
                    p.account_group AS account_group,
                    pos.portfolio, pos.security_label,
                    p.currency AS portfolio_currency,
                    p.investment_type AS portfolio_investment_type,
                    s.currency_code AS security_currency,
                    pos.position_basis, pos.position_date,
                    pos.src_system, pos.processing_date,
                    pos.quantity,
                    pos.average_cost_fc, pos.average_cost_lc,
                    pos.cost_fc, pos.cost_lc,
                    pos.market_value_fc, pos.market_value_lc,
                    pos.net_book_value_fc, pos.net_book_value_lc,
                    pos.unrealized_pnl_fc, pos.unrealized_pnl_lc,
                    pos.realized_pnl_fc, pos.realized_pnl_lc,
                    pos.provision_fc, pos.provision_lc,
                    pos.dividend_fc, pos.dividend_lc,
                    pos.uncall_fc, pos.uncall_lc,
                    pos.pipeline_fc, pos.pipeline_lc,
                    pos.position_type,
                    pos.isin,
                    pos.processing_timestamp,
                    COALESCE(p.revaluation_status, '') AS revaluation_status,
                    s.security_id AS security_id
                FROM {DATABASE}.{TABLE} pos
                LEFT JOIN {DATABASE}.cis_portfolio p
                    ON pos.portfolio = p.name
                    AND (p.is_active = true OR p.is_active IS NULL)
                LEFT JOIN {DATABASE}.cis_security s
                    ON pos.security_label = s.security_name
                {where}
                ORDER BY {order_by}
                LIMIT {limit}
                OFFSET {offset}
            """

            results = impala_manager.execute_query(query, database=DATABASE)
            return results if results else []

        except Exception as e:
            logger.error(f"Error fetching positions: {str(e)}")
            return []

    def get_position_count(
        self,
        portfolios: Optional[list] = None,
        securities: Optional[list] = None,
        src_system: Optional[Any] = None,
        position_basis: Optional[str] = None,
        position_type: Optional[Any] = None,   # str or list[str]
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> int:
        """Return total count matching filters (for pagination)."""
        try:
            conditions = self._build_position_conditions(
                portfolios=portfolios, securities=securities, src_system=src_system,
                position_basis=position_basis, position_type=position_type,
                date_from=date_from, date_to=date_to, prefix=''
            )
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            query = f"SELECT COUNT(*) AS cnt FROM {DATABASE}.{TABLE} {where}"
            results = impala_manager.execute_query(query, database=DATABASE)
            return int(results[0].get('cnt', 0)) if results else 0
        except Exception as e:
            logger.error(f"Error counting positions: {str(e)}")
            return 0

    def get_summary_stats(
        self,
        portfolios: Optional[list] = None,
        securities: Optional[list] = None,
        src_system: Optional[Any] = None,
        position_basis: Optional[str] = None,
        position_type: Optional[Any] = None,   # str or list[str]
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> Dict[str, Any]:
        """Aggregate stats for summary cards matching current filters."""
        try:
            conditions = self._build_position_conditions(
                portfolios=portfolios, securities=securities, src_system=src_system,
                position_basis=position_basis, position_type=position_type,
                date_from=date_from, date_to=date_to, prefix=''
            )
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            query = f"""
                SELECT
                    COUNT(*)                          AS total_positions,
                    SUM(market_value_fc)              AS total_market_value_fc,
                    SUM(market_value_lc)              AS total_market_value_lc,
                    SUM(cost_fc)                      AS total_cost_fc,
                    SUM(cost_lc)                      AS total_cost_lc,
                    SUM(unrealized_pnl_fc)            AS total_unrealized_pnl_fc,
                    SUM(unrealized_pnl_lc)            AS total_unrealized_pnl_lc,
                    SUM(realized_pnl_fc)              AS total_realized_pnl_fc,
                    SUM(COALESCE(uncall_fc, 0))       AS total_uncall_fc,
                    SUM(COALESCE(pipeline_fc, 0))     AS total_pipeline_fc
                FROM {DATABASE}.{TABLE}
                {where}
            """

            results = impala_manager.execute_query(query, database=DATABASE)
            return results[0] if results else {}

        except Exception as e:
            logger.error(f"Error computing position stats: {str(e)}")
            return {}

    def get_distinct_src_systems(self) -> List[str]:
        """Get distinct src_system values for filter dropdown."""
        try:
            query = f"""
                SELECT DISTINCT src_system
                FROM {DATABASE}.{TABLE}
                WHERE src_system IS NOT NULL
                ORDER BY src_system
            """
            results = impala_manager.execute_query(query, database=DATABASE)
            return [r.get('src_system') for r in results if r.get('src_system')]
        except Exception as e:
            logger.error(f"Error fetching src_systems: {str(e)}")
            return []

    def get_distinct_portfolios(self) -> List[str]:
        """Get distinct portfolio values for filter dropdown."""
        try:
            query = f"""
                SELECT DISTINCT portfolio
                FROM {DATABASE}.{TABLE}
                WHERE portfolio IS NOT NULL
                ORDER BY portfolio
                LIMIT 200
            """
            results = impala_manager.execute_query(query, database=DATABASE)
            return [r.get('portfolio') for r in results if r.get('portfolio')]
        except Exception as e:
            logger.error(f"Error fetching portfolios: {str(e)}")
            return []

    def get_max_position_date(self) -> Optional[str]:
        """Return the latest position_date in the table as YYYY-MM-DD, or None."""
        try:
            results = impala_manager.execute_query(
                f"SELECT MAX(position_date) AS max_date FROM {DATABASE}.{TABLE}",
                database=DATABASE,
            )
            if results:
                val = results[0].get('max_date')
                if val:
                    return str(val)[:10]
        except Exception as e:
            logger.error(f"Error fetching max position_date: {e}")
        return None


position_repository = PositionRepository()
