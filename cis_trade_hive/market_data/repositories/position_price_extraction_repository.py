"""
Position Price Extraction Repository

Extracts candidate equity prices from cis_position for securities that do not
yet have a CIS-sourced equity price record.

Scope (all conditions must be satisfied):
  - position_date = CURRENT_DATE()
  - is_latest = true
  - DISTINCT by (security_label, currency_code)
  - Exclude portfolios where portfolio_group  = 'UOI'
  - Exclude portfolios where investment_type IN ('SUBSIDIARY CO', 'ASSOCIATED CO')
  - Exclude portfolios where entity_group    = 'UOBS'
  - Exclude QUOTED securities (cis_security.quoted_unquoted = 'QUOTED')
  - Exclude securities that already have ANY equity price row with src_system = 'CIS'

Output columns match the equity-price position-upload CSV format:
  security_label, currency_code, price_date, market_value, quantity, isin
"""

import logging
import re
from typing import List, Dict, Any

from core.repositories.impala_connection import impala_manager
from django.conf import settings

logger = logging.getLogger(__name__)

IMPALA_CONFIG = settings.IMPALA_CONFIG
DATABASE = IMPALA_CONFIG['DATABASE']


class PositionPriceExtractionRepository:

    @staticmethod
    def get_extraction_candidates(position_date: str = None) -> List[Dict[str, Any]]:
        """
        Return candidate rows for equity price upload from today's positions.

        Args:
            position_date: Override date in YYYY-MM-DD format.
                           Defaults to CURRENT_DATE() (Impala function).

        Returns:
            List of dicts with keys:
              security_label, currency_code, price_date,
              market_value, quantity, isin
        """
        if position_date:
            # Strictly validate to YYYY-MM-DD to prevent SQL injection
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', position_date):
                raise ValueError(f"Invalid position_date format: '{position_date}'. Expected YYYY-MM-DD.")
            pos_date_expr = f"'{position_date}'"
        else:
            pos_date_expr = 'CURRENT_DATE()'

        query = f"""
            SELECT DISTINCT
                pos.security_label,
                COALESCE(s.currency_code, '') AS currency_code,
                pos.position_date             AS price_date,
                pos.market_value_fc           AS market_value,
                pos.quantity,
                COALESCE(pos.isin, s.isin, '') AS isin
            FROM {DATABASE}.cis_position pos

            -- Portfolio attributes for exclusion rules
            LEFT JOIN {DATABASE}.cis_portfolio pf
                ON pos.portfolio = pf.name
               AND (pf.is_active = true OR pf.is_active IS NULL)

            -- Security attributes: currency + quoted/unquoted status
            LEFT JOIN {DATABASE}.cis_security s
                ON pos.security_label = s.security_name

            WHERE pos.position_date = {pos_date_expr}
              AND pos.is_latest      = true
              AND pos.quantity       IS NOT NULL
              AND pos.quantity       <> 0
              AND pos.market_value_fc IS NOT NULL

            -- Exclude portfolio group UOI
              AND COALESCE(UPPER(TRIM(pf.portfolio_group)), '') <> 'UOI'

            -- Exclude Subsidiary / Associated investment types
              AND UPPER(TRIM(COALESCE(pf.investment_type, '')))
                  NOT IN ('SUBSIDIARY CO', 'ASSOCIATED CO')

            -- Exclude entity group UOBS
              AND COALESCE(UPPER(TRIM(pf.entity_group)), '') <> 'UOBS'

            -- Exclude QUOTED securities (already have exchange prices)
              AND COALESCE(UPPER(TRIM(s.quoted_unquoted)), '') <> 'QUOTED'

            -- Exclude securities that already have ANY equity price with src_system = 'CIS'
              AND pos.security_label NOT IN (
                  SELECT DISTINCT ep.security_label
                  FROM {DATABASE}.cis_equity_price ep
                  WHERE UPPER(ep.src_system) = 'CIS'
              )

            ORDER BY pos.security_label, currency_code
        """

        try:
            results = impala_manager.execute_query(query, database=DATABASE)
            if not results:
                return []
            # Normalise numeric types to plain Python for JSON serialisation
            cleaned = []
            for row in results:
                cleaned.append({
                    'security_label': str(row.get('security_label') or ''),
                    'currency_code':  str(row.get('currency_code') or ''),
                    'price_date':     str(row.get('price_date') or ''),
                    'market_value':   float(row.get('market_value') or 0),
                    'quantity':       float(row.get('quantity') or 0),
                    'isin':           str(row.get('isin') or ''),
                })
            return cleaned
        except Exception as e:
            logger.error(f"Error fetching position price extraction candidates: {e}")
            return []


position_price_extraction_repository = PositionPriceExtractionRepository()
