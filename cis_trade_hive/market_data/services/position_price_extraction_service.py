"""
Position Price Extraction Service

Business logic for extracting candidate equity prices from today's positions
and submitting them to the equity price table with src_system = 'POSITION_UPLOAD'.

The extracted CSV is in the standard equity price upload format:
  security_label, currency_code, price_date, main_closing_price, isin
(price pre-calculated as market_value_fc / quantity)
This means the downloaded file can be reviewed by the user and then uploaded
through the existing equity price upload page with source = POSITION_UPLOAD.
"""

import csv
import io
import logging
from decimal import Decimal, InvalidOperation
from datetime import date
from typing import List, Dict, Any, Optional

from market_data.repositories.position_price_extraction_repository import (
    position_price_extraction_repository,
)
from market_data.repositories.equity_price_hive_repository import (
    equity_price_hive_repository,
)

logger = logging.getLogger(__name__)

# Source tag written to cis_equity_price when rows are uploaded via this flow
POSITION_UPLOAD_SOURCE = 'POSITION_UPLOAD'


class PositionPriceExtractionService:

    @staticmethod
    def _calc_price(market_value, quantity) -> Optional[Decimal]:
        """Return market_value / quantity rounded to 6 dp, or None if not calculable."""
        try:
            mv  = Decimal(str(market_value))
            qty = Decimal(str(quantity))
            if qty == 0:
                return None
            return (mv / qty).quantize(Decimal('0.000001'))
        except (InvalidOperation, ZeroDivisionError):
            return None

    @staticmethod
    def get_candidates(position_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return extraction candidates from today's (or specified) position snapshot.

        Each row contains:
          security_label, currency_code, price_date,
          market_value, quantity, isin, calculated_price
        """
        rows = position_price_extraction_repository.get_extraction_candidates(
            position_date=position_date
        )
        for row in rows:
            price = PositionPriceExtractionService._calc_price(
                row['market_value'], row['quantity']
            )
            row['calculated_price'] = float(price) if price is not None else None
        return rows

    @staticmethod
    def build_csv(rows: List[Dict[str, Any]]) -> str:
        """
        Serialise extraction rows to a CSV string in the standard equity price
        upload format (security_label, currency_code, price_date,
        main_closing_price, isin) so the file can be uploaded through the
        existing equity price upload page with source = POSITION_UPLOAD.
        """
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=['security_label', 'currency_code', 'price_date',
                        'main_closing_price', 'isin'],
            extrasaction='ignore',
            lineterminator='\r\n',
        )
        writer.writeheader()
        for row in rows:
            price = PositionPriceExtractionService._calc_price(
                row.get('market_value'), row.get('quantity')
            )
            writer.writerow({
                'security_label':    row.get('security_label', ''),
                'currency_code':     row.get('currency_code', ''),
                'price_date':        row.get('price_date', ''),
                'main_closing_price': str(price) if price is not None else '',
                'isin':              row.get('isin', ''),
            })
        return output.getvalue()

    @staticmethod
    def upload_rows(rows: List[Dict[str, Any]], username: str = 'SYSTEM') -> Dict[str, Any]:
        """
        Upsert pre-computed equity price rows with src_system = POSITION_UPLOAD.

        Args:
            rows: List of dicts with security_label, currency_code, price_date,
                  calculated_price (float), isin.  These are the rows that come
                  directly from get_candidates() / the extraction table.
            username: Performing user (for audit)

        Returns:
            Dict with success_count, failure_count, failures
        """
        from market_data.services.equity_price_service import EquityPriceService

        price_rows: List[Dict[str, Any]] = []
        skipped = []

        for row in rows:
            price = PositionPriceExtractionService._calc_price(
                row.get('market_value'), row.get('quantity')
            )
            if price is None:
                skipped.append({'row': row, 'error': 'Cannot compute price (quantity zero or invalid)'})
                continue
            price_rows.append({
                'security_label':    row.get('security_label', ''),
                'currency_code':     row.get('currency_code', ''),
                'price_date':        row.get('price_date', ''),
                'main_closing_price': str(price),
                'isin':              row.get('isin') or None,
                'src_system':        POSITION_UPLOAD_SOURCE,
            })

        result = EquityPriceService.bulk_upload_equity_prices(price_rows, username=username)

        result['failure_count'] += len(skipped)
        result['failures'] = result.get('failures', []) + skipped
        return result


position_price_extraction_service = PositionPriceExtractionService()
