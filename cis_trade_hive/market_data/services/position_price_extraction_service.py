"""
Position Price Extraction Service

Business logic for extracting candidate equity prices from today's positions
and submitting them to the equity price table with src_system = 'POSITION_UPLOAD'.
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
from market_data.services.equity_price_service import EquityPriceService

logger = logging.getLogger(__name__)

# Source tag written to cis_equity_price when rows are uploaded via this flow
POSITION_UPLOAD_SOURCE = 'POSITION_UPLOAD'


class PositionPriceExtractionService:

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
        # Compute the implied price for display purposes
        for row in rows:
            try:
                mv  = Decimal(str(row['market_value']))
                qty = Decimal(str(row['quantity']))
                row['calculated_price'] = float((mv / qty).quantize(Decimal('0.000001'))) if qty else None
            except (InvalidOperation, ZeroDivisionError):
                row['calculated_price'] = None
        return rows

    @staticmethod
    def build_csv(rows: List[Dict[str, Any]]) -> str:
        """
        Serialise extraction rows to a CSV string in position-upload format.

        Columns: security_label, currency_code, price_date, market_value, quantity, isin
        """
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=['security_label', 'currency_code', 'price_date',
                        'market_value', 'quantity', 'isin'],
            extrasaction='ignore',
            lineterminator='\r\n',
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({
                'security_label': row.get('security_label', ''),
                'currency_code':  row.get('currency_code', ''),
                'price_date':     row.get('price_date', ''),
                'market_value':   row.get('market_value', ''),
                'quantity':       row.get('quantity', ''),
                'isin':           row.get('isin', ''),
            })
        return output.getvalue()

    @staticmethod
    def upload_rows(rows: List[Dict[str, Any]], username: str = 'SYSTEM') -> Dict[str, Any]:
        """
        Convert position-format rows (market_value + quantity) to equity price rows
        and upsert them with src_system = POSITION_UPLOAD.

        Args:
            rows: List of dicts with security_label, currency_code, price_date,
                  market_value, quantity, isin
            username: Performing user (for audit)

        Returns:
            Result dict from EquityPriceService.bulk_upload_equity_prices
        """
        price_rows: List[Dict[str, Any]] = []
        skipped = []

        for row in rows:
            try:
                mv  = Decimal(str(row.get('market_value', '') or 0))
                qty = Decimal(str(row.get('quantity', '') or 0))
                if qty == 0:
                    skipped.append({'row': row, 'error': 'quantity is zero — cannot compute price'})
                    continue
                calculated_price = (mv / qty).quantize(Decimal('0.000001'))
                price_rows.append({
                    'security_label':     row.get('security_label', ''),
                    'currency_code':      row.get('currency_code', ''),
                    'price_date':         row.get('price_date', ''),
                    'main_closing_price': str(calculated_price),
                    'isin':               row.get('isin') or None,
                    'src_system':         POSITION_UPLOAD_SOURCE,
                })
            except InvalidOperation:
                skipped.append({'row': row, 'error': 'market_value or quantity is not a valid number'})
            except Exception as e:
                skipped.append({'row': row, 'error': str(e)})

        result = EquityPriceService.bulk_upload_equity_prices(price_rows, username=username)

        # Merge skipped rows into failures
        result['failure_count'] += len(skipped)
        result['failures'] = result.get('failures', []) + skipped
        return result


position_price_extraction_service = PositionPriceExtractionService()
