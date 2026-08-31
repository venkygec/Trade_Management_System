"""
Equity Price Service
Business logic for equity price operations following SOLID principles.

Uses composite primary key: (currency_code, security_label, price_date)
Supports versioned price history - multiple prices per security (one per date).

SOLID Principles Applied:
- Single Responsibility: Each method has one clear purpose
- Open/Closed: Extensible for new price sources
- Liskov Substitution: Service layer can be substituted
- Interface Segregation: Clean service interface
- Dependency Inversion: Depends on repository abstraction

Author: CisTrade Team
Last Updated: 2026-01-31
"""

from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import csv
import io
import logging
import threading
import time
from django.core.exceptions import ValidationError
from django.core.cache import cache

from market_data.repositories.equity_price_hive_repository import equity_price_hive_repository

logger = logging.getLogger(__name__)

# Per-key write locks: serialise concurrent edits to the same equity price row
# within a single Gunicorn worker process.  Combined with the REFRESH+token
# check this catches same-second simultaneous saves.
_write_locks: Dict[str, threading.Lock] = {}
_write_locks_meta = threading.Lock()  # protects the dict itself


def _get_write_lock(key: str) -> threading.Lock:
    with _write_locks_meta:
        if key not in _write_locks:
            _write_locks[key] = threading.Lock()
        return _write_locks[key]


class EquityPriceService:
    """
    Service for equity price business logic.

    Uses composite key: (currency_code, security_label, price_date)
    Supports versioned price history - multiple prices per security (one per date).

    Handles:
    - Equity price CRUD operations with business rules
    - Price validation and comparison
    - Historical price analysis
    - Caching for performance
    """

    # Cache TTL in seconds (15 minutes for equity prices)
    CACHE_TTL = 900

    @staticmethod
    def get_equity_prices(
        limit: int = 100,
        currency_code: Optional[str] = None,
        security_label: Optional[str] = None,
        isin: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        src_system: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get equity prices with filters and business logic.

        Args:
            limit: Maximum number of records (capped at 1000 for performance)
            currency_code: Filter by currency
            security_label: Filter by security name (supports wildcard search)
            isin: Filter by ISIN
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format

        Returns:
            List of equity price records

        Raises:
            ValidationError: If inputs are invalid
        """
        # Validate and cap limit
        if limit > 50000:
            logger.warning(f"Limit {limit} exceeds maximum, capping at 50000")
            limit = 50000

        # Validate date formats
        if date_from:
            EquityPriceService._validate_date_format(date_from)
        if date_to:
            EquityPriceService._validate_date_format(date_to)

        try:
            prices = equity_price_hive_repository.get_all_equity_prices(
                limit=limit,
                currency_code=currency_code,
                security_label=security_label,
                isin=isin,
                date_from=date_from,
                date_to=date_to,
                src_system=src_system
            )

            logger.info(f"Retrieved {len(prices)} equity prices with filters")
            return prices

        except Exception as e:
            logger.error(f"Error in get_equity_prices: {str(e)}")
            raise

    @staticmethod
    def get_write_lock(currency_code: str, security_label: str, price_date: str) -> threading.Lock:
        """Return the per-key threading.Lock for serialising concurrent writes."""
        key = f"{currency_code}|{security_label}|{price_date}"
        return _get_write_lock(key)

    @staticmethod
    def refresh_table():
        """Force Impala to see the latest Kudu commits before a lock check re-read."""
        equity_price_hive_repository.refresh_table()

    @staticmethod
    def get_equity_price_by_key(currency_code: str, security_label: str, price_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get equity price by composite key.

        Args:
            currency_code: Currency code (part of composite key)
            security_label: Security label (part of composite key)
            price_date: Price date in YYYY-MM-DD format (optional - if None, returns latest)

        Returns:
            Equity price record or None

        Raises:
            ValidationError: If keys are invalid
        """
        if not currency_code or not security_label:
            raise ValidationError("Both currency_code and security_label are required")

        try:
            price = equity_price_hive_repository.get_equity_price_by_key(currency_code, security_label, price_date)

            if price:
                logger.info(f"Found equity price: {currency_code}/{security_label}/{price_date or 'latest'}")
            else:
                logger.warning(f"No equity price found: {currency_code}/{security_label}/{price_date or 'latest'}")

            return price

        except Exception as e:
            logger.error(f"Error getting equity price {currency_code}/{security_label}: {str(e)}")
            raise

    @staticmethod
    def create_equity_price(
        equity_price_data: Dict[str, Any],
        user: str = 'SYSTEM'
    ) -> bool:
        """
        Create new equity price with validation.

        Args:
            equity_price_data: Dictionary with price data
            user: Username creating the price

        Returns:
            True if successful

        Raises:
            ValidationError: If validation fails
        """
        # Validate required fields
        required_fields = ['currency_code', 'security_label', 'price_date', 'main_closing_price']
        for field in required_fields:
            if not equity_price_data.get(field):
                raise ValidationError(f"{field} is required")

        # Validate date format
        EquityPriceService._validate_date_format(equity_price_data['price_date'])

        # Validate price is not negative (0 is allowed, e.g. suspended securities)
        price = equity_price_data.get('main_closing_price')
        try:
            price_decimal = Decimal(str(price))
            if price_decimal < 0:
                raise ValidationError("Main closing price must not be negative")
        except (ValueError, TypeError):
            raise ValidationError("Invalid price value")

        # Add created_by
        equity_price_data['created_by'] = user

        # Generate timestamp if not provided
        if not equity_price_data.get('price_timestamp'):
            equity_price_data['price_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            success = equity_price_hive_repository.upsert_equity_price(equity_price_data, username=user)

            if success:
                logger.info(f"Successfully created equity price for {equity_price_data.get('currency_code')}/"
                           f"{equity_price_data.get('security_label')} on {equity_price_data.get('price_date')}")
                # Clear cache
                EquityPriceService.clear_cache()
            else:
                logger.error(f"Failed to create equity price")

            return success

        except Exception as e:
            logger.error(f"Error creating equity price: {str(e)}")
            raise

    @staticmethod
    def update_equity_price(
        currency_code: str,
        security_label: str,
        equity_price_data: Dict[str, Any],
        user: str = 'SYSTEM',
        price_date: Optional[str] = None
    ) -> bool:
        """
        Update existing equity price with validation.

        Args:
            currency_code: Currency code (part of composite key)
            security_label: Security label (part of composite key)
            equity_price_data: Dictionary with fields to update
            user: Username updating the price
            price_date: Price date (part of composite key). If None, updates the latest price.

        Returns:
            True if successful

        Raises:
            ValidationError: If validation fails
        """
        if not currency_code or not security_label:
            raise ValidationError("Both currency_code and security_label are required")

        # Validate date format if provided
        if 'price_date' in equity_price_data:
            EquityPriceService._validate_date_format(equity_price_data['price_date'])

        # Validate price if provided
        if 'main_closing_price' in equity_price_data:
            price = equity_price_data['main_closing_price']
            try:
                price_decimal = Decimal(str(price))
                if price_decimal < 0:
                    raise ValidationError("Main closing price must not be negative")
            except (ValueError, TypeError):
                raise ValidationError("Invalid price value")

        # Save old values to history before updating
        try:
            existing = equity_price_hive_repository.get_equity_price_by_key(
                currency_code, security_label, price_date
            )
            if existing:
                equity_price_hive_repository.save_to_history(existing, user, 'UPDATE')
        except Exception as e:
            logger.warning(f"Could not save history before update: {str(e)}")

        # Add updated_by
        equity_price_data['updated_by'] = user

        try:
            success = equity_price_hive_repository.update_equity_price(
                currency_code,
                security_label,
                equity_price_data,
                username=user,
                price_date=price_date
            )

            if success:
                logger.info(f"Successfully updated equity price: {currency_code}/{security_label}/{price_date or 'latest'}")
                # Clear cache
                EquityPriceService.clear_cache()
            else:
                logger.error(f"Failed to update equity price: {currency_code}/{security_label}/{price_date or 'latest'}")

            return success

        except Exception as e:
            logger.error(f"Error updating equity price {currency_code}/{security_label}: {str(e)}")
            raise

    @staticmethod
    def delete_equity_price(
        currency_code: str,
        security_label: str,
        user: str = 'SYSTEM',
        price_date: Optional[str] = None
    ) -> bool:
        """
        Soft delete equity price.

        Args:
            currency_code: Currency code (part of composite key)
            security_label: Security label (part of composite key)
            user: Username deleting the price
            price_date: Price date (part of composite key). If None, deletes the latest price.

        Returns:
            True if successful

        Raises:
            ValidationError: If keys are invalid
        """
        if not currency_code or not security_label:
            raise ValidationError("Both currency_code and security_label are required")

        # Save old values to history before deleting
        try:
            existing = equity_price_hive_repository.get_equity_price_by_key(
                currency_code, security_label, price_date
            )
            if existing:
                equity_price_hive_repository.save_to_history(existing, user, 'DELETE')
        except Exception as e:
            logger.warning(f"Could not save history before delete: {str(e)}")

        try:
            success = equity_price_hive_repository.delete_equity_price(
                currency_code,
                security_label,
                deleted_by=user,
                price_date=price_date
            )

            if success:
                logger.info(f"Successfully deleted equity price: {currency_code}/{security_label}/{price_date or 'latest'}")
                # Clear cache
                EquityPriceService.clear_cache()
            else:
                logger.error(f"Failed to delete equity price: {currency_code}/{security_label}/{price_date or 'latest'}")

            return success

        except Exception as e:
            logger.error(f"Error deleting equity price {currency_code}/{security_label}: {str(e)}")
            raise

    @staticmethod
    def get_price_history(
        security_label: str,
        days: int = 30,
        reference_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get historical prices for a security.

        Args:
            security_label: Security name
            days: Number of days of history (default 30, max 365)
            reference_date: Anchor date (YYYY-MM-DD) for the window; defaults to today.

        Returns:
            List of historical prices sorted by date descending

        Raises:
            ValidationError: If security_label is invalid or days is out of range
        """
        if not security_label:
            raise ValidationError("Security label is required")

        if days < 1 or days > 365:
            raise ValidationError("Days must be between 1 and 365")

        try:
            prices = equity_price_hive_repository.get_price_history(
                security_label=security_label,
                days=days,
                reference_date=reference_date,
            )

            logger.info(f"Retrieved {len(prices)} historical prices for {security_label} ({days} days)")
            return prices

        except Exception as e:
            logger.error(f"Error getting price history for {security_label}: {str(e)}")
            raise

    @staticmethod
    def get_version_history(
        currency_code: str,
        security_label: str,
        price_date: str
    ) -> List[Dict[str, Any]]:
        """
        Get edit history for a specific equity price record.

        Args:
            currency_code: Currency code
            security_label: Security label
            price_date: Price date

        Returns:
            List of historical versions (old values before edits)
        """
        if not currency_code or not security_label or not price_date:
            return []

        try:
            return equity_price_hive_repository.get_version_history(
                currency_code, security_label, price_date
            )
        except Exception as e:
            logger.error(f"Error getting version history: {str(e)}")
            return []

    @staticmethod
    def get_all_history_for_security(
        currency_code: str,
        security_label: str
    ) -> List[Dict[str, Any]]:
        """
        Get all edit history for a security across all dates.

        Args:
            currency_code: Currency code
            security_label: Security label

        Returns:
            List of historical versions
        """
        if not currency_code or not security_label:
            return []

        try:
            return equity_price_hive_repository.get_all_history_for_security(
                currency_code, security_label
            )
        except Exception as e:
            logger.error(f"Error getting security history: {str(e)}")
            return []

    @staticmethod
    def get_statistics() -> Dict[str, Any]:
        """
        Get equity price statistics.

        Returns:
            Dictionary with statistics:
            - total_prices: Total number of equity price records
            - unique_securities: Number of unique securities
            - unique_currencies: Number of currencies
            - latest_date: Most recent price date
            - earliest_date: Oldest price date
        """
        try:
            stats = equity_price_hive_repository.get_statistics()
            logger.info("Retrieved equity price statistics")
            return stats

        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
            raise

    @staticmethod
    def get_price_trend(
        security_label: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Analyze price trend over a period.

        Args:
            security_label: Security name
            days: Number of days to analyze

        Returns:
            Dictionary with trend analysis:
            - trend_direction: 'up', 'down', or 'stable'
            - change_amount: Absolute change
            - change_percent: Percentage change
            - volatility: Standard deviation of prices
            - min_price: Lowest price in period
            - max_price: Highest price in period
        """
        prices = EquityPriceService.get_price_history(security_label, days)

        if len(prices) < 2:
            return {
                'trend_direction': 'unknown',
                'change_amount': 0,
                'change_percent': 0,
                'volatility': 0,
                'min_price': None,
                'max_price': None,
                'data_points': len(prices)
            }

        # Extract prices
        price_values = [Decimal(str(p['main_closing_price'])) for p in prices if p.get('main_closing_price')]

        if not price_values:
            return {'error': 'No price data available'}

        first_price = price_values[0]   # Oldest (prices sorted ASC by price_date)
        last_price = price_values[-1]   # Newest
        change = last_price - first_price
        change_pct = (change / first_price * 100) if first_price > 0 else 0

        # Determine trend
        if abs(change_pct) < 0.5:
            trend = 'stable'
        elif change > 0:
            trend = 'up'
        else:
            trend = 'down'

        # Calculate volatility (standard deviation)
        mean_price = sum(price_values) / len(price_values)
        variance = sum((p - mean_price) ** 2 for p in price_values) / len(price_values)
        volatility = float(variance ** Decimal('0.5'))

        result = {
            'security_label': security_label,
            'period_days': days,
            'data_points': len(price_values),
            'trend_direction': trend,
            'change_amount': float(change),
            'change_percent': float(change_pct),
            'volatility': volatility,
            'min_price': float(min(price_values)),
            'max_price': float(max(price_values)),
            'avg_price': float(mean_price),
            'first_price': float(first_price),
            'last_price': float(last_price)
        }

        logger.info(f"Analyzed {days}-day trend for {security_label}: {trend} ({change_pct:.2f}%)")
        return result

    # Validation helper methods

    @staticmethod
    def _validate_date_format(date_str: str) -> None:
        """
        Validate date format is YYYY-MM-DD.

        Raises:
            ValidationError: If format is invalid
        """
        if not date_str:
            raise ValidationError("Date is required")

        if len(date_str) != 10:
            raise ValidationError("Date must be in YYYY-MM-DD format")

        if date_str[4] != '-' or date_str[7] != '-':
            raise ValidationError("Date must be in YYYY-MM-DD format")

        # Try parsing to ensure it's a valid date
        try:
            year = int(date_str[0:4])
            month = int(date_str[5:7])
            day = int(date_str[8:10])
            datetime(year, month, day)
        except ValueError:
            raise ValidationError(f"Invalid date: {date_str}")

    # Required CSV columns (case-insensitive header match)
    # Format A: direct price  →  main_closing_price required
    # Format B: position upload → market_value + quantity required (price = market_value / quantity)
    UPLOAD_REQUIRED_COLUMNS = {'security_label', 'currency_code', 'price_date', 'main_closing_price'}
    UPLOAD_POSITION_REQUIRED_COLUMNS = {'security_label', 'currency_code', 'price_date', 'market_value', 'quantity'}
    UPLOAD_OPTIONAL_COLUMNS = {'isin'}

    @staticmethod
    def _detect_upload_format(headers: list) -> str:
        """
        Detect which upload format is being used.

        Returns 'price' (direct main_closing_price) or 'position' (market_value+quantity).
        Raises ValueError if neither set of required columns is present.
        """
        header_set = set(headers)
        has_price_format = EquityPriceService.UPLOAD_REQUIRED_COLUMNS.issubset(header_set)
        has_position_format = EquityPriceService.UPLOAD_POSITION_REQUIRED_COLUMNS.issubset(header_set)
        if has_position_format:
            return 'position'
        if has_price_format:
            return 'price'
        # Neither format satisfied — report the closer match
        missing_price = EquityPriceService.UPLOAD_REQUIRED_COLUMNS - header_set
        missing_position = EquityPriceService.UPLOAD_POSITION_REQUIRED_COLUMNS - header_set
        if len(missing_position) <= len(missing_price):
            raise ValueError(f"Missing required columns for position format: {', '.join(sorted(missing_position))}")
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing_price))}")

    @staticmethod
    def build_upload_preview(file_obj, src_system: str = 'CIS') -> Dict[str, Any]:
        """
        Parse and validate a CSV file for equity price upload.

        Supports two formats:
          Format A (direct price):   security_label, currency_code, price_date, main_closing_price [, isin]
          Format B (position upload): security_label, currency_code, price_date, market_value, quantity [, isin]
                                      price is calculated as market_value / quantity.

        Returns a dict with:
          - valid_rows    : list of clean dicts ready for upsert
          - invalid_rows  : list of {'row': n, 'data': {}, 'errors': [...]}
          - total         : total rows parsed
          - columns_found : list of header columns detected
          - upload_format : 'price' or 'position'
          - error         : top-level parse error string (or None)
        """
        result = {
            'valid_rows': [],
            'invalid_rows': [],
            'total': 0,
            'columns_found': [],
            'upload_format': None,
            'error': None,
        }

        try:
            raw = file_obj.read()
            try:
                text = raw.decode('utf-8-sig')
            except UnicodeDecodeError:
                text = raw.decode('latin-1')

            reader = csv.DictReader(io.StringIO(text))
            headers = [h.strip().lower() for h in (reader.fieldnames or [])]
            result['columns_found'] = headers

            try:
                upload_format = EquityPriceService._detect_upload_format(headers)
            except ValueError as e:
                result['error'] = str(e)
                return result

            result['upload_format'] = upload_format

            for row_num, raw_row in enumerate(reader, start=2):
                row = {k.strip().lower(): (v or '').strip() for k, v in raw_row.items()}
                errors = []

                security_label = row.get('security_label', '')
                currency_code  = row.get('currency_code', '')
                price_date     = row.get('price_date', '')
                isin           = row.get('isin', '') or None

                if not security_label:
                    errors.append('security_label is required')
                if not currency_code:
                    errors.append('currency_code is required')
                if not price_date:
                    errors.append('price_date is required')
                else:
                    try:
                        EquityPriceService._validate_date_format(price_date)
                    except ValidationError as e:
                        errors.append(str(e.message))

                price_val = None

                if upload_format == 'position':
                    # Format B: derive price from market_value / quantity
                    mv_str  = row.get('market_value', '')
                    qty_str = row.get('quantity', '')
                    if not mv_str:
                        errors.append('market_value is required')
                    if not qty_str:
                        errors.append('quantity is required')
                    if mv_str and qty_str:
                        try:
                            mv_val  = Decimal(mv_str)
                            qty_val = Decimal(qty_str)
                            if qty_val == 0:
                                errors.append('quantity must not be zero')
                            else:
                                price_val = (mv_val / qty_val).quantize(Decimal('0.000001'))
                                if price_val < 0:
                                    errors.append('calculated price (market_value / quantity) must not be negative')
                        except InvalidOperation:
                            errors.append('market_value and quantity must be valid numbers')
                else:
                    # Format A: price provided directly
                    price_str = row.get('main_closing_price', '')
                    if not price_str:
                        errors.append('main_closing_price is required')
                    else:
                        try:
                            price_val = Decimal(price_str)
                            if price_val < 0:
                                errors.append('main_closing_price must not be negative')
                        except InvalidOperation:
                            errors.append(f'main_closing_price "{price_str}" is not a valid number')
                            price_val = None

                result['total'] += 1

                if errors:
                    result['invalid_rows'].append({
                        'row': row_num,
                        'data': row,
                        'errors': errors,
                    })
                else:
                    result['valid_rows'].append({
                        'security_label':     security_label,
                        'currency_code':      currency_code,
                        'price_date':         price_date,
                        'main_closing_price': str(price_val),
                        'isin':               isin,
                        'src_system':         src_system,
                    })

        except Exception as e:
            logger.error(f"Error parsing equity price CSV: {e}")
            result['error'] = 'Unexpected error while parsing the CSV file.'

        return result

    @staticmethod
    def bulk_upload_equity_prices(
        rows: List[Dict[str, Any]],
        username: str = 'SYSTEM',
        chunk_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Upsert a list of equity price rows (already validated).

        Returns:
          - success_count : int
          - failure_count : int
          - failures      : list of {'row': dict, 'error': str}
        """
        success_count = 0
        failure_count = 0
        failures = []

        for row in rows:
            try:
                currency_code  = row.get('currency_code', '')
                security_label = row.get('security_label', '')
                price_date     = row.get('price_date', '')

                # Block upload if an existing GMP record covers this composite key
                existing = equity_price_hive_repository.get_equity_price_by_key(
                    currency_code, security_label, price_date
                )
                if existing and str(existing.get('src_system', '')).upper() == 'GMP':
                    failure_count += 1
                    failures.append({
                        'row': row,
                        'error': f'Record already exists with src_system=GMP — upload blocked to protect source data',
                    })
                    continue

                is_update = existing is not None
                row['created_by'] = username
                ok = equity_price_hive_repository.upsert_equity_price(
                    row, username=username, is_update=is_update
                )
                if ok:
                    success_count += 1
                else:
                    failure_count += 1
                    failures.append({'row': row, 'error': 'Upsert returned False'})
            except Exception as e:
                failure_count += 1
                failures.append({'row': row, 'error': str(e)})
                logger.error(f"Error upserting row {row}: {e}")

        EquityPriceService.clear_cache()
        logger.info(f"Bulk upload: {success_count} success, {failure_count} failure")
        return {
            'success_count': success_count,
            'failure_count': failure_count,
            'failures': failures,
        }

    @staticmethod
    def clear_cache() -> None:
        """
        Clear all equity price caches.

        Useful after data updates or for troubleshooting.
        """
        cache_keys = [
            "equity_price_statistics"
        ]

        for key in cache_keys:
            cache.delete(key)

        logger.info(f"Cleared {len(cache_keys)} equity price cache keys")


# Singleton instance
equity_price_service = EquityPriceService()
