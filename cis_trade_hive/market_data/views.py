"""
Market Data Views
Handles FX Rate operations with Hive integration and CSV export.

Updated to use gmp_cis.gmp_cis_sta_dly_fx_rates external table.
"""

from django.shortcuts import render
from django.http import HttpResponse
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.exceptions import ValidationError
from datetime import datetime, timedelta
import csv
import json
import logging

from market_data.services import fx_rate_service
from market_data.services.position_price_extraction_service import position_price_extraction_service
from core.audit.audit_kudu_repository import audit_log_kudu_repository
from trade.repositories.trade_kudu_repository import trade_kudu_repository


class FXRateWrapper:
    """
    Wrapper to convert Hive dict data to object with attributes for template compatibility.

    Updated for gmp_cis_sta_dly_fx_rates external table structure.
    """
    def __init__(self, data, index=0):
        self.data = data

        # Map fields from external table structure
        self.currency_pair = data.get('currency_pair', '')
        self.base_currency = data.get('base_currency', '')
        self.quote_currency = data.get('quote_currency', '')
        self.bid_rate = data.get('bid_rate', 0)
        self.ask_rate = data.get('ask_rate', 0)
        self.mid_rate = data.get('mid_rate', 0)

        # Use mid_rate as the primary rate
        self.rate = self.mid_rate

        # Trade date fields (YYYYMMDD from table, YYYY-MM-DD display from repository)
        self.trade_date = data.get('trade_date', '')
        self.trade_date_display = data.get('trade_date_display', '')
        self.rate_date = self.trade_date_display or self.trade_date  # For backward compatibility

        # ETL processing date (YYYYMMDD - date when file was loaded)
        self.processing_date = data.get('processing_date', '')
        self.processing_date_display = self._format_date(self.processing_date) if self.processing_date else ''

        # Source and reference
        self.source = data.get('source', '')
        self.reference_id = data.get('reference_id', '')

        # Spread (calculated by repository)
        self.spread = data.get('spread', 0)
        self.spread_bps = data.get('spread_bps', 0)  # Basis points

        # Legacy fields for compatibility
        self.is_active = 'true'  # All records in external table are active
        self.rate_time = ''  # No time field in external table

        # Calculate spread percentage for display
        try:
            mid = float(self.mid_rate) if self.mid_rate else 0
            spread_val = float(self.spread) if self.spread else 0
            self.spread_percentage = (spread_val / mid * 100) if mid > 0 else 0
        except (ValueError, TypeError):
            self.spread_percentage = 0

        # Determine freshness based on trade_date
        self.freshness = self._calculate_freshness()

        # Generate ID from hash
        self.id = abs(hash(self.currency_pair + str(self.trade_date) + str(index))) % 1000000

    def _format_date(self, date_str: str) -> str:
        """Format YYYYMMDD to YYYY-MM-DD"""
        if len(date_str) == 8:
            return f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

    def _calculate_freshness(self):
        """
        Calculate freshness status based on trade_date.

        Since we only have date (not time), we determine freshness by:
        - Same date as today = fresh
        - 1-2 days old = normal
        - >2 days old = stale
        """
        try:
            if not self.trade_date:
                return 'unknown'

            # Parse trade_date (YYYYMMDD format)
            trade_date_str = str(self.trade_date)
            if len(trade_date_str) == 8:
                year = int(trade_date_str[0:4])
                month = int(trade_date_str[4:6])
                day = int(trade_date_str[6:8])
                rate_date = datetime(year, month, day)
            else:
                return 'unknown'

            # Calculate age in days
            today = datetime.now().date()
            age_days = (today - rate_date.date()).days

            if age_days == 0:
                return 'fresh'
            elif age_days <= 2:
                return 'normal'
            else:
                return 'stale'
        except Exception:
            return 'unknown'

    def get_spread(self):
        """Get bid-ask spread"""
        return self.spread

    def get_spread_bps(self):
        """Get bid-ask spread in basis points"""
        return self.spread_bps

    def get_spread_percentage(self):
        """Get bid-ask spread as percentage"""
        return self.spread_percentage

    def get_freshness_status(self):
        """Get freshness status for display"""
        return self.freshness

    def get_freshness_color(self):
        """Get Bootstrap color class for freshness"""
        colors = {
            'fresh': 'success',
            'normal': 'info',
            'stale': 'warning',
            'unknown': 'secondary'
        }
        return colors.get(self.freshness, 'secondary')


def fx_rate_list(request):
    """
    List all FX rates with search, filter, and CSV export.

    Uses gmp_cis_sta_dly_fx_rates external table via service layer.
    """
    # Get filters
    currency_pair = request.GET.get('currency_pair', '').strip()
    date_from = request.GET.get('date_from', '').strip()
    date_to = request.GET.get('date_to', '').strip()
    base_currency = request.GET.get('base_currency', '').strip()
    source_filter = request.GET.get('source', '').strip()
    export = request.GET.get('export', '').strip()

    # Convert date formats from YYYY-MM-DD to YYYYMMDD if provided
    date_from_fmt = date_from.replace('-', '') if date_from else None
    date_to_fmt = date_to.replace('-', '') if date_to else None

    # Get FX rates from service layer
    try:
        fx_rates_data = fx_rate_service.get_fx_rates(
            limit=1000,
            currency_pair=currency_pair if currency_pair else None,
            date_from=date_from_fmt,
            date_to=date_to_fmt,
            base_currency=base_currency if base_currency else None,
            source=source_filter if source_filter else None
        )
    except ValidationError as e:
        fx_rates_data = []
        # TODO: Show error message to user

    # CSV Export
    if export == 'csv':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="fx_rates.csv"'

        writer = csv.writer(response)
        writer.writerow([
            'Currency Pair', 'Base', 'Quote', 'Rate', 'Bid', 'Ask', 'Mid',
            'Spread', 'Spread %', 'Date', 'Time', 'Source', 'Active'
        ])

        for rate_data in fx_rates_data:
            rate = FXRateWrapper(rate_data)
            writer.writerow([
                rate.currency_pair,
                rate.base_currency,
                rate.quote_currency,
                rate.rate,
                rate.bid_rate,
                rate.ask_rate,
                rate.mid_rate,
                f"{rate.spread:.10f}",
                f"{rate.spread_percentage:.4f}",
                rate.rate_date,
                rate.rate_time,
                rate.source,
                rate.is_active
            ])

        # Log export to Hive - Get user info from session (ACL authentication)
        username = request.session.get('user_login', 'anonymous')
        user_id = str(request.session.get('user_id', ''))
        user_email = request.session.get('user_email', '')

        audit_log_kudu_repository.log_action(
            user_id=user_id,
            username=username,
            user_email=user_email,
            action_type='EXPORT',
            entity_type='FX_RATE',
            action_description=f'Exported {len(fx_rates_data)} FX rates to CSV from Hive',
            status='SUCCESS',
            request_method='GET',
            request_path=request.path,
            ip_address=request.META.get('REMOTE_ADDR'),
            user_agent=request.META.get('HTTP_USER_AGENT', '')
        )

        return response

    # Wrap dictionaries in objects for template compatibility
    wrapped_rates = [FXRateWrapper(r, idx) for idx, r in enumerate(fx_rates_data)]

    # Pagination
    paginator = Paginator(wrapped_rates, 25)
    page = request.GET.get('page', 1)

    try:
        fx_rates = paginator.page(page)
    except PageNotAnInteger:
        fx_rates = paginator.page(1)
    except EmptyPage:
        fx_rates = paginator.page(paginator.num_pages if paginator.num_pages > 0 else 1)

    # Get unique currency pairs, base currencies, and sources for filters
    currency_pairs = fx_rate_service.get_currency_pairs()
    base_currencies = fx_rate_service.get_base_currencies()
    sources = fx_rate_service.get_sources()

    # Log view to Hive - Commented out for VIEW actions (only log CREATE, UPDATE, DELETE)
    # username = request.session.get('user_login', 'anonymous')
    # user_id = str(request.session.get('user_id', ''))
    # user_email = request.session.get('user_email', '')

    # audit_log_kudu_repository.log_action(
    #     user_id=user_id,
    #     username=username,
    #     user_email=user_email,
    #     action_type='VIEW',
    #     entity_type='FX_RATE',
    #     action_description=f'Viewed FX rate list from Hive ({len(fx_rates_data)} rates)',
    #     status='SUCCESS',
    #     request_method='GET',
    #     request_path=request.path,
    #     ip_address=request.META.get('REMOTE_ADDR'),
    #     user_agent=request.META.get('HTTP_USER_AGENT', '')
    # )

    context = {
        'page_obj': fx_rates,
        'currency_pair': currency_pair,
        'date_from': date_from,
        'date_to': date_to,
        'base_currency': base_currency,
        'source_filter': source_filter,
        'currency_pairs': currency_pairs,
        'base_currencies': base_currencies,
        'sources': sources,
        'total_count': len(fx_rates_data),
        'using_hive': True,
    }

    return render(request, 'market_data/fx_rate_list.html', context)


def market_data_dashboard(request):
    """
    Market Data dashboard with statistics for FX Rates and Equity Prices.

    Shows combined statistics and latest data for both modules.
    """
    # Get FX Rate statistics
    fx_stats = fx_rate_service.get_statistics()

    # Get latest FX rates
    latest_fx_rates = fx_rate_service.get_latest_rates(limit=10)
    wrapped_fx_rates = [FXRateWrapper(r, idx) for idx, r in enumerate(latest_fx_rates)]

    # Get unique base currencies
    base_currencies = fx_rate_service.get_base_currencies()

    # Get Equity Price statistics
    equity_price_stats = equity_price_service.get_statistics()

    # Get latest equity prices
    latest_equity_prices = equity_price_service.get_equity_prices(limit=10)
    wrapped_equity_prices = [EquityPriceWrapper(r, idx) for idx, r in enumerate(latest_equity_prices)]

    # Log view to audit - Commented out for VIEW actions (only log CREATE, UPDATE, DELETE)
    # username = request.session.get('user_login', 'anonymous')
    # user_id = str(request.session.get('user_id', ''))
    # user_email = request.session.get('user_email', '')

    # audit_log_kudu_repository.log_action(
    #     user_id=user_id,
    #     username=username,
    #     user_email=user_email,
    #     action_type='VIEW',
    #     entity_type='MARKET_DATA',
    #     action_description='Viewed Market Data dashboard',
    #     status='SUCCESS',
    #     request_method='GET',
    #     request_path=request.path,
    #     ip_address=request.META.get('REMOTE_ADDR'),
    #     user_agent=request.META.get('HTTP_USER_AGENT', '')
    # )

    context = {
        'fx_stats': fx_stats,
        'latest_fx_rates': wrapped_fx_rates,
        'base_currencies': base_currencies,
        'equity_price_stats': equity_price_stats,
        'latest_equity_prices': wrapped_equity_prices,
        'using_hive': True,
    }

    return render(request, 'market_data/market_data_dashboard.html', context)


# Keep old function name for backward compatibility
def fx_rate_dashboard(request):
    """Redirect to Market Data Dashboard for backward compatibility."""
    return market_data_dashboard(request)


def fx_rate_detail(request, currency_pair):
    """
    View FX rate details and history for a specific currency pair.

    Uses service layer to get historical rates and trend analysis.
    """
    # Get rate history (last 30 days) from service
    try:
        history = fx_rate_service.get_rate_history(currency_pair, days=30)
    except ValidationError:
        history = []

    if not history:
        # No data found
        context = {
            'currency_pair': currency_pair,
            'rate': None,
            'history': [],
            'chart_data': {'dates': [], 'rates': []},
        }
        return render(request, 'market_data/fx_rate_detail.html', context)

    # Get latest rate
    latest_rate_data = history[-1] if history else None
    latest_rate = FXRateWrapper(latest_rate_data) if latest_rate_data else None

    # Wrap history
    wrapped_history = [FXRateWrapper(r, idx) for idx, r in enumerate(history)]

    # Prepare chart data (JSON for Chart.js)
    chart_data = {
        'dates': [r.rate_date for r in wrapped_history],
        'rates': [float(r.rate) if r.rate else 0 for r in wrapped_history],
        'bid_rates': [float(r.bid_rate) if r.bid_rate else 0 for r in wrapped_history],
        'ask_rates': [float(r.ask_rate) if r.ask_rate else 0 for r in wrapped_history],
    }

    # Log view to Hive - Commented out (only log CREATE, UPDATE, DELETE)
    # username = request.session.get('user_login', 'anonymous')
    # user_id = str(request.session.get('user_id', ''))
    # user_email = request.session.get('user_email', '')

    # audit_log_kudu_repository.log_action(
    #     user_id=user_id,
    #     username=username,
    #     user_email=user_email,
    #     action_type='VIEW',
    #     entity_type='FX_RATE',
    #     entity_id=currency_pair,
    #     action_description=f'Viewed FX rate detail for {currency_pair}',
    #     status='SUCCESS',
    #     request_method='GET',
    #     request_path=request.path,
    #     ip_address=request.META.get('REMOTE_ADDR'),
    #     user_agent=request.META.get('HTTP_USER_AGENT', '')
    # )

    # Convert chart data to JSON for JavaScript
    import json
    chart_data_json = json.dumps(chart_data)

    context = {
        'currency_pair': currency_pair,
        'rate': latest_rate,
        'history': wrapped_history[-10:],  # Show last 10 records
        'chart_data': chart_data_json,
        'using_hive': True,
    }

    return render(request, 'market_data/fx_rate_detail.html', context)


# ============================================================================
# EQUITY PRICE VIEWS
# ============================================================================

from market_data.services.equity_price_service import equity_price_service
from market_data.services.equity_price_dropdown_service import equity_price_dropdown_service
from django.shortcuts import redirect
from django.http import JsonResponse
from django.views.decorators.http import require_POST


class EquityPriceWrapper:
    """
    Wrapper to convert Hive dict data to object with attributes for template compatibility.

    Uses composite key: (currency_code, security_label)
    """
    def __init__(self, data, index=0):
        self.data = data

        # Map fields from Kudu table - Composite key fields
        self.currency_code = data.get('currency_code', '')
        self.security_label = data.get('security_label', '')

        # Business fields
        self.security_description = data.get('security_description', '')
        self.isin = data.get('isin', '')
        self.price_date = data.get('price_date', '')
        self.main_closing_price = data.get('main_closing_price', 0)
        self.price_timestamp = data.get('price_timestamp', '')
        self.price_datetime = data.get('price_datetime', '')
        self.src_system = data.get('src_system', 'CIS')

        # Audit fields
        self.is_active = data.get('is_active', True)
        self.created_by = data.get('created_by', '')
        self.created_at_display = data.get('created_at_display', '')
        self.updated_by = data.get('updated_by', '')
        self.updated_at_display = data.get('updated_at_display', '')

        # Generate ID for template (hash of composite key)
        self.id = abs(hash(f"{self.currency_code}/{self.security_label}")) % 1000000


def equity_price_list(request):
    """
    List all equity prices with search, filter, and CSV export.
    """
    # Get filters
    currency_code = request.GET.get('currency_code', '').strip()
    security_label = request.GET.get('security_label', '').strip()
    isin = request.GET.get('isin', '').strip()
    date_from = request.GET.get('date_from', '').strip()
    date_to = request.GET.get('date_to', '').strip()
    src_system = request.GET.get('src_system', '').strip()
    export = request.GET.get('export', '').strip()

    # Get equity prices from service layer
    try:
        equity_prices_data = equity_price_service.get_equity_prices(
            limit=1000,
            currency_code=currency_code if currency_code else None,
            security_label=security_label if security_label else None,
            isin=isin if isin else None,
            date_from=date_from if date_from else None,
            date_to=date_to if date_to else None,
            src_system=src_system if src_system else None
        )
    except ValidationError as e:
        equity_prices_data = []

    # CSV Export
    if export == 'csv':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="equity_prices.csv"'

        writer = csv.writer(response)
        writer.writerow([
            'Currency', 'Security', 'Security Description', 'ISIN', 'Date', 'Price',
            'Timestamp', 'Source System', 'Created By', 'Created At'
        ])

        for price_data in equity_prices_data:
            price = EquityPriceWrapper(price_data)
            writer.writerow([
                price.currency_code,
                price.security_label,
                price.security_description,
                price.isin,
                price.price_date,
                price.main_closing_price,
                price.price_datetime,
                price.src_system,
                price.created_by,
                price.created_at_display
            ])

        # Log export
        username = request.session.get('user_login', 'anonymous')
        user_id = str(request.session.get('user_id', ''))
        user_email = request.session.get('user_email', '')

        audit_log_kudu_repository.log_action(
            user_id=user_id,
            username=username,
            user_email=user_email,
            action_type='EXPORT',
            entity_type='EQUITY_PRICE',
            action_description=f'Exported {len(equity_prices_data)} equity prices to CSV',
            status='SUCCESS',
            request_method='GET',
            request_path=request.path,
            ip_address=request.META.get('REMOTE_ADDR'),
            user_agent=request.META.get('HTTP_USER_AGENT', '')
        )

        return response

    # Wrap dictionaries in objects for template compatibility
    wrapped_prices = [EquityPriceWrapper(p, idx) for idx, p in enumerate(equity_prices_data)]

    # Pagination
    paginator = Paginator(wrapped_prices, 25)
    page = request.GET.get('page', 1)

    try:
        equity_prices = paginator.page(page)
    except PageNotAnInteger:
        equity_prices = paginator.page(1)
    except EmptyPage:
        equity_prices = paginator.page(paginator.num_pages if paginator.num_pages > 0 else 1)

    # Get dropdown options for filters
    dropdown_options = equity_price_dropdown_service.get_all_dropdown_options(
        user=request.session.get('user_login', 'SYSTEM')
    )

    # Log view - Commented out (only log CREATE, UPDATE, DELETE)
    # username = request.session.get('user_login', 'anonymous')
    # user_id = str(request.session.get('user_id', ''))
    # user_email = request.session.get('user_email', '')

    # audit_log_kudu_repository.log_action(
    #     user_id=user_id,
    #     username=username,
    #     user_email=user_email,
    #     action_type='VIEW',
    #     entity_type='EQUITY_PRICE',
    #     action_description=f'Viewed equity price list ({len(equity_prices_data)} prices)',
    #     status='SUCCESS',
    #     request_method='GET',
    #     request_path=request.path,
    #     ip_address=request.META.get('REMOTE_ADDR'),
    #     user_agent=request.META.get('HTTP_USER_AGENT', '')
    # )

    context = {
        'page_obj': equity_prices,
        'currency_code': currency_code,
        'security_label': security_label,
        'isin': isin,
        'date_from': date_from,
        'date_to': date_to,
        'src_system': src_system,
        'currencies': dropdown_options.get('currencies', []),
        'securities': dropdown_options.get('securities', []),
        'total_count': len(equity_prices_data),
    }

    return render(request, 'market_data/equity_price_list.html', context)


def equity_price_detail(request, currency_code: str, price_date: str):
    """
    Detail view for equity price with version history.
    Shows current price info and edit history (old values before changes).
    """
    from urllib.parse import unquote
    security_label = unquote(request.GET.get('security_label', ''))

    # Get current price by composite key
    try:
        current_price = equity_price_service.get_equity_price_by_key(currency_code, security_label, price_date)
        if not current_price:
            return HttpResponse("Equity price not found", status=404)
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)

    # Get price history for this security — anchor the 90-day window on the
    # record's own price_date so older records still show surrounding history.
    price_history = equity_price_service.get_price_history(security_label, days=90, reference_date=price_date)

    # Get version history (edit history from history table)
    version_history = equity_price_service.get_version_history(currency_code, security_label, price_date)

    # Get price trend
    trend = equity_price_service.get_price_trend(security_label, days=30)

    # Wrap current price
    wrapped_price = EquityPriceWrapper(current_price)

    # Wrap price history
    wrapped_history = [EquityPriceWrapper(p, idx) for idx, p in enumerate(price_history)]

    context = {
        'price': wrapped_price,
        'price_data': current_price,
        'price_history': wrapped_history,
        'version_history': version_history,
        'trend': trend,
        'currency_code': currency_code,
        'security_label': security_label,
        'price_date': price_date,
    }

    return render(request, 'market_data/equity_price_detail.html', context)


def equity_price_create(request):
    """
    Create new equity price.

    Uses composite key: (currency_code, security_label)
    """
    if request.method == 'POST':
        # Get form data - removed market and group_name
        equity_price_data = {
            'currency_code': request.POST.get('currency_code', '').strip(),
            'security_label': request.POST.get('security_label', '').strip(),
            'isin': request.POST.get('isin', '').strip(),
            'price_date': request.POST.get('price_date', '').strip(),
            'main_closing_price': request.POST.get('main_closing_price', '').strip(),
            'src_system': 'CIS',
        }

        # Get user info
        username = request.session.get('user_login', 'SYSTEM')

        try:
            # Create equity price
            success = equity_price_service.create_equity_price(
                equity_price_data,
                user=username
            )

            if success:
                # Log to audit
                user_id = str(request.session.get('user_id', ''))
                user_email = request.session.get('user_email', '')

                audit_log_kudu_repository.log_action(
                    user_id=user_id,
                    username=username,
                    user_email=user_email,
                    action_type='CREATE',
                    entity_type='EQUITY_PRICE',
                    entity_id=f"{equity_price_data['currency_code']}/{equity_price_data['security_label']}",
                    entity_name=equity_price_data['security_label'],
                    action_description=f"Created equity price for {equity_price_data['security_label']} on {equity_price_data['price_date']}",
                    status='SUCCESS',
                    request_method=request.method,
                    request_path=request.path,
                    ip_address=request.META.get('REMOTE_ADDR'),
                    user_agent=request.META.get('HTTP_USER_AGENT', '')
                )

                # Auto-refresh position market values
                try:
                    trade_kudu_repository.refresh_market_values()
                except Exception:
                    pass  # Non-blocking: don't fail price create if refresh fails

                # Redirect to list
                return redirect('market_data:equity_price_list')
            else:
                error_message = "Failed to create equity price"
        except ValidationError as e:
            error_message = str(e)

        # Re-render form with error
        dropdown_options = equity_price_dropdown_service.get_all_dropdown_options(username)

        context = {
            'error': error_message,
            'form_data': equity_price_data,
            'currencies': dropdown_options.get('currencies', []),
            'securities': dropdown_options.get('securities', []),
        }

        return render(request, 'market_data/equity_price_form.html', context)

    else:
        # GET - show form
        username = request.session.get('user_login', 'SYSTEM')
        dropdown_options = equity_price_dropdown_service.get_all_dropdown_options(username)

        # Default date to today
        from datetime import date
        today = date.today().strftime('%Y-%m-%d')

        context = {
            'currencies': dropdown_options.get('currencies', []),
            'securities': dropdown_options.get('securities', []),
            'default_date': today,
        }

        return render(request, 'market_data/equity_price_form.html', context)


def equity_price_edit(request, currency_code: str, price_date: str):
    """
    Edit existing equity price.

    Uses composite key: (currency_code, security_label, price_date)
    """
    from urllib.parse import unquote
    # POST hidden field has raw value; GET param is percent-encoded — unquote handles both
    security_label = unquote(
        request.POST.get('security_label', '') or request.GET.get('security_label', '')
    )

    # Get existing price by composite key (including price_date)
    try:
        existing_price = equity_price_service.get_equity_price_by_key(currency_code, security_label, price_date)
        if not existing_price:
            return HttpResponse("Equity price not found", status=404)
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)

    # GMP records are read-only — block edit at the view level regardless of
    # how the URL was reached (direct link bypass, etc.)
    if (existing_price.get('src_system') or '').upper() != 'CIS':
        messages.error(request, 'GMP source records are read-only and cannot be edited.')
        return redirect('market_data:equity_price_list')

    if request.method == 'POST':
        # Get form data
        equity_price_data = {
            'isin': request.POST.get('isin', '').strip(),
            'price_date': request.POST.get('price_date', '').strip(),
            'main_closing_price': request.POST.get('main_closing_price', '').strip(),
            'src_system': 'CIS',
        }

        # Get user info
        username = request.session.get('user_login', 'SYSTEM')
        lock_price = existing_price  # will be overwritten inside the lock block

        # Acquire a per-key process lock so two simultaneous POSTs for the
        # same equity price row are serialised within this Gunicorn worker.
        # The second request will then see the first request's updated
        # price_timestamp and updated_by, making the conflict check fire.
        _write_lock = equity_price_service.get_write_lock(currency_code, security_label, price_date)
        with _write_lock:
            # REFRESH the Impala metadata so we see the very latest Kudu commit,
            # then re-read the row fresh for the conflict check.
            equity_price_service.refresh_table()
            fresh_price = equity_price_service.get_equity_price_by_key(currency_code, security_label, price_date)
            lock_price = fresh_price or existing_price

            token = request.POST.get('price_timestamp_token', '').strip()
            current_ts = str(lock_price.get('price_timestamp') or '').strip()

            # Second guard: updated_by changes even if Kudu read lag hides the
            # new price_timestamp — a different editor's username will be present.
            token_updated_by = request.POST.get('updated_by_token', '').strip()
            current_updated_by = str(lock_price.get('updated_by') or lock_price.get('created_by') or '').strip()

            ts_conflict = token and current_ts and token != current_ts
            user_conflict = (
                token_updated_by
                and current_updated_by
                and token_updated_by != current_updated_by
                and current_updated_by != username
            )

            if ts_conflict or user_conflict:
                error_message = (
                    f"This price was updated by another user while you were editing "
                    f"(last saved by {lock_price.get('updated_by') or lock_price.get('created_by', 'unknown')} "
                    f"at {lock_price.get('price_datetime') or current_ts}). "
                    f"Please reload the page to get the latest values before saving."
                )
                dropdown_options = equity_price_dropdown_service.get_all_dropdown_options(username)
                context = {
                    'error': error_message,
                    'equity_price': lock_price,
                    'form_data': equity_price_data,
                    'currencies': dropdown_options.get('currencies', []),
                    'securities': dropdown_options.get('securities', []),
                    'is_edit': True,
                }
                return render(request, 'market_data/equity_price_form.html', context)

            # No conflict — perform the write while still holding the lock so a
            # concurrent request that passes the check above will see the new
            # price_timestamp when it re-reads after we release.
            try:
                success = equity_price_service.update_equity_price(
                    currency_code,
                    security_label,
                    equity_price_data,
                    user=username,
                    price_date=price_date
                )

                if success:
                    # Track changes for audit
                    old_values = {}
                    new_values = {}
                    changed_fields = []

                    trackable_fields = ['isin', 'price_date', 'main_closing_price', 'src_system']

                    for field in trackable_fields:
                        old_val = lock_price.get(field, '')
                        new_val = equity_price_data.get(field, '')
                        if str(old_val) != str(new_val):
                            old_values[field] = old_val
                            new_values[field] = new_val
                            changed_fields.append(field)

                    user_id = str(request.session.get('user_id', ''))
                    user_email = request.session.get('user_email', '')

                    audit_log_kudu_repository.log_action(
                        user_id=user_id,
                        username=username,
                        user_email=user_email,
                        action_type='UPDATE',
                        entity_type='EQUITY_PRICE',
                        entity_id=f"{currency_code}/{security_label}/{price_date}",
                        entity_name=security_label,
                        action_description=f"Updated equity price for {security_label} on {price_date} - Changed fields: {', '.join(changed_fields) if changed_fields else 'No changes'}",
                        old_value=json.dumps(old_values, default=str) if old_values else None,
                        new_value=json.dumps(new_values, default=str) if new_values else None,
                        field_name=', '.join(changed_fields) if changed_fields else None,
                        status='SUCCESS',
                        request_method=request.method,
                        request_path=request.path,
                        ip_address=request.META.get('REMOTE_ADDR'),
                        user_agent=request.META.get('HTTP_USER_AGENT', '')
                    )

                    try:
                        trade_kudu_repository.refresh_market_values()
                    except Exception:
                        pass

                    return redirect('market_data:equity_price_list')
                else:
                    error_message = "Failed to update equity price"
            except ValidationError as e:
                error_message = str(e)

        # Re-render form with error (outside the lock — safe to take time here)
        dropdown_options = equity_price_dropdown_service.get_all_dropdown_options(username)

        context = {
            'error': error_message,
            'equity_price': lock_price,
            'form_data': equity_price_data,
            'currencies': dropdown_options.get('currencies', []),
            'securities': dropdown_options.get('securities', []),
            'is_edit': True,
        }

        return render(request, 'market_data/equity_price_form.html', context)

    else:
        # GET - show form with existing data
        username = request.session.get('user_login', 'SYSTEM')
        dropdown_options = equity_price_dropdown_service.get_all_dropdown_options(username)

        context = {
            'equity_price': existing_price,
            'currencies': dropdown_options.get('currencies', []),
            'securities': dropdown_options.get('securities', []),
            'is_edit': True,
        }

        return render(request, 'market_data/equity_price_form.html', context)


# ============================================================================
# EQUITY PRICE UPLOAD VIEWS
# ============================================================================

def equity_price_upload(request):
    """Render the equity price CSV upload page."""
    return render(request, 'market_data/equity_price_upload.html')


@require_POST
def equity_price_validate_file(request):
    """
    API: Validate uploaded CSV and return preview data.
    POST with multipart file field 'csv_file'.
    Returns JSON: {valid_rows, invalid_rows, total, columns_found, error}
    """
    csv_file = request.FILES.get('csv_file')
    if not csv_file:
        return JsonResponse({'error': 'No file uploaded'}, status=400)

    if not csv_file.name.lower().endswith('.csv'):
        return JsonResponse({'error': 'Only CSV files are supported'}, status=400)

    if csv_file.size > 10 * 1024 * 1024:  # 10 MB guard
        return JsonResponse({'error': 'File too large (max 10 MB)'}, status=400)

    try:
        result = equity_price_service.build_upload_preview(csv_file)
        # Convert valid_rows to plain dicts for JSON serialisation
        return JsonResponse({
            'error':         result['error'],
            'total':         result['total'],
            'valid_count':   len(result['valid_rows']),
            'invalid_count': len(result['invalid_rows']),
            'columns_found': result['columns_found'],
            'valid_rows':    result['valid_rows'],
            'invalid_rows':  result['invalid_rows'],
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_POST
def equity_price_upload_chunk(request):
    """
    API: Upsert a chunk of pre-validated equity price rows.
    POST JSON body: { "rows": [ {security_label, currency_code, price_date,
                                  main_closing_price, isin}, ... ] }
    Returns JSON: {success_count, failure_count, failures}
    """
    try:
        body = json.loads(request.body)
        rows = body.get('rows', [])
        if not rows:
            return JsonResponse({'error': 'No rows provided'}, status=400)

        username = request.session.get('user_login', 'SYSTEM')
        result = equity_price_service.bulk_upload_equity_prices(rows, username=username)
        return JsonResponse(result)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON body'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


# ---------------------------------------------------------------------------
# Position Price Extraction views
# ---------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

def position_price_extraction(request):
    """Render the Subsi/Associate position price extraction page."""
    return render(request, 'market_data/position_price_extraction.html')


def position_price_extraction_data(request):
    """
    API: Return extraction candidates as JSON.
    GET ?position_date=YYYY-MM-DD  (optional; defaults to today)
    """
    from django.http import JsonResponse
    position_date = request.GET.get('position_date', '').strip() or None
    try:
        rows = position_price_extraction_service.get_candidates(position_date=position_date)
        return JsonResponse({'rows': rows, 'count': len(rows)})
    except Exception as e:
        _logger.error("position_price_extraction_data error: %s", e)
        return JsonResponse({'error': 'Failed to fetch extraction data.'}, status=500)


def position_price_extraction_download(request):
    """
    API: Stream extraction candidates as a CSV file download.
    GET ?position_date=YYYY-MM-DD  (optional; defaults to today)
    """
    from datetime import date as _date
    position_date = request.GET.get('position_date', '').strip() or None
    try:
        rows = position_price_extraction_service.get_candidates(position_date=position_date)
        csv_content = position_price_extraction_service.build_csv(rows)
        filename_date = position_date or _date.today().strftime('%Y-%m-%d')
        response = HttpResponse(csv_content, content_type='text/csv')
        response['Content-Disposition'] = (
            f'attachment; filename="equity_price_extraction_{filename_date}.csv"'
        )
        return response
    except Exception as e:
        _logger.error("position_price_extraction_download error: %s", e)
        return HttpResponse('Error generating CSV. Please try again.', status=500)


from django.views.decorators.http import require_POST as _require_POST


@_require_POST
def position_price_extraction_upload(request):
    """
    API: Upload extraction rows to equity price table with src_system = POSITION_UPLOAD.
    POST JSON body: { "rows": [ {security_label, currency_code, price_date,
                                  market_value, quantity, isin}, ... ] }
    Returns JSON: {success_count, failure_count, failures}
    """
    from django.http import JsonResponse
    try:
        body = json.loads(request.body)
        rows = body.get('rows', [])
        if not rows:
            return JsonResponse({'error': 'No rows provided'}, status=400)
        username = request.session.get('user_login', 'SYSTEM')
        result = position_price_extraction_service.upload_rows(rows, username=username)
        return JsonResponse(result)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON body'}, status=400)
    except Exception as e:
        _logger.error("position_price_extraction_upload error: %s", e)
        return JsonResponse({'error': 'Upload failed. Please try again.'}, status=500)
