"""
Position Views

Read-only view of cis_position table with filtering and CSV export.
Supports filters: portfolio, security, src_system, position_basis,
position_type, position_date range.
"""

import csv
import logging
from datetime import datetime, timedelta

from django.shortcuts import render
from django.http import HttpResponse

from core.views.auth_views import require_login
from core.services.system_date_service import system_date_service
from trade.repositories.position_repository import position_repository

logger = logging.getLogger(__name__)

PAGE_SIZE = 100

POSITION_BASIS_CHOICES = ['TRADED', 'SETTLED']
POSITION_TYPE_CHOICES = ['INT', 'EOD', 'SOD', 'CORR']


@require_login
def position_list(request):
    """
    Read-only position list from cis_position.
    Filters: portfolio, security, src_system, position_basis,
             position_type (multi), date_from, date_to.
    Sorting: sort_by + sort_dir params, preserved across pagination.
    Supports CSV export via ?export=csv.
    """
    # --- Filter params ---
    selected_portfolios   = request.GET.getlist('portfolios')
    selected_securities   = request.GET.getlist('securities')
    selected_src_systems  = request.GET.getlist('src_system')
    selected_pos_types    = request.GET.getlist('position_type')   # multi-select
    pos_type_cleared      = request.GET.get('position_type_cleared', '0').strip()
    # Default to INT on first load; honour explicit "Clear all" (sentinel=1) to show all types
    if not selected_pos_types and pos_type_cleared != '1':
        selected_pos_types = ['INT']
    position_basis        = request.GET.get('position_basis', '').strip()
    date_from             = request.GET.get('date_from', '').strip()
    date_to               = request.GET.get('date_to', '').strip()
    export                = request.GET.get('export', '').strip()

    # --- Sort params ---
    sort_by  = request.GET.get('sort_by', '').strip()
    sort_dir = request.GET.get('sort_dir', 'DESC').strip().upper()
    if sort_dir not in ('ASC', 'DESC'):
        sort_dir = 'DESC'

    # Default date range: report_date (T-1) to settlement T+2.
    if not date_from and not date_to:
        sd_info   = system_date_service.get_system_date_info()
        date_from = sd_info.report_date_display or sd_info.system_date_display
        date_to   = sd_info.settlement_t2_display or (
            sd_info.system_date + timedelta(days=2)
        ).strftime('%Y-%m-%d')

    filter_kwargs = dict(
        portfolios=selected_portfolios or None,
        securities=selected_securities or None,
        src_system=selected_src_systems or None,
        position_basis=position_basis or None,
        position_type=selected_pos_types or None,
        date_from=date_from or None,
        date_to=date_to or None,
    )

    # --- CSV Export ---
    if export == 'csv':
        positions = position_repository.get_positions(
            **filter_kwargs, sort_by=sort_by, sort_dir=sort_dir,
            limit=50000, offset=0
        )
        return _export_csv(positions)

    # --- Paginated fetch ---
    try:
        page = max(1, int(request.GET.get('page', 1)))
    except (ValueError, TypeError):
        page = 1

    offset      = (page - 1) * PAGE_SIZE
    total       = position_repository.get_position_count(**filter_kwargs)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    positions   = position_repository.get_positions(
        **filter_kwargs, sort_by=sort_by, sort_dir=sort_dir,
        limit=PAGE_SIZE, offset=offset
    )
    stats = position_repository.get_summary_stats(**filter_kwargs)

    # --- Dropdown data ---
    src_systems = position_repository.get_distinct_src_systems()
    portfolios  = position_repository.get_distinct_portfolios()

    # base_qs: all params except page — used by pagination and sort links.
    _qs = request.GET.copy()
    _qs.pop('page', None)
    base_qs = _qs.urlencode()

    context = {
        # Data
        'positions':    positions,
        'stats':        stats,
        'total':        total,
        'page':         page,
        'page_prev':    page - 1,
        'page_next':    page + 1,
        'total_pages':  total_pages,
        'page_range':   _page_range(page, total_pages),
        'base_qs':      base_qs,

        # Active filters
        'selected_portfolios':   selected_portfolios,
        'selected_securities':   selected_securities,
        'selected_src_systems':  selected_src_systems,
        'selected_pos_types':    selected_pos_types,
        'f_position_basis':      position_basis,
        'f_date_from':           date_from,
        'f_date_to':             date_to,

        # Sort state
        'sort_by':  sort_by,
        'sort_dir': sort_dir,

        # Dropdown options
        'src_systems':            src_systems,
        'portfolios':             portfolios,
        'position_basis_choices': POSITION_BASIS_CHOICES,
        'position_type_choices':  POSITION_TYPE_CHOICES,
    }

    return render(request, 'trade/position_list.html', context)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _export_csv(positions):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = (
        f'attachment; filename="positions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
    )

    def fmt_num(val, dp=2):
        if val is None or val == '':
            return ''
        try:
            return f'{float(val):,.{dp}f}'
        except (ValueError, TypeError):
            return val

    writer = csv.writer(response)
    writer.writerow([
        'Account Group', 'Entity Group', 'Portfolio', 'Portfolio Currency',
        'Investment Type', 'Security', 'Security Currency', 'Quoted/Unquotwd',
        'ISIN', 'Position Basis', 'Position Date',
        'Src System', 'Position Type', 'Quantity',
        'Avg Cost FC', 'Avg Cost LC', 'Cost FC', 'Cost LC',
        'Market Value FC', 'Market Value LC',
        'Net Book Value FC', 'Net Book Value LC',
        'Unrealized PnL FC', 'Unrealized PnL LC',
        'Realized PnL FC', 'Realized PnL LC',
        'Provision FC', 'Provision LC',
        'Dividend FC', 'Dividend LC',
        'Uncall FC', 'Uncall LC',
        'Pipeline FC', 'Pipeline LC',
        'Reval Status',
        'Processing Date',
        'Processing Timestamp',
    ])

    for p in positions:
        writer.writerow([
            p.get('account_group'),
            p.get('portfolio_entity_group'),
            p.get('portfolio'),
            p.get('portfolio_currency'),
            p.get('portfolio_investment_type'),
            p.get('security_label'),
            p.get('security_currency'),
            p.get('security_quoted_unquoted'),
            p.get('isin'),
            p.get('position_basis'),
            p.get('position_date'),
            p.get('src_system'),
            p.get('position_type'),
            fmt_num(p.get('quantity'), dp=4),
            fmt_num(p.get('average_cost_fc'), dp=6),
            fmt_num(p.get('average_cost_lc'), dp=6),
            fmt_num(p.get('cost_fc')),
            fmt_num(p.get('cost_lc')),
            fmt_num(p.get('market_value_fc')),
            fmt_num(p.get('market_value_lc')),
            fmt_num(p.get('net_book_value_fc')),
            fmt_num(p.get('net_book_value_lc')),
            fmt_num(p.get('unrealized_pnl_fc')),
            fmt_num(p.get('unrealized_pnl_lc')),
            fmt_num(p.get('realized_pnl_fc')),
            fmt_num(p.get('realized_pnl_lc')),
            fmt_num(p.get('provision_fc')),
            fmt_num(p.get('provision_lc')),
            fmt_num(p.get('dividend_fc')),
            fmt_num(p.get('dividend_lc')),
            fmt_num(p.get('uncall_fc')),
            fmt_num(p.get('uncall_lc')),
            fmt_num(p.get('pipeline_fc')),
            fmt_num(p.get('pipeline_lc')),
            p.get('revaluation_status', ''),
            p.get('processing_date'),
            p.get('processing_timestamp'),
        ])

    return response


def _page_range(current, total):
    """Return a limited window of page numbers around current page."""
    start = max(1, current - 3)
    end   = min(total, current + 3)
    return range(start, end + 1)
