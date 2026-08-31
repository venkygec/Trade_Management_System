"""
Market Data URL Configuration
"""

from django.urls import path
from . import views

app_name = 'market_data'

urlpatterns = [
    # Market Data Dashboard (combined)
    path('dashboard/', views.market_data_dashboard, name='market_data_dashboard'),

    # FX Rates
    path('fx-rates/', views.fx_rate_list, name='fx_rate_list'),
    path('fx-rates/dashboard/', views.fx_rate_dashboard, name='fx_dashboard'),  # Backward compatibility
    path('fx-rates/<str:currency_pair>/', views.fx_rate_detail, name='fx_rate_detail'),

    # Equity Prices
    path('equity-prices/', views.equity_price_list, name='equity_price_list'),
    path('equity-prices/create/', views.equity_price_create, name='equity_price_create'),
    path('equity-prices/upload/', views.equity_price_upload, name='equity_price_upload'),
    path('equity-prices/upload/validate/', views.equity_price_validate_file, name='equity_price_validate_file'),
    path('equity-prices/upload/submit/', views.equity_price_upload_chunk, name='equity_price_upload_chunk'),

    # Position Price Extraction (Subsi / Associate investments)
    path('equity-prices/position-extraction/', views.position_price_extraction, name='position_price_extraction'),
    path('equity-prices/position-extraction/data/', views.position_price_extraction_data, name='position_price_extraction_data'),
    path('equity-prices/position-extraction/download/', views.position_price_extraction_download, name='position_price_extraction_download'),
    path('equity-prices/position-extraction/upload/', views.position_price_extraction_upload, name='position_price_extraction_upload'),

    path('equity-prices/<str:currency_code>/<str:price_date>/detail/', views.equity_price_detail, name='equity_price_detail'),
    path('equity-prices/<str:currency_code>/<str:price_date>/edit/', views.equity_price_edit, name='equity_price_edit'),
]
