import csv
import re
from io import StringIO
from unittest.mock import patch

from django.test import Client, TestCase
from django.urls import reverse


class PositionListViewTestCase(TestCase):
    def setUp(self):
        self.client = Client()
        session = self.client.session
        session['user_login'] = 'testuser'
        session['user_id'] = 1
        session['user_email'] = 'test@example.com'
        session.save()

        self.position = {
            'account_group': 'AG-1',
            'portfolio_entity_group': 'UOBS',
            'portfolio': 'PORT-1',
            'portfolio_currency': 'USD',
            'portfolio_investment_type': 'Equity',
            'revaluation_status': 'REVAL',
            'security_label': 'AAPL UQ',
            'security_currency': 'USD',
            'security_quoted_unquoted': 'QUOTED',
            'security_id': 101,
            'isin': 'US0378331005',
            'position_basis': 'TRADED',
            'position_date': '2026-08-30',
            'src_system': 'CIS',
            'position_type': 'INT',
            'quantity': 100,
            'average_cost_fc': 10.12345678,
            'average_cost_lc': 10.12345678,
            'cost_fc': 1000,
            'cost_lc': 1000,
            'market_value_fc': 1200,
            'market_value_lc': 1200,
            'net_book_value_fc': 1100,
            'unrealized_pnl_fc': 200,
            'unrealized_pnl_lc': 200,
            'realized_pnl_fc': 50,
            'provision_fc': 0,
            'provision_lc': 0,
            'dividend_fc': 0,
            'uncall_fc': 0,
            'uncall_lc': 0,
            'pipeline_fc': 0,
            'processing_date': '2026-08-30',
            'processing_timestamp': '2026-08-30 10:00:00',
        }

    @patch('trade.views_position.position_repository')
    def test_position_list_renders_new_columns_in_requested_order(self, mock_repo):
        mock_repo.get_position_count.return_value = 1
        mock_repo.get_positions.return_value = [self.position]
        mock_repo.get_summary_stats.return_value = {}
        mock_repo.get_distinct_src_systems.return_value = ['CIS']
        mock_repo.get_distinct_portfolios.return_value = ['PORT-1']

        response = self.client.get(
            reverse('trade:position_list'),
            {'date_from': '2026-08-30', 'date_to': '2026-08-30'},
        )

        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        table_headers = re.findall(r'onclick="sortBy\([^)]+\)">\s*([^<]+?)\s*<span class="sort-icon"></span>', content)
        self.assertGreaterEqual(len(table_headers), 10)
        self.assertEqual(
            table_headers[:10],
            [
                'Account Group',
                'Portfolio Entity Group',
                'Portfolio',
                'Portfolio Currency',
                'Portfolio Investment Type',
                'Reval',
                'Security',
                'Security Currency',
                'Security Quoted/Unquoted',
                'ISIN',
            ],
        )

        self.assertContains(response, 'AG-1')
        self.assertContains(response, 'UOBS')
        self.assertContains(response, 'PORT-1')
        self.assertContains(response, 'Equity')
        self.assertContains(response, 'AAPL UQ')
        self.assertContains(response, 'QUOTED')

    @patch('trade.views_position.position_repository')
    def test_position_list_csv_export_includes_new_columns(self, mock_repo):
        mock_repo.get_positions.return_value = [self.position]

        response = self.client.get(
            reverse('trade:position_list'),
            {'export': 'csv', 'date_from': '2026-08-30', 'date_to': '2026-08-30'},
        )

        self.assertEqual(response.status_code, 200)

        rows = list(csv.reader(StringIO(response.content.decode())))
        self.assertEqual(
            rows[0][:9],
            [
                'Account Group',
                'Portfolio Entity Group',
                'Portfolio',
                'Portfolio Currency',
                'Portfolio Investment Type',
                'Security',
                'Security Currency',
                'Security Quoted/Unquoted',
                'ISIN',
            ],
        )
        self.assertEqual(
            rows[1][:9],
            ['AG-1', 'UOBS', 'PORT-1', 'USD', 'Equity', 'AAPL UQ', 'USD', 'QUOTED', 'US0378331005'],
        )
