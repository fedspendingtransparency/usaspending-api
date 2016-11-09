from django.test import TestCase, Client
import pytest

from model_mommy import mommy


class FinancialActivitiesTests(TestCase):
    def setUp(self):
        self.award = mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', _quantity=2)

    @pytest.mark.django_db
    def test_financial_activities_list(self):
        """
        Ensure the financial activities endpoint lists the right number of entities
        """
        resp = self.client.get('/api/v1/financial_activities/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.data), 3)
