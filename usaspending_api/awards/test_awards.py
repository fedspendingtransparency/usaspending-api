from django.test import TestCase, Client
import pytest

from model_mommy import mommy


class AwardTests(TestCase):

    fixtures = ['awards']

    def setUp(self):
        self.award = mommy.make('awards.FinancialAccountsByAwardsTransactionObligations', _quantity=2)

    @pytest.mark.django_db
    def test_award_list(self):
        """
        Ensure the awards endpoint lists the right number of awards
        """
        resp = self.client.get('/api/v1/awards/summary')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.data), 2)
