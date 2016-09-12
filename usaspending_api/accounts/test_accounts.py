from django.test import TestCase, Client
import pytest

from model_mommy import mommy

class AccountsTest(TestCase):
    def setUp(self):
        self.award = mommy.make('accounts.AppropriationAccountBalances', _quantity=2)

    @pytest.mark.django_db
    def test_account_list(self):
        """
        Ensure the accounts endpoint lists the right number of entities
        """
        resp = self.client.get('/api/v1/accounts/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.data), 2)
