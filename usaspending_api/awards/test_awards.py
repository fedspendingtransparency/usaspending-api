from django.test import TestCase, Client
import pytest
import json

from model_mommy import mommy


class AwardTests(TestCase):

    fixtures = ['awards']

    def setUp(self):
        self.awards = mommy.make('awards.FinancialAccountsByAwardsTransactionObligations', _quantity=2)

    @pytest.mark.django_db
    def test_award_list(self):
        """
        Ensure the awards endpoint lists the right number of awards
        """
        resp = self.client.get('/api/v1/awards/')
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(len(resp.data) >= 2)

        self.assertEqual(self.client.get('/api/v1/awards/fain/ABCD').status_code, 200)
        self.assertEqual(self.client.get('/api/v1/awards/uri/ABCD').status_code, 200)
        self.assertEqual(self.client.get('/api/v1/awards/piid/ABCD').status_code, 200)

    @pytest.mark.django_db
    def test_award_list_summary(self):
        """
        Ensure the awards endpoint summary lists the right number of awards
        """
        resp = self.client.get('/api/v1/awards/summary/')
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(len(resp.data) > 2)

        self.assertEqual(self.client.get('/api/v1/awards/summary/fain/ABCD').status_code, 200)
        self.assertEqual(self.client.get('/api/v1/awards/summary/uri/ABCD').status_code, 200)
        self.assertEqual(self.client.get('/api/v1/awards/summary/piid/ABCD').status_code, 200)
        self.assertEqual(self.client.get('/api/v1/awards/summary/?fy=2016&funding_fpds=3100').status_code, 200)

        self.assertEqual(self.client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10})).status_code, 200)
        self.assertEqual(self.client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10, "filters": [{"field": "funding_agency__fpds_code", "operation": "equals", "value": "0300"}]})).status_code, 200)
        self.assertEqual(self.client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10, "filters": [{"combine_method": "OR", "filters": [{"field": "funding_agency__fpds_code", "operation": "equals", "value": "0300"}, {"field": "awarding_agency__fpds_code", "operation": "equals", "value": "0300"}]}]})).status_code, 200)
