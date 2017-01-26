from django.test import TestCase
import pytest
import json


class AwardTests(TestCase):

    @pytest.mark.django_db
    def test_award_endpoint(self):
        """
        Test the awards endpoint
        """
        resp = self.client.get('/api/v1/awards/')
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(len(resp.data) > 2)

        self.assertEqual(self.client.get('/api/v1/awards/').status_code, 200)

        self.assertEqual(self.client.post(
            '/api/v1/awards/', content_type='application/json',
            data=json.dumps({"page": 1, "limit": 10})).status_code, 200)

        self.assertEqual(self.client.post(
            '/api/v1/awards/', content_type='application/json',
            data=json.dumps({
                "page": 1, "limit": 10,
                "filters": [{"field": "funding_agency__toptier_agency__fpds_code", "operation": "equals", "value": "0300"}]
            })).status_code, 200)

        self.assertEqual(self.client.post(
            '/api/v1/awards/', content_type='application/json',
            data=json.dumps({
                "page": 1, "limit": 10,
                "filters": [{"combine_method": "OR", "filters": [{"field": "funding_agency__toptier_agency__fpds_code", "operation": "equals", "value": "0300"}, {"field": "awarding_agency__toptier_agency__fpds_code", "operation": "equals", "value": "0300"}]}]
            })).status_code, 200)

        self.assertEqual(self.client.post(
            '/api/v1/awards/', content_type='application/json',
            data=json.dumps({
                "page": 1, "limit": 10,
                "filters": [{"field": "funding_agency__toptier_agency__fpds_code", "operation": "ff", "value": "0300"}]
            })).status_code, 400)
