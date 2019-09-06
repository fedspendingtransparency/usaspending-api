from django.test import TestCase
from rest_framework import status

from usaspending_api.awards.tests.data.award_test_data import create_award_test_data, AWARD_COUNT
from usaspending_api.awards.v2.views.accounts import SORTABLE_COLUMNS


FEDERAL_ACCOUNT_ENDPOINT = "/api/v2/awards/accounts/"


class AwardAccountsTestCase(TestCase):
    @classmethod
    def setUp(cls):
        create_award_test_data()

    def _test_post(self, request, expected_status_code=status.HTTP_200_OK):
        """
        Perform the actual request and interrogates the results.

        request is the Python dictionary that will be posted to the endpoint.
        expected_response_parameters are the values that you would normally
            pass into _generate_expected_response but we're going to do that
            for you so just pass the parameters as a tuple or list.
        expected_status_code is the HTTP status we expect to be returned from
            the call to the endpoint.
        """
        response = self.client.post(FEDERAL_ACCOUNT_ENDPOINT, request)
        assert response.status_code == expected_status_code

    def test_complete_queries(self):
        for _id in range(1, AWARD_COUNT + 1):
            self._test_post({"award_id": _id})

    def test_with_nonexistent_id(self):

        self._test_post({"award_id": 0})

        self._test_post({"award_id": "GENERATED_UNIQUE_AWARD_ID_000"})

    def test_with_bogus_id(self):

        self._test_post({"award_id": None})

    def test_sort_columns(self):

        for sortable_column in SORTABLE_COLUMNS:

            self._test_post({"award_id": 2, "order": "desc", "sort": sortable_column})

            self._test_post({"award_id": 2, "order": "asc", "sort": sortable_column})

        self._test_post({"award_id": 2, "sort": "BOGUS FIELD"}, expected_status_code=status.HTTP_400_BAD_REQUEST)
