import json

from django.test import TestCase
from rest_framework import status
from usaspending_api.awards.tests.data.award_test_data import (
    create_award_test_data,
    AWARD_COUNT,
    AGENCY_COUNT_BY_AWARD_ID,
    OBLIGATED_AMOUNT_BY_AWARD_ID,
)


AGGREGATE_ENDPOINT = "/api/v2/awards/funding_rollup/"


class AwardFundingRollupTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        create_award_test_data()

    @staticmethod
    def _generate_expected_response(award_id):
        """
        With hopes of making test data reusable a similar approach to the IDV test cases
        has been adopted. All test data is found in award_test_data.py along with some defined
        constants of what can be expected as results. Any change to one of these files will
        mean a change needs to be made to both.
        """

        results = {
            "total_transaction_obligated_amount": OBLIGATED_AMOUNT_BY_AWARD_ID[award_id],
            "awarding_agency_count": AGENCY_COUNT_BY_AWARD_ID[award_id]["awarding"],
            "funding_agency_count": AGENCY_COUNT_BY_AWARD_ID[award_id]["funding"],
            "federal_account_count": award_id,
        }

        return results

    def _test_post(self, request, expected_response_parameters_tuple=None, expected_status_code=status.HTTP_200_OK):
        """
        Perform the actual request and interrogates the results.

        request is the Python dictionary that will be posted to the endpoint.
        expected_response_parameters are the values that you would normally
            pass into _generate_expected_response but we're going to do that
            for you so just pass the parameters as a tuple or list.
        expected_status_code is the HTTP status we expect to be returned from
            the call to the endpoint.

        Returns... nothing useful.
        """
        response = self.client.post(AGGREGATE_ENDPOINT, request)
        assert response.status_code == expected_status_code
        if expected_response_parameters_tuple is not None:
            expected_response = self._generate_expected_response(*expected_response_parameters_tuple)
            assert json.loads(response.content.decode("utf-8")) == expected_response

    def test_complete_queries(self):
        for _id in range(1, AWARD_COUNT + 1):
            self._test_post({"award_id": _id}, (_id,))

    def test_with_nonexistent_id(self):

        self._test_post({"award_id": 0}, (0,))

        self._test_post({"award_id": "GENERATED_UNIQUE_AWARD_ID_000"}, (0,))

    def test_with_bogus_id(self):

        self._test_post({"award_id": None}, (0,))
