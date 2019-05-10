import json

from django.test import TestCase
from rest_framework import status
from usaspending_api.awards.models import Award
from usaspending_api.awards.tests.data.idv_funding_data import create_funding_data_tree, PARENTS, IDVS, AWARD_COUNT


AGGREGATE_ENDPOINT = '/api/v2/awards/idvs/funding_rollup/'


class IDVFundingRollupTestCase(TestCase):

    @classmethod
    def setUp(cls):
        create_funding_data_tree()

    @staticmethod
    def _generate_expected_response(award_id, *args):
        """
        Rather than manually generate an insane number of potential responses
        to test the various parameter combinations, we're going to procedurally
        generate them.  award_ids is the list of ids we expect back from the
        request in the order we expect them.  Unfortunately, for this to work,
        test data had to be generated in a specific way.  If you change how
        test data is generated you will probably also have to change this.  For
        example, IDVs have obligated amounts in the thousands whereas contracts
        have obligated amounts in the single digits and teens.
        """
        children = [k for k in PARENTS if PARENTS[k] == award_id and award_id in IDVS]
        grandchildren = [k for k in PARENTS if PARENTS[k] in children and PARENTS[k] in IDVS]
        non_idv_children = [k for k in children if k not in IDVS]
        non_idv_grandchildren = [k for k in grandchildren if k not in IDVS]
        _id = sum(non_idv_children) + sum(non_idv_grandchildren)
        results = {
            'total_transaction_obligated_amount': _id * 10000 + _id + _id / 100,
            'awarding_agency_count': len(non_idv_children) + len(non_idv_grandchildren),
            'funding_agency_count': len(non_idv_children) + len(non_idv_grandchildren),
            'federal_account_count': len(non_idv_children) + len(non_idv_grandchildren)
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
            assert json.loads(response.content.decode('utf-8')) == expected_response

    def test_complete_queries(self):
        for _id in range(1, AWARD_COUNT + 1):
            self._test_post(
                {'award_id': _id},
                (_id,)
            )

    def test_with_nonexistent_id(self):

        self._test_post(
            {'award_id': 0},
            (0,)
        )

        self._test_post(
            {'award_id': 'GENERATED_UNIQUE_AWARD_ID_000'},
            (0,)
        )

    def test_with_bogus_id(self):

        self._test_post(
            {'award_id': None},
            (0,)
        )

    def test_null_agencies_accounts(self):
        """
        We are going to null out some accounts/agencies to ensure our count is
        correct.  According the LOVELY drawing in idv_funding_data.py, C14 will
        be a great candidate for this exercise.  It's ultimate parent is I2.
        """
        # Grab the counts for I2.
        response = self.client.post(AGGREGATE_ENDPOINT, {'award_id': 2})
        awarding_agency_count = response.data['awarding_agency_count']
        funding_agency_count = response.data['funding_agency_count']

        # Grab the treasury appropriation account for C14 and null out its agency values.
        Award.objects.filter(pk=14).update(
            awarding_agency_id=None,
            funding_agency_id=None)

        # Now re-grab the rollup values and ensure they are decremented accordingly.
        response = self.client.post(AGGREGATE_ENDPOINT, {'award_id': 2})
        assert awarding_agency_count == response.data['awarding_agency_count'] + 1
        assert funding_agency_count == response.data['funding_agency_count'] + 1
