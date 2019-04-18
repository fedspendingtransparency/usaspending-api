import json

from django.test import TestCase
from rest_framework import status
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.v2.views.idvs.funding import SORTABLE_COLUMNS
from usaspending_api.awards.tests.data.idv_funding_data import create_funding_data_tree, PARENTS, IDVS, AWARD_COUNT


DETAIL_ENDPOINT = '/api/v2/awards/idvs/funding/'

AGGREGATE_ENDPOINT = '/api/v2/awards/idvs/funding_rollup/'

FEDERAL_ACCOUNT_ENDPOINT = '/api/v2/awards/idvs/accounts/'


class IDVFundingTestCase(TestCase):

    @classmethod
    def setUp(cls):
        create_funding_data_tree()

    @staticmethod
    def _generate_expected_response(previous, next, page, has_previous, has_next, *award_ids):
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
        results = []
        for _id in award_ids:
            _sid = str(_id).zfill(3)
            results.append({
                'award_id': _id,
                'generated_unique_award_id': 'GENERATED_UNIQUE_AWARD_ID_%s' % _sid,
                'reporting_fiscal_year': 2000 + _id,
                'reporting_fiscal_quarter': _id % 4 + 1,
                'piid': 'piid_%s' % _sid,
                'funding_agency_id': 9000 + _id,
                'reporting_agency_id': _sid.zfill(3),
                'reporting_agency_name': 'reporting agency name %s' % _sid,
                'agency_id': _sid.zfill(3),
                'main_account_code': _sid.zfill(4),
                'account_title': 'account title %s' % _sid,
                'program_activity_code': _sid,
                'program_activity_name': 'program activity %s' % _sid,
                'object_class': '1' + _sid,
                'object_class_name': 'object class %s' % _sid,
                'transaction_obligated_amount': _id * 10000 + _id + _id / 100
            })

        page_metadata = {
            'previous': previous,
            'next': next,
            'page': page,
            'hasPrevious': has_previous,
            'hasNext': has_next
        }

        return {'results': results, 'page_metadata': page_metadata}

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
        response = self.client.post(DETAIL_ENDPOINT, request)
        assert response.status_code == expected_status_code
        if expected_response_parameters_tuple is not None:
            expected_response = self._generate_expected_response(*expected_response_parameters_tuple)
            assert json.loads(response.content.decode('utf-8')) == expected_response

    def test_defaults(self):

        self._test_post(
            {'award_id': 1},
            (None, None, 1, False, False, 6)
        )

        self._test_post(
            {'award_id': 'GENERATED_UNIQUE_AWARD_ID_001'},
            (None, None, 1, False, False, 6)
        )

        self._test_post(
            {'award_id': 2},
            (None, None, 1, False, False, 14, 13, 12, 11, 10, 9)
        )

    def test_with_nonexistent_id(self):

        self._test_post(
            {'award_id': 0},
            (None, None, 1, False, False)
        )

        self._test_post(
            {'award_id': 'GENERATED_UNIQUE_AWARD_ID_000'},
            (None, None, 1, False, False)
        )

    def test_with_bogus_id(self):

        self._test_post(
            {'award_id': None},
            (None, None, 1, False, False)
        )

    def test_piid_filter(self):

        self._test_post(
            {'award_id': 2, 'piid': 'piid_013'},
            (None, None, 1, False, False, 13)
        )

        self._test_post(
            {'award_id': 1, 'piid': 'nonexistent_piid'},
            (None, None, 1, False, False)
        )

        self._test_post(
            {'award_id': 1, 'piid': 12345},
            (None, None, 1, False, False)
        )

    def test_limit_values(self):

        self._test_post(
            {'award_id': 2, 'limit': 1},
            (None, 2, 1, False, True, 14)
        )

        self._test_post(
            {'award_id': 2, 'limit': 6},
            (None, None, 1, False, False, 14, 13, 12, 11, 10, 9)
        )

        self._test_post(
            {'award_id': 2, 'limit': 0},
            expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {'award_id': 2, 'limit': 2000000000},
            expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {'award_id': 2, 'limit': {'BOGUS': 'LIMIT'}},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_page_values(self):

        self._test_post(
            {'award_id': 2, 'limit': 1, 'page': 2},
            (1, 3, 2, True, True, 13)
        )

        self._test_post(
            {'award_id': 2, 'limit': 1, 'page': 6},
            (5, None, 6, True, False, 9)
        )

        # This should probably not be right, but it is the expected result.
        self._test_post(
            {'award_id': 2, 'limit': 1, 'page': 99},
            (98, None, 99, True, False)
        )

        self._test_post(
            {'award_id': 2, 'limit': 1, 'page': 0},
            expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {'award_id': 2, 'limit': 1, 'page': 'BOGUS PAGE'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_sort_columns(self):

        for sortable_column in SORTABLE_COLUMNS:

            self._test_post(
                {'award_id': 2, 'order': 'desc', 'sort': sortable_column},
                (None, None, 1, False, False, 14, 13, 12, 11, 10, 9)
            )

            self._test_post(
                {'award_id': 2, 'order': 'asc', 'sort': sortable_column},
                (None, None, 1, False, False, 9, 10, 11, 12, 13, 14)
            )

        self._test_post(
            {'award_id': 2, 'sort': 'BOGUS FIELD'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_sort_order_values(self):

        self._test_post(
            {'award_id': 2, 'order': 'desc'},
            (None, None, 1, False, False, 14, 13, 12, 11, 10, 9)
        )

        self._test_post(
            {'award_id': 2, 'order': 'asc'},
            (None, None, 1, False, False, 9, 10, 11, 12, 13, 14)
        )

        self._test_post(
            {'award_id': 2, 'order': 'BOGUS ORDER'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_complete_queries(self):

        self._test_post(
            {'award_id': 2, 'piid': 'piid_013', 'limit': 3, 'page': 1, 'sort': 'piid', 'order': 'asc'},
            (None, None, 1, False, False, 13)
            )

    def test_dev_2307(self):

        # Make one of the transaction_obligated_amount values NaN.  Going from
        # the drawing above, if we update contract 12, we should see this
        # record for IDV 7.
        FinancialAccountsByAwards.objects.filter(pk=12).update(transaction_obligated_amount='NaN')

        # Retrieve the NaN value.
        response = self.client.post(
            DETAIL_ENDPOINT,
            {'award_id': 7, 'sort': 'transaction_obligated_amount', 'order': 'desc'}
        )
        assert response.status_code == 200
        result = json.loads(response.content.decode('utf-8'))
        assert len(result['results']) == 2
        for r in result['results']:
            assert r['transaction_obligated_amount'] in (None, 110011.11)


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
        odd_agency_in_children = True if sum([1 for k in non_idv_children if k % 2 != 0]) > 0 else False
        odd_agency_in_grandchildren = True if sum([1 for k in non_idv_grandchildren if k % 2 != 0]) > 0 else False
        odd_agency_count = 1 if odd_agency_in_grandchildren or odd_agency_in_children else 0
        even_agency_child_count = len([k for k in non_idv_children if k % 2 == 0])
        even_agency_grandchildren_count = len([k for k in non_idv_grandchildren if k % 2 == 0])
        _id = sum(non_idv_children) + sum(non_idv_grandchildren)
        results = {
            'total_transaction_obligated_amount': _id * 10000 + _id + _id / 100,
            'awarding_agency_count': even_agency_grandchildren_count + even_agency_child_count + odd_agency_count,
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


class IDVFundingTreemapTestCase(TestCase):

    @classmethod
    def setUp(cls):
        create_funding_data_tree()

    def _test_post(self, request, expected_response_parameters_tuple=None, expected_status_code=status.HTTP_200_OK):
        """
        Perform the actual request and interrogates the results.

        request is the Python dictionary that will be posted to the endpoint.
        expected_response_parameters are the values that you would normally
            pass into _generate_expected_response but we're going to do that
            for you so just pass the parameters as a tuple or list.
        expected_status_code is the HTTP status we expect to be returned from
            the call to the endpoint.

        Because this endpoint uses the same shared SQL as the roll-up endpoint and simply adds a group-by clause,
        it is not necessary to test the aggregation again, as any adjustment to the aggregation SQL that is incorrect
        would cause the Rollup Test cases to fail first anyway.

        Returns... nothing useful.
        """
        response = self.client.post(FEDERAL_ACCOUNT_ENDPOINT, request)
        assert response.status_code == expected_status_code

    def test_complete_queries(self):
        for _id in range(1, AWARD_COUNT + 1):
            self._test_post(
                {'award_id': _id}
            )

    def test_with_nonexistent_id(self):

        self._test_post(
            {'award_id': 0},
        )

        self._test_post(
            {'award_id': 'GENERATED_UNIQUE_AWARD_ID_000'},
        )

    def test_with_bogus_id(self):

        self._test_post(
            {'award_id': None},
        )
