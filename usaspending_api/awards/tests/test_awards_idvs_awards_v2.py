import json

from django.test import TestCase
from model_mommy import mommy
from rest_framework import status
from usaspending_api.awards.v2.views.idvs.awards import SORTABLE_COLUMNS


ENDPOINT = '/api/v2/awards/idvs/awards/'

AWARD_COUNT = 15
IDVS = (1, 2, 3, 4, 5, 7, 8)
PARENTS = {3: 1, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 2, 11: 7, 12: 7, 13: 8, 14: 8, 15: 9}


class IDVAwardsTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        """
        You'll have to use your imagination a bit with my budget tree drawings.
        These are the two hierarchies being built by this function.  "I" means
        IDV.  "C" means contract.  The number is the award id.  So in this
        drawing, I1 is the parent of I3, I4, I5, and C6.  I2 is the grandparent
        of C11, C12, C13, C14, and C15.  Please note that the C9 -> C15
        relationship is actually invalid in the IDV world.  I've added it for
        testing purposes, however.  Anyhow, hope this helps.  There's a reason
        I'm in software and not showing my wares at an art gallery somewhere.

                  I1                                        I2
          I3   I4   I5   C6                  I7        I8        C9        C10
                                          C11 C12   C13 C14      C15
        """

        # We'll need some "latest transactions".
        for transaction_id in range(1, AWARD_COUNT + 1):
            mommy.make(
                'awards.TransactionNormalized',
                id=transaction_id
            )
            mommy.make(
                'awards.TransactionFPDS',
                transaction_id=transaction_id,
                funding_agency_name='subtier_funding_agency_name_%s' % transaction_id,
                ordering_period_end_date='2018-01-%02d' % transaction_id
            )

        # We'll need some awards (and agencies).
        for award_id in range(1, AWARD_COUNT + 1):
            parent_n = PARENTS.get(award_id)
            mommy.make(
                'references.Agency',
                id=award_id * 12,
                toptier_agency_id=award_id * 12
            )
            mommy.make(
                'references.ToptierAgency',
                toptier_agency_id=award_id * 12,
                name='toptier_funding_agency_name_%s' % (award_id * 12),
            )
            mommy.make(
                'awards.Award',
                id=award_id,
                generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % award_id,
                type=('IDV_%s' if award_id in IDVS else 'CONTRACT_%s') % award_id,
                total_obligation=award_id,
                piid='piid_%s' % award_id,
                fpds_agency_id='fpds_agency_id_%s' % award_id,
                parent_award_piid='piid_%s' % parent_n if parent_n else None,
                fpds_parent_agency_id='fpds_agency_id_%s' % parent_n if parent_n else None,
                latest_transaction_id=award_id,
                type_description='type_description_%s' % award_id,
                description='description_%s' % award_id,
                period_of_performance_current_end_date='2018-03-%02d' % award_id,
                period_of_performance_start_date='2018-02-%02d' % award_id,
                funding_agency_id=award_id * 12,
            )

        # We'll need some parent_awards.
        for award_id in IDVS:
            mommy.make(
                'awards.ParentAward',
                award_id=award_id,
                generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % award_id,
                rollup_total_obligation=award_id * 1000,
                parent_award_id=PARENTS.get(award_id)
            )

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
        for award_id in award_ids:
            results.append({
                'award_id': award_id,
                'award_type': 'type_description_%s' % award_id,
                'description': 'description_%s' % award_id,
                'funding_agency': 'toptier_funding_agency_name_%s' % (award_id * 12),
                'funding_agency_id': award_id * 12,
                'generated_unique_award_id': 'GENERATED_UNIQUE_AWARD_ID_%s' % award_id,
                'last_date_to_order': '2018-01-%02d' % award_id,
                'obligated_amount': float(award_id * (1000 if award_id in IDVS else 1)),
                'period_of_performance_current_end_date': '2018-03-%02d' % award_id,
                'period_of_performance_start_date': '2018-02-%02d' % award_id,
                'piid': 'piid_%s' % award_id
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
        response = self.client.post(ENDPOINT, request)
        assert response.status_code == expected_status_code
        if expected_response_parameters_tuple is not None:
            expected_response = self._generate_expected_response(*expected_response_parameters_tuple)
            assert json.loads(response.content.decode('utf-8')) == expected_response

    def test_defaults(self):

        self._test_post(
            {'award_id': 1},
            (None, None, 1, False, False, 5, 4, 3)
        )

        self._test_post(
            {'award_id': 'GENERATED_UNIQUE_AWARD_ID_1'},
            (None, None, 1, False, False, 5, 4, 3)
        )

    def test_with_nonexistent_id(self):

        self._test_post(
            {'award_id': 0},
            (None, None, 1, False, False)
        )

        self._test_post(
            {'award_id': 'GENERATED_UNIQUE_AWARD_ID_0'},
            (None, None, 1, False, False)
        )

    def test_with_bogus_id(self):

        self._test_post(
            {'award_id': None},
            (None, None, 1, False, False)
        )

    def test_type(self):

        self._test_post(
            {'award_id': 1, 'type': 'child_idvs'},
            (None, None, 1, False, False, 5, 4, 3)
        )

        self._test_post(
            {'award_id': 1, 'type': 'child_awards'},
            (None, None, 1, False, False, 6)
        )

        self._test_post(
            {'award_id': 1, 'type': 'grandchild_awards'},
            (None, None, 1, False, False)
        )

        self._test_post(
            {'award_id': 2, 'type': 'grandchild_awards'},
            (None, None, 1, False, False, 14, 13, 12, 11)
        )

        self._test_post(
            {'award_id': 1, 'type': 'BOGUS TYPE'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_limit_values(self):

        self._test_post(
            {'award_id': 1, 'limit': 1},
            (None, 2, 1, False, True, 5)
        )

        self._test_post(
            {'award_id': 1, 'limit': 5},
            (None, None, 1, False, False, 5, 4, 3)
        )

        self._test_post(
            {'award_id': 1, 'limit': 0},
            expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {'award_id': 1, 'limit': 2000000000},
            expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {'award_id': 1, 'limit': {'BOGUS': 'LIMIT'}},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_page_values(self):

        self._test_post(
            {'award_id': 1, 'limit': 1, 'page': 2},
            (1, 3, 2, True, True, 4)
        )

        self._test_post(
            {'award_id': 1, 'limit': 1, 'page': 3},
            (2, None, 3, True, False, 3)
        )

        self._test_post(
            {'award_id': 1, 'limit': 1, 'page': 4},
            (3, None, 4, True, False)
        )

        self._test_post(
            {'award_id': 1, 'limit': 1, 'page': 0},
            expected_status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )

        self._test_post(
            {'award_id': 1, 'limit': 1, 'page': 'BOGUS PAGE'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_sort_columns(self):

        for sortable_column in SORTABLE_COLUMNS:

            self._test_post(
                {'award_id': 1, 'order': 'desc', 'sort': sortable_column},
                (None, None, 1, False, False, 5, 4, 3)
            )

            self._test_post(
                {'award_id': 1, 'order': 'asc', 'sort': sortable_column},
                (None, None, 1, False, False, 3, 4, 5)
            )

        self._test_post(
            {'award_id': 1, 'sort': 'BOGUS FIELD'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_sort_order_values(self):

        self._test_post(
            {'award_id': 1, 'order': 'desc'},
            (None, None, 1, False, False, 5, 4, 3)
        )

        self._test_post(
            {'award_id': 1, 'order': 'asc'},
            (None, None, 1, False, False, 3, 4, 5)
        )

        self._test_post(
            {'award_id': 1, 'order': 'BOGUS ORDER'},
            expected_status_code=status.HTTP_400_BAD_REQUEST
        )

    def test_complete_queries(self):

        self._test_post(
            {'award_id': 1, 'type': 'child_idvs', 'limit': 3, 'page': 1, 'sort': 'description', 'order': 'asc'},
            (None, None, 1, False, False, 3, 4, 5)
        )

        self._test_post(
            {'award_id': 1, 'type': 'child_awards', 'limit': 3, 'page': 1, 'sort': 'description', 'order': 'asc'},
            (None, None, 1, False, False, 6)
        )

    def test_no_grandchildren_returned(self):

        self._test_post(
            {'award_id': 2, 'type': 'child_idvs'},
            (None, None, 1, False, False, 8, 7)
        )

        self._test_post(
            {'award_id': 2, 'type': 'child_awards'},
            (None, None, 1, False, False, 10, 9)
        )

    def test_no_parents_returned(self):

        self._test_post(
            {'award_id': 7, 'type': 'child_idvs'},
            (None, None, 1, False, False)
        )

        self._test_post(
            {'award_id': 7, 'type': 'child_awards'},
            (None, None, 1, False, False, 12, 11)
        )

    def test_nothing_returned_for_bogus_contract_relationship(self):

        self._test_post(
            {'award_id': 9, 'type': 'child_idvs'},
            (None, None, 1, False, False)
        )

        self._test_post(
            {'award_id': 9, 'type': 'child_awards'},
            (None, None, 1, False, False)
        )

    def test_missing_agency(self):
        # A bug was found where awards wouldn't show up if the funding agency was
        # null.  This will reproduce that situation.
        _id = 99999

        mommy.make(
            'awards.TransactionNormalized',
            id=_id
        )
        mommy.make(
            'awards.TransactionFPDS',
            transaction_id=_id,
            funding_agency_name='subtier_funding_agency_name_%s' % _id
        )
        parent_n = PARENTS.get(3)  # Use use parent information for I3
        mommy.make(
            'awards.Award',
            id=_id,
            generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % _id,
            type='CONTRACT_%s' % _id,
            total_obligation=_id,
            piid='piid_%s' % _id,
            fpds_agency_id='fpds_agency_id_%s' % _id,
            parent_award_piid='piid_%s' % parent_n,
            fpds_parent_agency_id='fpds_agency_id_%s' % parent_n,
            latest_transaction_id=_id,
            type_description='type_description_%s' % _id,
            description='description_%s' % _id,
            period_of_performance_current_end_date='2018-03-28',
            period_of_performance_start_date='2018-02-28'
        )

        response = self.client.post(ENDPOINT, {'award_id': parent_n, 'type': 'child_awards'})

        # This should return two results.  Prior to the bug, only one result would be returned.
        assert len(response.data['results']) == 2
