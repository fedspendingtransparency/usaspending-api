import json

from django.db import connection
from django.db.models import Max
from django.test import TestCase
from model_mommy import mommy
from rest_framework import status
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.v2.views.idvs.funding import SORTABLE_COLUMNS


ENDPOINT = '/api/v2/awards/idvs/funding/'

AWARD_COUNT = 15
IDVS = (1, 2, 3, 4, 5, 7, 8)
PARENTS = {3: 1, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 2, 11: 7, 12: 7, 13: 8, 14: 8, 15: 9}


class IDVFundingTestCase(TestCase):

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
        for _id in range(1, AWARD_COUNT + 1):
            _pid = PARENTS.get(_id)

            # To ensure our text sorts happen in the correct order, pad numbers with zeros.
            _spid = str(_pid).zfill(3) if _pid else None
            _sid = str(_id).zfill(3)

            mommy.make(
                'references.Agency',
                id=9000 + _id
            )

            mommy.make(
                'awards.Award',
                id=_id,
                generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % _sid,
                type=('IDV_%s' if _id in IDVS else 'CONTRACT_%s') % _sid,
                piid='piid_%s' % _sid,
                fpds_agency_id='fpds_agency_id_%s' % _sid,
                parent_award_piid='piid_%s' % _spid if _spid else None,
                fpds_parent_agency_id='fpds_agency_id_%s' % _spid if _spid else None,
                funding_agency_id=9000 + _id
            )

            if _id in IDVS:
                mommy.make(
                    'awards.ParentAward',
                    award_id=_id,
                    generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % _sid,
                    parent_award_id=_pid
                )

            mommy.make(
                'submissions.SubmissionAttributes',
                submission_id=_id,
                reporting_fiscal_year=2000 + _id,
                reporting_fiscal_quarter=_id % 4 + 1
            )

            mommy.make(
                'accounts.TreasuryAppropriationAccount',
                treasury_account_identifier=_id,
                reporting_agency_id=str(_id).zfill(3),
                reporting_agency_name='reporting agency name %s' % _sid,
                agency_id=str(_id).zfill(3),
                main_account_code=str(_id).zfill(4),
                account_title='account title %s' % _sid
            )

            mommy.make(
                'references.RefProgramActivity',
                id=_id,
                program_activity_code=_sid,
                program_activity_name='program activity %s' % _sid
            )

            mommy.make(
                'references.ObjectClass',
                id=_id,
                object_class='1' + _sid,
                object_class_name='object class %s' % _sid
            )

            mommy.make(
                'awards.FinancialAccountsByAwards',
                financial_accounts_by_awards_id=_id,
                award_id=_id,
                submission_id=_id,
                treasury_account_id=_id,
                program_activity_id=_id,
                object_class_id=_id,
                transaction_obligated_amount=_id * 10000 + _id + _id / 100
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
        response = self.client.post(ENDPOINT, request)
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
            ENDPOINT,
            {'award_id': 7, 'sort': 'transaction_obligated_amount', 'order': 'desc'}
        )
        assert response.status_code == 200
        result = json.loads(response.content.decode('utf-8'))
        assert len(result['results']) == 2
        for r in result['results']:
            assert r['transaction_obligated_amount'] in (None, 110011.11)
