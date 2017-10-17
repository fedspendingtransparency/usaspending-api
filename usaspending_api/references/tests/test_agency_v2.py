import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def financial_spending_data(db):
    # Create agency - submission relationship
    # Create AGENCY AND TopTier AGENCY
    ttagency1 = mommy.make('references.ToptierAgency', name="tta_name", cgac_code='100', website='http://test.com',
                           mission='test', icon_filename='test')
    mommy.make('references.Agency', id=1, toptier_agency=ttagency1)

    # create TAS
    tas = mommy.make('accounts.TreasuryAppropriationAccount', funding_toptier_agency=ttagency1)

    # CREATE SUBMISSIONS
    # submission_3 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2015, cgac_code='100')
    submission_1 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2017,
                              reporting_fiscal_quarter=2, cgac_code='100')
    # submission_2 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2016, cgac_code='100')

    # CREATE AppropriationAccountBalances
    aab = mommy.make('accounts.AppropriationAccountBalances', final_of_fy=True, reporting_period_start="2017-1-1",
                     submission=submission_1, budget_authority_available_amount_total_cpe=2,
                     obligations_incurred_total_by_tas_cpe=2, gross_outlay_amount_by_tas_cpe=2,
                     treasury_account_identifier=tas)

    # CREATE OverallTotals
    ot = mommy.make('references.OverallTotals', fiscal_year=2017, total_budget_authority=3860000000.00)


@pytest.mark.django_db
def test_award_type_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/references/agency/1/')
    assert resp.status_code == status.HTTP_200_OK
    # expected resp.data:
    # assert resp.data == {'results': {'agency_name': 'tta_name', 'active_fy': '2017', 'active_fq': '2',
    #                                  'outlay_amount': '2.00', 'obligated_amount': '2.00',
    #                                  'budget_authority_amount': '2.00',
    #                                  'current_total_budget_authority_amount': '8361447130497.72',
    #                                  'website': 'http://test.com',
    #                                  'mission': 'test',
    #                                  'icon_filename': 'test'}}

    assert resp.data['results']['outlay_amount'] == '2.00'
    assert resp.data['results']['obligated_amount'] == '2.00'
    assert resp.data['results']['budget_authority_amount'] == '2.00'

    # check for bad request due to missing params
    resp = client.get('/api/v2/references/agency/4/')
    assert resp.data == {'results': {}}
