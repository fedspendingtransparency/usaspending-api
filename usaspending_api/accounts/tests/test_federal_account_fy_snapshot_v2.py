import pytest
from datetime import datetime

from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.helpers import fy

@pytest.fixture
def financial_spending_data(db):
    this_fy = fy(datetime.today())
    subm2018 = mommy.make('submissions.SubmissionAttributes', submission_id=1, reporting_fiscal_year=this_fy)

    # create Object classes
    aab1 = mommy.make(
        'accounts.AppropriationAccountBalances',
        treasury_account_identifier__federal_account__id=1,
        treasury_account_identifier__federal_account_id=1,
        final_of_fy=True,
        submission=subm2018,
        gross_outlay_amount_by_tas_cpe=1000000,
        budget_authority_available_amount_total_cpe=2000000,
        obligations_incurred_total_by_tas_cpe=3000000,
        unobligated_balance_cpe=4000000,
        budget_authority_unobligated_balance_brought_forward_fyb=5000000,
        adjustments_to_unobligated_balance_brought_forward_cpe=6000000,
        other_budgetary_resources_amount_cpe=7000000,
        budget_authority_appropriated_amount_cpe=8000000,
    )


@pytest.mark.django_db
def test_federal_account_fiscal_year_snapshot_v2_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/federal_accounts/1/fiscal_year_snapshot')
    assert resp.status_code == status.HTTP_200_OK

    # test response in correct form

    assert 'results' in resp.json()
    results = resp.json()['results']
    assert 'outlay' in results
    assert 'budget_authority' in results
    assert 'obligated' in results
    assert 'unobligated' in results
    assert 'balance_brought_forward' in results
    assert 'other_budgetary_resources' in results
    assert 'appropriations' in results

    assert results['outlay'] == 1000000
    assert results['budget_authority'] == 2000000
    assert results['obligated'] == 3000000
    assert results['unobligated'] == 4000000
    assert results['balance_brought_forward'] == 11000000
    assert results['other_budgetary_resources'] == 7000000
    assert results['appropriations'] == 8000000
