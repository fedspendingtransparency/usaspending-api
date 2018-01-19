import pytest
from datetime import datetime

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):
    latest_subm = mommy.make(
        'submissions.SubmissionAttributes', certified_date='2017-12-01', reporting_fiscal_year=2017)
    last_year_subm = mommy.make(
        'submissions.SubmissionAttributes', certified_date='2016-12-01', reporting_fiscal_year=2016)

    # create Object classes
    mommy.make(
        'accounts.AppropriationAccountBalances',
        treasury_account_identifier__federal_account__id=1,
        treasury_account_identifier__federal_account_id=1,
        final_of_fy=True,
        submission=latest_subm,
        gross_outlay_amount_by_tas_cpe=1000000,
        budget_authority_available_amount_total_cpe=2000000,
        obligations_incurred_total_by_tas_cpe=3000000,
        unobligated_balance_cpe=4000000,
        budget_authority_unobligated_balance_brought_forward_fyb=5000000,
        adjustments_to_unobligated_balance_brought_forward_cpe=6000000,
        other_budgetary_resources_amount_cpe=7000000,
        budget_authority_appropriated_amount_cpe=8000000,
    )

    # these AAB records should not show up in the endpoint, they are too old
    mommy.make(
        'accounts.AppropriationAccountBalances',
        treasury_account_identifier__federal_account__id=1,
        treasury_account_identifier__federal_account_id=1,
        final_of_fy=False,
        submission=latest_subm,
        gross_outlay_amount_by_tas_cpe=999,
    )
    mommy.make(
        'accounts.AppropriationAccountBalances',
        treasury_account_identifier__federal_account__id=1,
        treasury_account_identifier__federal_account_id=1,
        final_of_fy=True,
        submission=last_year_subm,
        gross_outlay_amount_by_tas_cpe=999,
    )


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


@pytest.mark.django_db
def test_federal_account_fiscal_year_snapshot_v2_endpoint_no_results(client, financial_spending_data):
    """Test response when no AAB records found."""

    resp = client.get('/api/v2/federal_accounts/999/fiscal_year_snapshot')
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {}
