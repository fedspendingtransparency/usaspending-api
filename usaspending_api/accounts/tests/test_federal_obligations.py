import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def financial_obligations_models():
    fiscal_year = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2016)
    top_tier_id = mommy.make('references.Agency', id=654, toptier_agency_id=987).toptier_agency_id
    top_tier = mommy.make('references.ToptierAgency', toptier_agency_id=top_tier_id)
    federal_id_awesome = mommy.make('accounts.FederalAccount', id=6969, account_title='Turtlenecks and Chains')
    federal_id_lame = mommy.make('accounts.FederalAccount', id=1234, account_title='Suits and Ties')
    """
        Until this gets updated with mock.Mock(),
        the following cascade of variables applied to parameters,
        is the work around for annotating.
    """
    funding = mommy.make(
        'accounts.TreasuryAppropriationAccount',
        funding_toptier_agency=top_tier
    ).funding_toptier_agency

    agency_name = mommy.make(
        'accounts.TreasuryAppropriationAccount',
        funding_toptier_agency=funding,
        reporting_agency_name='Department of Style'
    ).reporting_agency_name

    acct_awesome = mommy.make(
        'accounts.TreasuryAppropriationAccount',
        funding_toptier_agency=funding,
        reporting_agency_name=agency_name,
        account_title='Turtlenecks and Chains'
    ).account_title

    id_awesome = mommy.make(
        'accounts.TreasuryAppropriationAccount',
        funding_toptier_agency=funding,
        reporting_agency_name=agency_name,
        account_title=acct_awesome,
        federal_account=federal_id_awesome
    )

    acct_lame = mommy.make(
        'accounts.TreasuryAppropriationAccount',
        funding_toptier_agency=top_tier,
        reporting_agency_name=agency_name,
        account_title='Suits and Ties'
    ).account_title

    id_lame = mommy.make(
        'accounts.TreasuryAppropriationAccount',
        funding_toptier_agency=funding,
        reporting_agency_name=agency_name,
        account_title=acct_lame,
        federal_account=federal_id_lame
    )

    # Get Awesome account values
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=-100,
        treasury_account=id_awesome
    )
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=200,
        treasury_account=id_awesome
    )
    # Test to make sure False value is ignored in calculation
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=fiscal_year,
        final_of_fy=False,
        obligations_incurred_by_program_object_class_cpe=200,
        treasury_account=id_awesome
    )

    # Get Lame account values
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=500,
        treasury_account=id_lame
    )
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=-100,
        treasury_account=id_lame
    )


@pytest.mark.django_db
def test_financial_obligations(client, financial_obligations_models):
    """Test the financial_obligations endpoint."""
    resp = client.get('/api/v2/federal_obligations/?funding_agency_id=654&fiscal_year=2016')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2
    res_awesome = resp.data['results'][0]
    assert res_awesome['id'] == '1234'
    assert res_awesome['agency_name'] == 'Department of Style'
    assert res_awesome['account_title'] == 'Suits and Ties'
    assert res_awesome['obligated_amount'] == '400.00'
    res_lame = resp.data['results'][1]
    assert res_lame['id'] == '6969'
    assert res_lame['agency_name'] == 'Department of Style'
    assert res_lame['account_title'] == 'Turtlenecks and Chains'
    assert res_lame['obligated_amount'] == '100.00'


@pytest.mark.django_db
def test_financial_obligations_params(client, financial_obligations_models):
    """Test invalid financial_obligations parameters."""
    resp = client.get('/api/v2/federal_obligations/?fiscal_year=2016')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
