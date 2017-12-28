import pytest
from datetime import datetime

from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.helpers import fy


@pytest.fixture
def financial_spending_data(db):
    this_fy = fy(datetime.today())
    latest_subm = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=this_fy)
    last_year_subm = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=this_fy - 1)

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


@pytest.mark.django_db
def test_federal_account_spending_by_category(client, financial_spending_data):
    """Test response when no AAB records found."""

    payload = {
        "category": "program_activity",
        "filters": {
            "object_classes": [{
                "major_object_class_name": "Personnel compensation and benefits",
                "object_class_names": [
                    "Full-time permanent",
                    "Other than full-time permanent",
                ]
            }, {
                "major_object_class_name": "Other"
            }],
            "program_activites": [1, 2, 3],
            "time_period": [{
                "start_date": "2001-01-01",
                "end_date": "2001-01-31"
            }]
        }
    }
    resp = client.post('/api/v2/federal_accounts/1/spending_by_category', json=payload)
    assert resp.status_code == status.HTTP_200_OK

    # test response in correct form

    assert 'results' in resp.json()
    results = resp.json()['results']
    assert len(results)
    for (k, v) in results.items():
        assert isinstance(k, str)
        assert hasattr(v, '__pow__')  # is a number
