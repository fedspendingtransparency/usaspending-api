import json

import pytest
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):
    ta1 = baker.make("TreasuryAppropriationAccount", federal_account__id=1, federal_account_id=1)
    baker.make(
        "FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        object_class__major_object_class=10,
        object_class__object_class=111,
        program_activity__program_activity_code="0001",
    )
    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2016-01-01",
        reporting_period_end="2016-06-01",
        submission__reporting_fiscal_year=2016,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta1,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )

    ta1a = baker.make("TreasuryAppropriationAccount", federal_account__id=1, federal_account_id=1)
    baker.make(
        "FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        object_class__major_object_class=40,
        object_class__object_class=444,
        program_activity__program_activity_code="0004",
    )
    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2014-01-01",
        reporting_period_end="2014-06-01",
        submission__reporting_fiscal_year=2014,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta1a,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )

    ta2 = baker.make("TreasuryAppropriationAccount", federal_account__id=2, federal_account_id=2)
    baker.make(
        "FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta2,
        object_class__major_object_class=10,
        object_class__object_class=111,
        program_activity__program_activity_code="0001",
    )

    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2016-01-01",
        reporting_period_end="2016-06-01",
        submission__reporting_fiscal_year=2016,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta2,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )

    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2016-01-01",
        reporting_period_end="2016-06-01",
        submission__reporting_fiscal_year=2016,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta2,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )

    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2014-01-01",
        reporting_period_end="2014-06-01",
        submission__reporting_fiscal_year=2014,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta1,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )

    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2016-01-01",
        reporting_period_end="2016-06-01",
        submission__reporting_fiscal_year=2016,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta1,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )

    baker.make(
        "AppropriationAccountBalances",
        reporting_period_start="2016-01-01",
        reporting_period_end="2016-06-01",
        submission__reporting_fiscal_year=2016,
        submission__reporting_fiscal_quarter=3,
        treasury_account_identifier=ta1,
        final_of_fy=True,
        unobligated_balance_cpe=10000,
        gross_outlay_amount_by_tas_cpe=1000000,
        obligations_incurred_total_by_tas_cpe=1000,
    )


specific_payload = {
    "category": "program_activity",
    "filters": {
        "object_class": [
            {
                "major_object_class": 10,  # Personnel compensation and benefits
                "object_class": [111, 113],  # Full-time permanent, Other than full-time permanent, ...
            },
            {"major_object_class": 90},  # Other
        ],
        "program_activity": ["0001", "0002"],
        "time_period": [{"start_date": "2010-10-01", "end_date": "2020-11-30"}],
    },
}

simple_payload = {
    "category": "program_activity",
    "filters": {"time_period": [{"start_date": "2010-10-01", "end_date": "2020-11-30"}]},
}

fy2016_payload = {
    "category": "program_activity",
    "filters": {"time_period": [{"start_date": "2015-10-01", "end_date": "2016-09-30"}]},
}


@pytest.mark.skip
@pytest.mark.django_db
def test_federal_account_spending_over_time(client, financial_spending_data):
    """Test grouping over time"""

    resp = client.post(
        "/api/v2/federal_accounts/1/spending_over_time",
        content_type="application/json",
        data=json.dumps(simple_payload),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test response in correct form
    assert "results" in resp.json()
    simple_results = resp.json()["results"]
    assert len(simple_results)
    for result in simple_results:
        for k, v in result.items():
            assert isinstance(k, str)
            assert hasattr(v, "__pow__") or k == "time_period"  # is a number
    assert simple_results[0]["outlay"] == 2000000
    assert simple_results[0]["time_period"] == {"fiscal_year": "2014", "quarter": "3"}
    assert simple_results[1]["outlay"] == 3000000
    assert simple_results[1]["time_period"] == {"fiscal_year": "2016", "quarter": "3"}

    resp = client.post(
        "/api/v2/federal_accounts/1/spending_over_time",
        content_type="application/json",
        data=json.dumps(fy2016_payload),
    )
    fy2016_results = resp.json()["results"]
    assert fy2016_results[0]["outlay"] == 0
    assert fy2016_results[0]["time_period"] == {"fiscal_year": "2014", "quarter": "3"}
    assert fy2016_results[1]["outlay"] == 3000000
    assert fy2016_results[1]["time_period"] == {"fiscal_year": "2016", "quarter": "3"}

    resp = client.post(
        "/api/v2/federal_accounts/1/spending_over_time",
        content_type="application/json",
        data=json.dumps(specific_payload),
    )
    specific_results = resp.json()["results"]
    assert len(specific_results) == 2
    assert specific_results[0]["outlay"] == 1000000
    assert specific_results[0]["time_period"] == {"fiscal_year": "2014", "quarter": "3"}
    assert specific_results[1]["outlay"] == 3000000
    assert specific_results[1]["time_period"] == {"fiscal_year": "2016", "quarter": "3"}
