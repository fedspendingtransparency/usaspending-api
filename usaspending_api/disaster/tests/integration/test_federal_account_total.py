import pytest
import json
from model_mommy import mommy
from rest_framework import status

url = "/api/v2/disaster/federal_account/spending/"


@pytest.fixture
def account_data():
    # fed_acct1 = mommy.make(
    #     "accounts.FederalAccount", account_title="gifts", federal_account_code="000-0000"
    # )
    # tre_acct1 = mommy.make(
    #     "accounts.TreasuryAppropriationAccount",
    #     federal_account=fed_acct1,
    #     tas_rendering_label="2020/99",
    #     account_title="flowers",
    # )
    # sub1 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2020, reporting_fiscal_period=3)
    # mommy.make(
    #     "financial_activities.FinancialAccountsByProgramActivityObjectClass",
    #     submission=sub1,
    #     final_of_fy=True,
    #     obligations_incurred_by_program_object_class_cpe=100,
    #     gross_outlay_amount_by_program_object_class_cpe=111,
    #     disaster_emergency_fund__code="M",
    #     treasury_account=tre_acct1,
    # )

    sub1 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2020, reporting_fiscal_period=3)
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        final_of_fy=True,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=111,
        disaster_emergency_fund__code="M",
        treasury_account__treasury_account_identifier=22,
        treasury_account__tas_rendering_label="2020/99",
        treasury_account__account_title="flowers",
        treasury_account__federal_account__id=21,
        treasury_account__federal_account__account_title="gifts",
        treasury_account__federal_account__federal_account_code="000-0000",
    )


@pytest.mark.django_db
def test_federal_account_success(client, account_data):
    body = {"filter": {"def_codes": ["M"]}, "spending_type": "total"}

    resp = client.post(url, content_type="application/json", data=json.dumps(body))
    expected_result = {
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "children": [
                    {
                        "code": "2020/99",
                        "count": 1,
                        "description": "flowers",
                        "id": 22,
                        "obligation": 100.0,
                        "outlay": 111.0,
                        "total_budgetary_resources": None,
                    }
                ],
                "code": "000-0000",
                "count": 1,
                "description": "gifts",
                "id": 21,
                "obligation": 100.0,
                "outlay": 111.0,
                "total_budgetary_resources": None,
            }
        ],
    }
    assert resp.status_code == status.HTTP_200_OK
    print(json.dumps(resp.json()))
    assert resp.json() == expected_result
