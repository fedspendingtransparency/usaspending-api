import pytest
from model_bakery import baker
from rest_framework import status

url = "/api/v2/federal_accounts/{federal_account_code}/program_activities/{query_params}"


@pytest.fixture
def program_activities_test_data():
    federal_account_1 = baker.make("accounts.FederalAccount", federal_account_code="000-0001")
    federal_account_2 = baker.make("accounts.FederalAccount", federal_account_code="000-0002")
    treasury_account_1 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=federal_account_1)
    treasury_account_2 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=federal_account_2)
    park_1 = baker.make("references.ProgramActivityPark", code="00000000001", name="PARK 1")
    park_2 = baker.make("references.ProgramActivityPark", code="00000000002", name="PARK 2")
    park_3 = baker.make("references.ProgramActivityPark", code="00000000003", name="PARK 3")
    park_4 = baker.make("references.ProgramActivityPark", code="00000000004", name="PARK 4")
    pac_pan_1 = baker.make(
        "references.RefProgramActivity", program_activity_code="0001", program_activity_name="PAC/PAN 1"
    )
    pac_pan_2 = baker.make(
        "references.RefProgramActivity", program_activity_code="0002", program_activity_name="PAC/PAN 2"
    )
    sa = baker.make("submissions.SubmissionAttributes", is_final_balances_for_fy=True)
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_1,
        program_activity=None,
        submission=sa,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=None,
        program_activity=pac_pan_1,
        submission=sa,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_2,
        program_activity=pac_pan_2,
        submission=sa,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_3,
        program_activity=None,
        submission=sa,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_3,
        program_activity=None,
        submission=sa,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_2,
        program_activity_reporting_key=park_4,
        program_activity=None,
        submission=sa,
    )


@pytest.mark.django_db
def test_success(client, program_activities_test_data):
    resp = client.get(url.format(federal_account_code="000-0001", query_params=""))
    expected_result = {
        "results": [
            {"code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
            {"code": "00000000003", "name": "PARK 3", "type": "PARK"},
            {"code": "00000000002", "name": "PARK 2", "type": "PARK"},
            {"code": "00000000001", "name": "PARK 1", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 4,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    resp = client.get(url.format(federal_account_code="000-0002", query_params=""))
    expected_result = {
        "results": [
            {"code": "00000000004", "name": "PARK 4", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 1,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_pagination(client, program_activities_test_data):
    resp = client.get(url.format(federal_account_code="000-0001", query_params="?limit=3&sort=name&order=asc"))
    expected_result = {
        "results": [
            {"code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
            {"code": "00000000001", "name": "PARK 1", "type": "PARK"},
            {"code": "00000000002", "name": "PARK 2", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 4,
            "limit": 3,
            "next": 2,
            "previous": None,
            "hasNext": True,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    resp = client.get(url.format(federal_account_code="000-0001", query_params="?limit=3&sort=name&order=asc&page=2"))
    expected_result = {
        "results": [
            {"code": "00000000003", "name": "PARK 3", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 2,
            "total": 4,
            "limit": 3,
            "next": None,
            "previous": 1,
            "hasNext": False,
            "hasPrevious": True,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
