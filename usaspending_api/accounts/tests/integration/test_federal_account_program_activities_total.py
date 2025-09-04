import json
import pytest
from model_bakery import baker
from rest_framework import status

url = "/api/v2/federal_accounts/{federal_account_code}/program_activities/total/{query_params}"


@pytest.fixture
def program_activities_total_test_data():
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
    sa = baker.make("submissions.SubmissionAttributes", is_final_balances_for_fy=True, submission_id=1)
    oc1 = baker.make("references.ObjectClass", major_object_class_name="moc1", object_class_name="oc1")
    oc2 = baker.make("references.ObjectClass", major_object_class_name="moc1", object_class_name="oc1")
    oc3 = baker.make("references.ObjectClass", major_object_class_name="moc2", object_class_name="oc3")
    oc4 = baker.make("references.ObjectClass", major_object_class_name="moc2", object_class_name="oc4")
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_1,
        program_activity=None,
        submission=sa,
        obligations_incurred_by_program_object_class_cpe=6000,
        reporting_period_start="2020-05-03",
        reporting_period_end="2020-05-04",
        object_class=oc1,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=None,
        program_activity=pac_pan_1,
        submission=sa,
        obligations_incurred_by_program_object_class_cpe=1,
        reporting_period_start="2021-05-03",
        reporting_period_end="2021-05-04",
        object_class=oc2,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_2,
        program_activity=pac_pan_2,
        submission=sa,
        obligations_incurred_by_program_object_class_cpe=130,
        reporting_period_start="2024-05-03",
        reporting_period_end="2024-05-04",
        object_class=oc3,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_3,
        program_activity=None,
        submission=sa,
        obligations_incurred_by_program_object_class_cpe=-1500,
        reporting_period_start="2020-05-03",
        reporting_period_end="2020-05-04",
        object_class=oc4,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_1,
        program_activity_reporting_key=park_3,
        program_activity=None,
        submission=sa,
        obligations_incurred_by_program_object_class_cpe=45612,
        reporting_period_start="2025-05-03",
        reporting_period_end="2025-05-04",
        object_class=oc4,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=treasury_account_2,
        program_activity_reporting_key=park_4,
        program_activity=None,
        submission=sa,
        obligations_incurred_by_program_object_class_cpe=1200,
        object_class=oc4,
    )


@pytest.mark.django_db
def test_success(client, program_activities_total_test_data):
    resp = client.post("/api/v2/federal_accounts/000-0001/program_activities/total", content_type="application/json")

    expected_results = {
        "results": [
            {"obligations": 44112.0, "code": "00000000003", "name": "PARK 3", "type": "PARK"},
            {"obligations": 6000.0, "code": "00000000001", "name": "PARK 1", "type": "PARK"},
            {"obligations": 130.0, "code": "00000000002", "name": "PARK 2", "type": "PARK"},
            {"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
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
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_success_with_filters(client, program_activities_total_test_data):
    request = {
        "filters": {
            "time_period": [{"start_date": "2020-01-01", "end_date": "2022-01-01"}],
            "program_activity": ["0001"],
        }
    }
    resp = client.post(
        "/api/v2/federal_accounts/000-0001/program_activities/total",
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [{"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"}],
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
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_object_class_filter(client, program_activities_total_test_data):
    request = {"filters": {"object_class": ["moc1", "oc3"]}}
    resp = client.post(
        "/api/v2/federal_accounts/000-0001/program_activities/total",
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [
            {"obligations": 6000.0, "code": "00000000001", "name": "PARK 1", "type": "PARK"},
            {"obligations": 130.0, "code": "00000000002", "name": "PARK 2", "type": "PARK"},
            {"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 3,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results
