# Stdlib imports
import json
from decimal import Decimal

# Core Django imports

# Third-party app imports
import pytest
from model_bakery import baker
from rest_framework import status

# Imports from your apps
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import RefProgramActivity
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.mark.django_db
def test_federal_account_spending_by_category_unique_program_activity_names(client):
    """
    Test the spending by category endpoint for the federal account profile page to ensure a unique set of
    program activity names are returned
    """

    models_to_mock = [
        {"model": FederalAccount, "id": -1},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -2, "federal_account_id": -1},
        {
            "model": RefProgramActivity,
            "id": -3,
            "program_activity_name": "SUPERMAN",
            "program_activity_code": "SUPR",
            "responsible_agency_id": "999",
            "allocation_transfer_agency_id": "999",
            "main_account_code": "9999",
            "budget_year": "1111",
        },
        {
            "model": RefProgramActivity,
            "id": -4,
            "program_activity_name": "SUPERMAN",
            "program_activity_code": "SUPR",
            "responsible_agency_id": "888",
            "allocation_transfer_agency_id": "888",
            "main_account_code": "8888",
            "budget_year": "2222",
        },
        {"model": SubmissionAttributes, "submission_id": 1, "is_final_balances_for_fy": True},
        {
            "model": FinancialAccountsByProgramActivityObjectClass,
            "financial_accounts_by_program_activity_object_class_id": -5,
            "obligations_incurred_by_program_object_class_cpe": -10,
            "treasury_account_id": -2,
            "program_activity_id": -3,
            "submission_id": 1,
        },
        {
            "model": FinancialAccountsByProgramActivityObjectClass,
            "financial_accounts_by_program_activity_object_class_id": -6,
            "obligations_incurred_by_program_object_class_cpe": -10,
            "treasury_account_id": -2,
            "program_activity_id": -4,
            "submission_id": 1,
        },
    ]

    for entry in models_to_mock:
        baker.make(entry.pop("model"), **entry)

    request = {
        "group": "program_activity__program_activity_name",
        "field": "obligations_incurred_by_program_object_class_cpe",
        "aggregate": "sum",
        "order": ["-aggregate"],
        "filters": [{"field": "treasury_account__federal_account", "operation": "equals", "value": -1}],
        "page": 1,
        "limit": 5,
        "auditTrail": "Rank vis - programActivity",
    }

    resp = client.post("/api/v1/tas/categories/total/", content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert "results" in resp.json()

    result_content = resp.json()["results"]

    expected_results = {
        "result_count": 1,
        "program_activity_name_via_item": ["SUPERMAN"],
        "program_activity_name_via_field": ["SUPERMAN"],
        "program_activity_totals": -20,
    }

    actual_results = {
        "result_count": len(result_content),
        "program_activity_name_via_item": [entry["item"] for entry in result_content],
        "program_activity_name_via_field": [
            entry["program_activity__program_activity_name"] for entry in result_content
        ],
        "program_activity_totals": sum([Decimal(entry["aggregate"]) for entry in result_content]),
    }

    assert expected_results == actual_results
