import json
import copy

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass,
    SubmissionAttributes,
    TreasuryAppropriationAccount,
)
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.references.models import Agency, GTASTotalObligation, ToptierAgency


ENDPOINT_URL = "/api/v2/spending/"
CONTENT_TYPE = "application/json"
GLOBAL_MOCK_DICT = [
    {"model": GTASTotalObligation, "fiscal_year": 1600, "fiscal_quarter": 1, "total_obligation": -10},
    {"model": SubmissionAttributes, "submission_id": -1, "reporting_fiscal_year": 1600, "reporting_fiscal_quarter": 1},
    {
        "model": ToptierAgency,
        "toptier_agency_id": -1,
        "name": "random_funding_name_1",
        "cgac_code": "random_funding_code_1",
    },
    {
        "model": ToptierAgency,
        "toptier_agency_id": -2,
        "name": "random_funding_name_2",
        "cgac_code": "random_funding_code_2",
    },
    {"model": Agency, "toptier_agency_id": -1, "toptier_flag": True},
    {"model": Agency, "toptier_agency_id": -2, "toptier_flag": True},
    {
        "model": TreasuryAppropriationAccount,
        "treasury_account_identifier": -1,
        "funding_toptier_agency_id": -1,
        "federal_account_id": 1,
    },
    {
        "model": FederalAccount,
        "id": 1,
        "account_title": "Tommy Two-Tone",
        "agency_identifier": "867",
        "main_account_code": "5309",
    },
    {
        "model": TreasuryAppropriationAccount,
        "treasury_account_identifier": -2,
        "funding_toptier_agency_id": -2,
        "federal_account_id": 1,
    },
    {
        "model": FinancialAccountsByProgramActivityObjectClass,
        "financial_accounts_by_program_activity_object_class_id": -1,
        "submission_id": -1,
        "treasury_account_id": -1,
        "obligations_incurred_by_program_object_class_cpe": -5,
    },
    {
        "model": FinancialAccountsByProgramActivityObjectClass,
        "financial_accounts_by_program_activity_object_class_id": -2,
        "submission_id": -1,
        "treasury_account_id": -1,
        "obligations_incurred_by_program_object_class_cpe": -10,
    },
    {
        "model": FinancialAccountsByProgramActivityObjectClass,
        "financial_accounts_by_program_activity_object_class_id": -3,
        "submission_id": -1,
        "treasury_account_id": -2,
        "obligations_incurred_by_program_object_class_cpe": -1,
    },
]


@pytest.mark.django_db
def test_unreported_data_actual_value_file_b(client):

    models = copy.deepcopy(GLOBAL_MOCK_DICT)
    for entry in models:
        mommy.make(entry.pop("model"), **entry)

    json_request = {"type": "agency", "filters": {"fy": "1600", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    expected_results = {
        "total": -10,
        "agencies": ["Unreported Data", "random_funding_name_2", "random_funding_name_1"],
        "amounts": [6, -1, -15],
    }

    actual_results = {
        "total": json_response["total"],
        "agencies": [entry["name"] for entry in json_response["results"]],
        "amounts": [entry["amount"] for entry in json_response["results"]],
    }

    assert expected_results == actual_results


@pytest.mark.django_db
def test_unreported_data_actual_value_file_c(client):
    models_to_mock = [
        {"model": GTASTotalObligation, "fiscal_year": 1600, "fiscal_quarter": 1, "total_obligation": -10},
        {
            "model": SubmissionAttributes,
            "submission_id": -1,
            "reporting_fiscal_year": 1600,
            "reporting_fiscal_quarter": 1,
        },
        {
            "model": ToptierAgency,
            "toptier_agency_id": -1,
            "name": "random_funding_name_1",
            "cgac_code": "random_funding_code_1",
        },
        {
            "model": ToptierAgency,
            "toptier_agency_id": -2,
            "name": "random_funding_name_2",
            "cgac_code": "random_funding_code_2",
        },
        {"model": Agency, "id": -1, "toptier_agency_id": -1, "toptier_flag": True},
        {"model": Agency, "id": -2, "toptier_agency_id": -2, "toptier_flag": True},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -1, "funding_toptier_agency_id": -1},
        {"model": TreasuryAppropriationAccount, "treasury_account_identifier": -2, "funding_toptier_agency_id": -2},
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -1,
            "submission_id": -1,
            "award__recipient__recipient_name": "random_recipient_name_1",
            "treasury_account_id": -1,
            "transaction_obligated_amount": -5,
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -2,
            "submission_id": -1,
            "award__recipient__recipient_name": "random_recipient_name_2",
            "treasury_account_id": -1,
            "transaction_obligated_amount": -10,
        },
        {
            "model": FinancialAccountsByAwards,
            "financial_accounts_by_awards_id": -3,
            "submission_id": -1,
            "award__recipient__recipient_name": "random_recipient_name_1",
            "treasury_account_id": -2,
            "transaction_obligated_amount": -1,
        },
    ]

    for entry in models_to_mock:
        mommy.make(entry.pop("model"), **entry)

    json_request = {"type": "recipient", "filters": {"agency": "-1", "fy": "1600", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    expected_results = {
        "total": -15,
        "agencies": ["random_recipient_name_1", "random_recipient_name_2"],
        "amounts": [-5, -10],
    }

    actual_results = {
        "total": json_response["total"],
        "agencies": [entry["name"] for entry in json_response["results"]],
        "amounts": [entry["amount"] for entry in json_response["results"]],
    }

    assert expected_results == actual_results


@pytest.mark.django_db
def test_unreported_data_no_data_available(client):
    json_request = {"type": "agency", "filters": {"fy": "1700", "quarter": "1"}}

    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))

    assert response.status_code == status.HTTP_200_OK

    json_response = response.json()

    expected_results = {"total": None}

    actual_results = {"total": json_response["total"]}

    assert expected_results == actual_results


@pytest.mark.django_db
def test_federal_account_linkage(client):
    models = copy.deepcopy(GLOBAL_MOCK_DICT)
    for entry in models:
        mommy.make(entry.pop("model"), **entry)
    json_request = {"type": "federal_account", "filters": {"fy": "1600", "quarter": "1"}}
    response = client.post(path=ENDPOINT_URL, content_type=CONTENT_TYPE, data=json.dumps(json_request))
    json_response = response.json()
    assert json_response["results"][0]["account_number"] == "867-5309"


@pytest.mark.django_db
def test_budget_function_filter_success(client):

    # Test for Budget Function Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "budget_function", "filters": {"fy": "2017"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Budget Sub Function Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "budget_function": "050"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "federal_account",
                "filters": {"fy": "2017", "budget_function": "050", "budget_subfunction": "053"},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "program_activity",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "object_class",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20",
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "budget_function": "050",
                    "budget_subfunction": "053",
                    "federal_account": 2715,
                    "program_activity": 17863,
                    "object_class": "20",
                    "recipient": 13916,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_budget_function_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_object_class_filter_success(client):

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "object_class", "filters": {"fy": "2017"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "object_class": "20"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017", "object_class": "20"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {"type": "program_activity", "filters": {"fy": "2017", "object_class": "20", "federal_account": 2358}}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {"fy": "2017", "object_class": "20", "federal_account": 2358, "program_activity": 15103},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "object_class": "20",
                    "federal_account": 2358,
                    "program_activity": 15103,
                    "recipient": 301773,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_object_class_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_agency_filter_success(client):

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "2017", "quarter": "3"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Agency Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "federal_account", "filters": {"fy": "2017"}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Federal Account Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "program_activity", "filters": {"fy": "2017", "federal_account": 1500}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Program Activity Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {"type": "object_class", "filters": {"fy": "2017", "federal_account": 1500, "program_activity": 12697}}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Recipient Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "recipient",
                "filters": {"fy": "2017", "federal_account": 1500, "program_activity": 12697, "object_class": "40"},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Test for Award Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps(
            {
                "type": "award",
                "filters": {
                    "fy": "2017",
                    "federal_account": 1500,
                    "program_activity": 12697,
                    "object_class": "40",
                    "recipient": 792917,
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_agency_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # Test for Object Class Results
    resp = client.post(
        "/api/v2/spending/",
        content_type="application/json",
        data=json.dumps({"type": "agency", "filters": {"fy": "23", "quarter": "3"}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
