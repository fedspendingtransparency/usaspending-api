import json
import pytest

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def model_instances():
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="050",
        budget_function_title="National Defense",
        budget_subfunction_code="051",
        budget_subfunction_title="Department of Defense-Military",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="050",
        budget_function_title="National Defense",
        budget_subfunction_code="054",
        budget_subfunction_title="Defense-related activities",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="050",
        budget_function_title="National Defense",
        budget_subfunction_code="053",
        budget_subfunction_title="Atomic energy defense activities",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="276",
        budget_subfunction_title="Energy information, policy, and regulation",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="271",
        budget_subfunction_title="Energy supply",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="272",
        budget_subfunction_title="Energy conservation",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="274",
        budget_subfunction_title="Emergency energy preparedness",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="276",
        budget_subfunction_title="Energy information, policy, and regulation",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="271",
        budget_subfunction_title="Energy supply",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="272",
        budget_subfunction_title="Energy conservation",
    )
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code="270",
        budget_function_title="Energy",
        budget_subfunction_code="274",
        budget_subfunction_title="Emergency energy preparedness",
    )


@pytest.mark.django_db
def test_list_budget_functions_unique(model_instances, client):
    """Ensure the list_budget_functions endpoint returns unique values"""
    response = client.get("/api/v2/budget_functions/list_budget_functions/")

    assert response.status_code == status.HTTP_200_OK
    assert "results" in response.json()
    assert len(response.json()["results"]) == 2


@pytest.mark.django_db
def test_list_budget_subfunctions_unique(model_instances, client):
    """Ensure the list_budget_subfunctions endpoint returns unique values"""
    response = client.post(
        "/api/v2/budget_functions/list_budget_subfunctions/", content_type="application/json", data=json.dumps({})
    )

    assert response.status_code == status.HTTP_200_OK
    assert "results" in response.json()
    assert len(response.json()["results"]) == 7


@pytest.mark.django_db
def test_list_budget_subfunctions_filter(model_instances, client):
    """Ensure the list_budget_subfunctions endpoint filters by the budget_function_code"""
    response = client.post(
        "/api/v2/budget_functions/list_budget_subfunctions/",
        content_type="application/json",
        data=json.dumps({"budget_function": "050"}),
    )

    assert response.status_code == status.HTTP_200_OK
    assert "results" in response.json()
    assert len(response.json()["results"]) == 3
