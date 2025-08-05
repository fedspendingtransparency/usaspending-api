from copy import deepcopy
import json

import pytest
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):

    ta1 = baker.make("TreasuryAppropriationAccount", treasury_account_identifier=1, federal_account__id=1)

    oc111 = baker.make("references.ObjectClass", major_object_class="10", object_class="111")
    oc113 = baker.make("references.ObjectClass", major_object_class="10", object_class="113")
    oc210 = baker.make("references.ObjectClass", major_object_class="20", object_class="210")
    oc310 = baker.make("references.ObjectClass", major_object_class="30", object_class="310")

    pa0001 = baker.make(
        "references.RefProgramActivity", program_activity_code="0001", program_activity_name="Office of the Secretary"
    )
    pa0002 = baker.make(
        "references.RefProgramActivity",
        program_activity_code="0002",
        program_activity_name="Under/Assistant Secretaries",
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2016-07-01",
        reporting_period_end="2016-12-01",
        program_activity=pa0001,
        object_class=oc111,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2016-07-01",
        reporting_period_end="2016-12-01",
        program_activity=pa0002,
        object_class=oc111,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2017-07-01",
        reporting_period_end="2017-12-01",
        program_activity=pa0001,
        object_class=oc113,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2016-07-01",
        reporting_period_end="2016-12-01",
        program_activity=pa0002,
        object_class=oc113,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2016-07-01",
        reporting_period_end="2016-12-01",
        program_activity=pa0001,
        object_class=oc210,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2017-07-01",
        reporting_period_end="2017-12-01",
        program_activity=pa0002,
        object_class=oc210,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2016-07-01",
        reporting_period_end="2017-12-01",
        program_activity=pa0001,
        object_class=oc310,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=ta1,
        reporting_period_start="2016-07-01",
        reporting_period_end="2016-12-01",
        program_activity=pa0002,
        object_class=oc310,
        obligations_incurred_by_program_object_class_cpe=1000000,
    )


base_payload = {
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


@pytest.mark.skip
@pytest.mark.django_db
def test_federal_account_spending_by_category(client, financial_spending_data):
    """Test grouping over all available categories"""

    resp = client.post(
        "/api/v2/federal_accounts/1/spending_by_category",
        content_type="application/json",
        data=json.dumps(base_payload),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test response in correct form

    assert "results" in resp.json()
    results = resp.json()["results"]
    assert len(results)
    for k, v in results.items():
        assert isinstance(k, str)
        assert hasattr(v, "__pow__")  # is a number

    assert results["Office of the Secretary"] == 2000000
    assert results["Under/Assistant Secretaries"] == 2000000


@pytest.mark.skip
@pytest.mark.django_db
def test_federal_account_spending_by_category_all_results(client, financial_spending_data):

    payload = deepcopy(base_payload)
    payload["filters"].pop("object_class")
    resp = client.post(
        "/api/v2/federal_accounts/1/spending_by_category", content_type="application/json", data=json.dumps(payload)
    )
    results = resp.json()["results"]
    assert results["Office of the Secretary"] == 4000000
    assert results["Under/Assistant Secretaries"] == 4000000


@pytest.mark.skip
@pytest.mark.django_db
def test_federal_account_spending_by_category_major_obj_filter(client, financial_spending_data):

    payload = deepcopy(base_payload)
    payload["filters"]["object_class"].append({"major_object_class": 20})
    resp = client.post(
        "/api/v2/federal_accounts/1/spending_by_category", content_type="application/json", data=json.dumps(payload)
    )
    results = resp.json()["results"]
    assert results["Office of the Secretary"] == 3000000
    assert results["Under/Assistant Secretaries"] == 3000000


@pytest.mark.skip
@pytest.mark.django_db
def test_federal_account_spending_by_category_filter_program_activity(client, financial_spending_data):

    payload = deepcopy(base_payload)
    payload["filters"]["program_activity"].pop(1)
    resp = client.post(
        "/api/v2/federal_accounts/1/spending_by_category", content_type="application/json", data=json.dumps(payload)
    )
    results = resp.json()["results"]
    assert results["Office of the Secretary"] == 2000000
    assert "Under/Assistant Secretaries" not in results


@pytest.mark.skip
@pytest.mark.django_db
def test_federal_account_spending_by_category_filter_date(client, financial_spending_data):

    payload = deepcopy(base_payload)
    payload["filters"]["time_period"][0]["end_date"] = "2016-12-31"
    resp = client.post(
        "/api/v2/federal_accounts/1/spending_by_category", content_type="application/json", data=json.dumps(payload)
    )
    results = resp.json()["results"]
    assert results["Office of the Secretary"] == 1000000
    assert results["Under/Assistant Secretaries"] == 2000000
