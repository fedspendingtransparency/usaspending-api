import json

import pytest
from rest_framework import status

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_award_type_success(client, refresh_matviews):

    # test for filters
    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A", "B", "C"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp = client.post(
        "/api/v2/search/spending_by_award_count",
        content_type="application/json",
        data=json.dumps({"filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_type_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps({"test": {}, "filters": {}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_award_no_intersection(client, mock_matviews_qs):
    mock_model_1 = MockModel(
        award_ts_vector="",
        type="02",
        type_of_contract_pricing="",
        naics_code="98374",
        cfda_number="987.0",
        pulled_from=None,
        uri=None,
        piid="djsd",
        fain=None,
        category="grant",
        award_id=90,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        generated_pragmatic_obligation=10,
        counts=3,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1])

    request = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {"award_type_codes": ["02", "03", "04", "05"]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["grants"] == 3

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == {
        "contracts": 0,
        "idvs": 0,
        "grants": 0,
        "direct_payments": 0,
        "loans": 0,
        "other": 0,
    }, "Results returned, there should all be 0"


@pytest.mark.django_db
def test_spending_by_award_subawards_no_intersection(client, mock_matviews_qs):
    mock_model_1 = MockModel(
        award_ts_vector="",
        subaward_id=9999,
        award_type="grant",
        prime_award_type="02",
        award_id=90,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        generated_pragmatic_obligation=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1])

    request = {
        "subawards": True,
        "fields": ["Sub-Award ID"],
        "sort": "Sub-Award ID",
        "filters": {"award_type_codes": ["02", "03", "04", "05"]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["subgrants"] == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == {"subcontracts": 0, "subgrants": 0}, "Results returned, there should all be 0"
