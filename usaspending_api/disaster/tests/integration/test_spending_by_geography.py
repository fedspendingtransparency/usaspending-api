import json
import pytest

from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def post(client, **kwargs):
    url = "/api/v2/disaster/spending_by_geography/"
    request_body = {}
    filters = {}

    if kwargs.get("def_codes"):
        filters["def_codes"] = kwargs["def_codes"]
    if kwargs.get("award_type_codes"):
        filters["award_type_codes"] = kwargs["award_type_codes"]

    request_body["filter"] = filters

    if kwargs.get("geo_layer"):
        request_body["geo_layer"] = kwargs["geo_layer"]
    if kwargs.get("geo_layer_filters"):
        request_body["geo_layer_filters"] = kwargs["geo_layer_filters"]
    if kwargs.get("spending_type"):
        request_body["spending_type"] = kwargs["spending_type"]

    resp = client.post(url, content_type="application/json", data=json.dumps(request_body))
    return resp


@pytest.mark.django_db
def test_spending_by_geography_failure_with_missing_fields(
    client, monkeypatch, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Test required "def_codes" in filter object
    resp = post(client, geo_layer="state", geo_layer_filters=["SC-01"], spending_type="obligation")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"

    # Test required "geo_layer" string
    resp = post(client, def_codes=["L"], geo_layer_filters=["SC-01"], spending_type="obligation")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'geo_layer' is a required field"

    # Test required "geo_layer_filters" string
    resp = post(client, def_codes=["L"], geo_layer="state", spending_type="obligation")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'geo_layer_filters' is a required field"

    # Test required "spending_type" string
    resp = post(client, def_codes=["L"], geo_layer="state", geo_layer_filters=["SC-01"])
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'spending_type' is a required field"


@pytest.mark.django_db
def test_spending_by_geography_failure_with_invalid_fields(
    client, monkeypatch, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Test invalid "geo_layer" string
    resp = post(client, def_codes=["L"], geo_layer="NOT VALID", geo_layer_filters=["SC-01"], spending_type="obligation")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'geo_layer' is outside valid values ['state', 'county', 'district']"

    # Test invalid "spending_type" string
    resp = post(client, def_codes=["L"], geo_layer="state", geo_layer_filters=["SC-01"], spending_type="NOT VALID")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.data["detail"]
        == "Field 'spending_type' is outside valid values ['obligation', 'outlay', 'face_value_of_loan']"
    )

    # Test invalid "geo_layer" string
    resp = post(client, def_codes=["L"], geo_layer="NOT VALID", geo_layer_filters=["SC-01"], spending_type="obligation")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'geo_layer' is outside valid values ['state', 'county', 'district']"

    # Test invalid "spending_type" string
    resp = post(client, def_codes=["L"], geo_layer="state", geo_layer_filters=["SC-01"], spending_type="NOT VALID")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.data["detail"]
        == "Field 'spending_type' is outside valid values ['obligation', 'outlay', 'face_value_of_loan']"
    )


def _get_shape_code_for_sort(result_dict):
    return result_dict["shape_code"]


@pytest.mark.django_db
def test_correct_response_with_different_geo_filters(
    client, monkeypatch, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_for_county,
        _test_correct_response_for_district,
        _test_correct_response_for_state,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_county(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="county",
        geo_layer_filters=["45001", "45005"],
        spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "county",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2000220.0,
                "display_name": "Charleston",
                "per_capita": 2000220.0,
                "population": 1,
                "shape_code": "45001",
            },
            {
                "amount": 200000.0,
                "display_name": "Test Name",
                "per_capita": 20000.0,
                "population": 10,
                "shape_code": "45005",
            },
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_district(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="district",
        geo_layer_filters=["4510", "4550", "5350"],
        spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "district",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2000000.0,
                "display_name": "SC-10",
                "per_capita": None,
                "population": None,
                "shape_code": "4510",
            },
            {
                "amount": 200200.0,
                "display_name": "SC-50",
                "per_capita": 2002.0,
                "population": 100,
                "shape_code": "4550",
            },
            {"amount": 22000.0, "display_name": "WA-50", "per_capita": 22.0, "population": 1000, "shape_code": "5350"},
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_state(client):
    resp = post(client, def_codes=["L", "M"], geo_layer="state", geo_layer_filters=["WA"], spending_type="obligation",)
    expected_response = {
        "geo_layer": "state",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 22000.0,
                "display_name": "Washington",
                "per_capita": 2.2,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


@pytest.mark.django_db
def test_correct_response_with_different_spending_types(
    client, monkeypatch, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_for_obligation,
        _test_correct_response_for_outlay,
        _test_correct_response_for_face_value_of_loan,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_obligation(client):
    resp = post(
        client, def_codes=["L", "M"], geo_layer="state", geo_layer_filters=["SC", "WA"], spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "state",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2200220.0,
                "display_name": "South Carolina",
                "per_capita": 2200.22,
                "population": 1000,
                "shape_code": "SC",
            },
            {
                "amount": 22000.0,
                "display_name": "Washington",
                "per_capita": 2.2,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_outlay(client):
    resp = post(
        client, def_codes=["L", "M"], geo_layer="state", geo_layer_filters=["SC", "WA"], spending_type="outlay",
    )
    expected_response = {
        "geo_layer": "state",
        "spending_type": "outlay",
        "results": [
            {
                "amount": 100.0,
                "display_name": "South Carolina",
                "per_capita": 0.1,
                "population": 1000,
                "shape_code": "SC",
            },
            {
                "amount": 1000.0,
                "display_name": "Washington",
                "per_capita": 0.1,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_face_value_of_loan(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="state",
        geo_layer_filters=["SC", "WA"],
        spending_type="face_value_of_loan",
    )
    expected_response = {
        "geo_layer": "state",
        "spending_type": "face_value_of_loan",
        "results": [
            {
                "amount": 330.0,
                "display_name": "South Carolina",
                "per_capita": 0.33,
                "population": 1000,
                "shape_code": "SC",
            },
            {"amount": 0.0, "display_name": "Washington", "per_capita": 0.0, "population": 10000, "shape_code": "WA"},
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_of_empty_list_for_county,
        _test_correct_response_of_empty_list_for_district,
        _test_correct_response_of_empty_list_for_state,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_of_empty_list_for_county(client):
    resp = post(
        client, def_codes=["N"], geo_layer="county", geo_layer_filters=["45001", "45005"], spending_type="obligation"
    )
    expected_response = {
        "geo_layer": "county",
        "spending_type": "obligation",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def _test_correct_response_of_empty_list_for_district(client):
    resp = post(
        client,
        def_codes=["N"],
        geo_layer="district",
        geo_layer_filters=["4510", "4550", "5350"],
        spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "district",
        "spending_type": "obligation",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def _test_correct_response_of_empty_list_for_state(client):
    resp = post(client, def_codes=["N"], geo_layer="state", geo_layer_filters=["WA"], spending_type="obligation")
    expected_response = {
        "geo_layer": "state",
        "spending_type": "obligation",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
