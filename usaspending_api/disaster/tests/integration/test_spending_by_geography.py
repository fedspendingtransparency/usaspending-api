import json
import pytest

from rest_framework import status

from usaspending_api.awards.v2.lookups.lookups import grant_type_mapping, contract_type_mapping, loan_type_mapping
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def _get_shape_code_for_sort(result_dict):
    return result_dict["shape_code"]


def _get_amount_for_sort(result_dict):
    return result_dict["amount"]


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
    if kwargs.get("scope"):
        request_body["scope"] = kwargs["scope"]

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
    assert resp.data["detail"] == "Field 'geo_layer' is outside valid values ['county', 'district', 'state']"

    # Test invalid "spending_type" string
    resp = post(client, def_codes=["L"], geo_layer="state", geo_layer_filters=["SC-01"], spending_type="NOT VALID")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.data["detail"]
        == "Field 'spending_type' is outside valid values ['obligation', 'outlay', 'face_value_of_loan']"
    )

    # Test invalid "award_type_codes" string
    resp = post(
        client,
        award_type_codes=["NOT VALID"],
        def_codes=["L"],
        geo_layer="state",
        geo_layer_filters=["SC-01"],
        spending_type="obligation",
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert "Field 'filter|award_type_codes' is outside valid values " in resp.data["detail"]

    # Test invalid "scope" string
    resp = post(client, def_codes=["L"], geo_layer="state", spending_type="obligation", scope="NOT VALID")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'scope' is outside valid values ['place_of_performance', 'recipient_location']"


@pytest.mark.django_db
def test_correct_response_with_different_geo_filters(
    client, monkeypatch, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_for_pop_county,
        _test_correct_response_for_pop_district,
        _test_correct_response_for_pop_state,
        _test_correct_response_for_recipient_location_county,
        _test_correct_response_for_recipient_location_district,
        _test_correct_response_for_recipient_location_state,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_pop_county(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="county",
        geo_layer_filters=["45001", "45005"],
        spending_type="obligation",
        scope="place_of_performance",
    )
    expected_response = {
        "geo_layer": "county",
        "scope": "place_of_performance",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2220000.0,
                "award_count": 3,
                "display_name": "Charleston",
                "per_capita": 2220000.0,
                "population": 1,
                "shape_code": "45001",
            },
            {
                "amount": 20.0,
                "award_count": 1,
                "display_name": "Test Name",
                "per_capita": 2.0,
                "population": 10,
                "shape_code": "45005",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_pop_district(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="district",
        geo_layer_filters=["4510", "4501", "4550", "4502", "5350", "5302"],
        spending_type="obligation",
        scope="place_of_performance",
    )
    expected_response = {
        "geo_layer": "district",
        "scope": "place_of_performance",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 20000.0,
                "award_count": 1,
                "display_name": "SC-01",
                "per_capita": None,
                "population": None,
                "shape_code": "4501",
            },
            {
                "amount": 20.0,
                "award_count": 1,
                "display_name": "SC-02",
                "per_capita": 0.2,
                "population": 100,
                "shape_code": "4502",
            },
            {
                "amount": 2200.0,
                "award_count": 2,
                "display_name": "WA-02",
                "per_capita": 2.2,
                "population": 1000,
                "shape_code": "5302",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_pop_state(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="state",
        geo_layer_filters=["WA"],
        spending_type="obligation",
        scope="place_of_performance",
    )
    expected_response = {
        "geo_layer": "state",
        "scope": "place_of_performance",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2200.0,
                "display_name": "Washington",
                "per_capita": 0.22,
                "population": 10000,
                "shape_code": "WA",
                "award_count": 2,
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_county(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="county",
        geo_layer_filters=["45001", "45005"],
        spending_type="obligation",
        scope="recipient_location",
    )
    expected_response = {
        "geo_layer": "county",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2000220.0,
                "display_name": "Charleston",
                "per_capita": 2000220.0,
                "population": 1,
                "shape_code": "45001",
                "award_count": 3,
            },
            {
                "amount": 200000.0,
                "display_name": "Test Name",
                "per_capita": 20000.0,
                "population": 10,
                "shape_code": "45005",
                "award_count": 1,
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_district(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="district",
        geo_layer_filters=["4510", "4501", "4550", "4502", "5350", "5302"],
        spending_type="obligation",
        scope="recipient_location",
    )
    expected_response = {
        "geo_layer": "district",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2000000.0,
                "display_name": "SC-01",
                "per_capita": None,
                "population": None,
                "shape_code": "4501",
                "award_count": 1,
            },
            {
                "amount": 200200.0,
                "display_name": "SC-02",
                "per_capita": 2002.0,
                "population": 100,
                "shape_code": "4502",
                "award_count": 2,
            },
            {
                "amount": 22000.0,
                "display_name": "WA-02",
                "per_capita": 22.0,
                "population": 1000,
                "shape_code": "5302",
                "award_count": 2,
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_state(client):
    resp = post(
        client,
        def_codes=["L", "M"],
        geo_layer="state",
        geo_layer_filters=["WA"],
        spending_type="obligation",
        scope="recipient_location",
    )
    expected_response = {
        "geo_layer": "state",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 22000.0,
                "award_count": 2,
                "display_name": "Washington",
                "per_capita": 2.2,
                "population": 10000,
                "shape_code": "WA",
            }
        ],
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
        client, def_codes=["L", "M"], geo_layer="state", geo_layer_filters=["SC", "WA"], spending_type="obligation"
    )
    expected_response = {
        "geo_layer": "state",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2200220.0,
                "display_name": "South Carolina",
                "per_capita": 2200.22,
                "population": 1000,
                "shape_code": "SC",
                "award_count": 4,
            },
            {
                "amount": 22000.0,
                "display_name": "Washington",
                "per_capita": 2.2,
                "population": 10000,
                "shape_code": "WA",
                "award_count": 2,
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_outlay(client):
    resp = post(client, def_codes=["L", "M"], geo_layer="state", geo_layer_filters=["SC", "WA"], spending_type="outlay")
    expected_response = {
        "geo_layer": "state",
        "scope": "recipient_location",
        "spending_type": "outlay",
        "results": [
            {
                "amount": 1100110.0,
                "display_name": "South Carolina",
                "per_capita": 1100.11,
                "population": 1000,
                "shape_code": "SC",
                "award_count": 4,
            },
            {
                "amount": 11000.0,
                "display_name": "Washington",
                "per_capita": 1.1,
                "population": 10000,
                "shape_code": "WA",
                "award_count": 2,
            },
        ],
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
        "scope": "recipient_location",
        "spending_type": "face_value_of_loan",
        "results": [
            {
                "amount": 330.0,
                "display_name": "South Carolina",
                "per_capita": 0.33,
                "population": 1000,
                "shape_code": "SC",
                "award_count": 4,
            },
            {
                "amount": 0.0,
                "display_name": "Washington",
                "per_capita": 0.0,
                "population": 10000,
                "shape_code": "WA",
                "award_count": 2,
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


@pytest.mark.django_db
def test_correct_response_for_award_type_codes(client, monkeypatch, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_of_loans,
        _test_correct_response_of_contracts,
        _test_correct_response_of_grants,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_of_loans(client):
    resp = post(
        client,
        award_type_codes=list(loan_type_mapping.keys()),
        def_codes=["L", "M"],
        geo_layer="county",
        geo_layer_filters=["45001", "45005"],
        spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "county",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 220.0,
                "display_name": "Charleston",
                "per_capita": 220.0,
                "population": 1,
                "shape_code": "45001",
                "award_count": 2,
            }
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_of_contracts(client):
    resp = post(
        client,
        award_type_codes=list(contract_type_mapping.keys()),
        def_codes=["L", "M"],
        geo_layer="district",
        geo_layer_filters=["4510", "4501", "4550", "4502", "5350", "5302"],
        spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "district",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2000000.0,
                "display_name": "SC-01",
                "per_capita": None,
                "population": None,
                "shape_code": "4501",
                "award_count": 1,
            },
            {
                "amount": 200000.0,
                "display_name": "SC-02",
                "per_capita": 2000.0,
                "population": 100,
                "shape_code": "4502",
                "award_count": 1,
            },
            {
                "amount": 22000.0,
                "display_name": "WA-02",
                "per_capita": 22.0,
                "population": 1000,
                "shape_code": "5302",
                "award_count": 2,
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_of_grants(client):
    resp = post(
        client,
        award_type_codes=list(grant_type_mapping.keys()),
        def_codes=["L", "M"],
        geo_layer="state",
        geo_layer_filters=["SC", "WA"],
        spending_type="obligation",
    )
    expected_response = {
        "geo_layer": "state",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


@pytest.mark.django_db
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
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [],
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
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def _test_correct_response_of_empty_list_for_state(client):
    resp = post(client, def_codes=["N"], geo_layer="state", geo_layer_filters=["WA"], spending_type="obligation")
    expected_response = {
        "geo_layer": "state",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


@pytest.mark.django_db
def test_correct_response_without_geo_filters(client, monkeypatch, elasticsearch_award_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_for_pop_county_without_geo_filters,
        _test_correct_response_for_pop_district_without_geo_filters,
        _test_correct_response_for_pop_state_without_geo_filters,
        _test_correct_response_for_recipient_location_county_without_geo_filters,
        _test_correct_response_for_recipient_location_district_without_geo_filters,
        _test_correct_response_for_recipient_location_state_without_geo_filters,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_pop_county_without_geo_filters(client):
    resp = post(
        client, def_codes=["L", "M"], geo_layer="county", spending_type="obligation", scope="place_of_performance"
    )
    expected_response = {
        "geo_layer": "county",
        "scope": "place_of_performance",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2.0,
                "award_count": 1,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": None,
            },
            {
                "amount": 20.0,
                "award_count": 1,
                "display_name": "Test Name",
                "per_capita": 2.0,
                "population": 10,
                "shape_code": "45005",
            },
            {
                "amount": 2200.0,
                "award_count": 2,
                "display_name": "Test Name",
                "per_capita": 22.0,
                "population": 100,
                "shape_code": "53005",
            },
            {
                "amount": 2220000.0,
                "award_count": 3,
                "display_name": "Charleston",
                "per_capita": 2220000.0,
                "population": 1,
                "shape_code": "45001",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_amount_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_pop_district_without_geo_filters(client):
    resp = post(
        client, def_codes=["L", "M"], geo_layer="district", spending_type="obligation", scope="place_of_performance"
    )
    expected_response = {
        "geo_layer": "district",
        "scope": "place_of_performance",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2.0,
                "award_count": 1,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": None,
            },
            {
                "amount": 20.0,
                "award_count": 1,
                "display_name": "SC-02",
                "per_capita": 0.2,
                "population": 100,
                "shape_code": "4502",
            },
            {
                "amount": 2200.0,
                "award_count": 2,
                "display_name": "WA-02",
                "per_capita": 2.2,
                "population": 1000,
                "shape_code": "5302",
            },
            {
                "amount": 20000.0,
                "award_count": 1,
                "display_name": "SC-01",
                "per_capita": None,
                "population": None,
                "shape_code": "4501",
            },
            {
                "amount": 2200000.0,
                "award_count": 2,
                "display_name": "SC-03",
                "per_capita": 2200000.0,
                "population": 1,
                "shape_code": "4503",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_amount_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_pop_state_without_geo_filters(client):
    resp = post(
        client, def_codes=["L", "M"], geo_layer="state", spending_type="obligation", scope="place_of_performance"
    )
    expected_response = {
        "geo_layer": "state",
        "scope": "place_of_performance",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2.0,
                "award_count": 1,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": None,
            },
            {
                "amount": 2200.0,
                "award_count": 2,
                "display_name": "Washington",
                "per_capita": 0.22,
                "population": 10000,
                "shape_code": "WA",
            },
            {
                "amount": 2220020.0,
                "award_count": 4,
                "display_name": "South Carolina",
                "per_capita": 2220.02,
                "population": 1000,
                "shape_code": "SC",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_amount_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_county_without_geo_filters(client):
    resp = post(client, def_codes=["L", "M"], geo_layer="county", spending_type="obligation")
    expected_response = {
        "geo_layer": "county",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2.0,
                "award_count": 1,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": None,
            },
            {
                "amount": 22000.0,
                "award_count": 2,
                "display_name": "Test Name",
                "per_capita": 220.0,
                "population": 100,
                "shape_code": "53005",
            },
            {
                "amount": 200000.0,
                "award_count": 1,
                "display_name": "Test Name",
                "per_capita": 20000.0,
                "population": 10,
                "shape_code": "45005",
            },
            {
                "amount": 2000220.0,
                "award_count": 3,
                "display_name": "Charleston",
                "per_capita": 2000220.0,
                "population": 1,
                "shape_code": "45001",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_amount_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_district_without_geo_filters(client):
    resp = post(client, def_codes=["L", "M"], geo_layer="district", spending_type="obligation")
    expected_response = {
        "geo_layer": "district",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2.0,
                "award_count": 1,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": None,
            },
            {
                "amount": 20.0,
                "award_count": 1,
                "display_name": "SC-03",
                "per_capita": 20.0,
                "population": 1,
                "shape_code": "4503",
            },
            {
                "amount": 22000.0,
                "award_count": 2,
                "display_name": "WA-02",
                "per_capita": 22.0,
                "population": 1000,
                "shape_code": "5302",
            },
            {
                "amount": 200200.0,
                "award_count": 2,
                "display_name": "SC-02",
                "per_capita": 2002.0,
                "population": 100,
                "shape_code": "4502",
            },
            {
                "amount": 2000000.0,
                "award_count": 1,
                "display_name": "SC-01",
                "per_capita": None,
                "population": None,
                "shape_code": "4501",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_amount_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_state_without_geo_filters(client):
    resp = post(client, def_codes=["L", "M"], geo_layer="state", spending_type="obligation")
    expected_response = {
        "geo_layer": "state",
        "scope": "recipient_location",
        "spending_type": "obligation",
        "results": [
            {
                "amount": 2.0,
                "award_count": 1,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": None,
            },
            {
                "amount": 22000.0,
                "award_count": 2,
                "display_name": "Washington",
                "per_capita": 2.2,
                "population": 10000,
                "shape_code": "WA",
            },
            {
                "amount": 2200220.0,
                "award_count": 4,
                "display_name": "South Carolina",
                "per_capita": 2200.22,
                "population": 1000,
                "shape_code": "SC",
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_amount_for_sort)
    assert resp_json == expected_response
