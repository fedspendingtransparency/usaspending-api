import json

import pytest
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

spending_level_deprecation_message = (
    "The 'subawards' field will be deprecated in the future. "
    "Set 'spending_level' to 'subawards' instead. See documentation for more information."
)


@pytest.mark.django_db
def test_spending_by_geography_failure(client, monkeypatch, elasticsearch_transaction_index):
    """Verify error on bad autocomplete request for budget function."""

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_geography/",
        content_type="application/json",
        data=json.dumps({"scope": "test", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_spending_by_geography_subawards_success(client):

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
                "subawards": True,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_spending_level_subawards_success(client):

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
                "spending_level": "subawards",
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_subawards_failure(client):

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
                "subawards": "string",
            }
        ),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_geography_spending_level_failure(client):

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
                "spending_level": "invalid",
            }
        ),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_geography_subawards_legacy_param(client):

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
                "subawards": True,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["spending_level"] == "subawards"


def _get_shape_code_for_sort(result_dict):
    return result_dict["shape_code"]


def test_success_with_all_filters(
    client, monkeypatch, elasticsearch_subaward_index, elasticsearch_transaction_index, awards_and_transactions
):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_cases = [
        _test_success_with_all_filters_place_of_performance_county,
        _test_success_with_all_filters_place_of_performance_district,
        _test_success_with_all_filters_place_of_performance_state,
        _test_success_with_all_filters_recipient_location_county,
        _test_success_with_all_filters_recipient_location_district,
        _test_success_with_all_filters_recipient_location_state,
    ]

    for test in test_cases:
        test(client)


def _test_success_with_all_filters_place_of_performance_county(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "place_of_performance", "geo_layer": "county", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_place_of_performance_district(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "place_of_performance", "geo_layer": "district", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_place_of_performance_state(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "place_of_performance", "geo_layer": "state", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_recipient_location_county(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "recipient_location", "geo_layer": "county", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_recipient_location_district(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "recipient_location", "geo_layer": "district", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_recipient_location_state(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "recipient_location", "geo_layer": "state", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response_with_geo_filters(
    client, monkeypatch, elasticsearch_subaward_index, elasticsearch_transaction_index, awards_and_transactions
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_cases = [
        _test_correct_response_for_place_of_performance_county_with_geo_filters,
        _test_correct_response_for_place_of_performance_district_with_geo_filters,
        _test_correct_response_for_place_of_performance_state_with_geo_filters,
        _test_correct_response_for_place_of_performance_country_with_geo_filters,
        _test_correct_response_for_recipient_location_county_with_geo_filters,
        _test_correct_response_for_recipient_location_district_with_geo_filters,
        _test_correct_response_for_recipient_location_state_with_geo_filters,
        _test_correct_response_for_recipient_location_country_with_geo_filters,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_place_of_performance_county_with_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "geo_layer_filters": ["45001", "53005"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 550005.0,
                "display_name": "Charleston",
                "per_capita": 550005.0,
                "population": 1,
                "shape_code": "45001",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "Test Name",
                "per_capita": 55.0,
                "population": 100,
                "shape_code": "53005",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_district_with_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "district",
                "geo_layer_filters": ["5351", "4551"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "district",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 50.0,
                "display_name": "SC-51",
                "per_capita": 0.5,
                "population": 100,
                "shape_code": "4551",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "WA-51",
                "per_capita": 2.75,
                "population": 2000,
                "shape_code": "5351",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_state_with_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "geo_layer_filters": ["SC"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 550055.0,
                "display_name": "South Carolina",
                "per_capita": 550.06,
                "population": 1000,
                "shape_code": "SC",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_country_with_geo_filters(client):
    # Prime awards

    # Get only foreign country results
    # (USA results should be excluded since `place_of_performance_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "geo_layer_filters": ["CAN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "place_of_performance_scope": "foreign",
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (CAN results should be excluded since `place_of_performance_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "geo_layer_filters": ["CAN", "USA"],
                "filters": {
                    "place_of_performance_scope": "domestic",
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5555555.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            }
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign results
    # (USA and CAN should both be in the results since `place_of_performance_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "geo_layer_filters": ["CAN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
            {
                "aggregated_amount": 5555555.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Subawards

    # Get only foreign country results
    # (USA results should be excluded since `place_of_performance_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": "subawards",
                "geo_layer_filters": ["CAN", "USA"],
                "filters": {
                    "place_of_performance_scope": "foreign",
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 12345.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (CAN results should be excluded since `place_of_performance_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": "subawards",
                "geo_layer_filters": ["CAN", "USA"],
                "filters": {
                    "place_of_performance_scope": "domestic",
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 733231.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign results
    # (USA and CAN should both be in the results since `place_of_performance_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": "subawards",
                "geo_layer_filters": ["CAN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 12345.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
            {
                "aggregated_amount": 733231.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_county_with_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["45005", "45001"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000550.0,
                "display_name": "Charleston",
                "per_capita": 5000550.0,
                "population": 1,
                "shape_code": "45001",
            },
            {
                "aggregated_amount": 500000.0,
                "display_name": "Test Name",
                "per_capita": 50000.0,
                "population": 10,
                "shape_code": "45005",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_district_with_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "district",
                "geo_layer_filters": ["4511", "4551", "5351"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "district",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "SC-11",
                "per_capita": 500000.0,
                "population": 10,
                "shape_code": "4511",
            },
            {
                "aggregated_amount": 500500.0,
                "display_name": "SC-51",
                "per_capita": 5005.0,
                "population": 100,
                "shape_code": "4551",
            },
            {
                "aggregated_amount": 55000.0,
                "display_name": "WA-51",
                "per_capita": 27.5,
                "population": 2000,
                "shape_code": "5351",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_state_with_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "state",
                "geo_layer_filters": ["WA"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 55000.0,
                "display_name": "Washington",
                "per_capita": 5.5,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_country_with_geo_filters(client):
    # Prime awards

    # Get only foreign country results
    # (USA results should be excluded since `recipient_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "geo_layer_filters": ["JPN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "recipient_scope": "foreign",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (JPN results should be excluded since `recipient_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "geo_layer_filters": ["JPN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "recipient_scope": "domestic",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5555550.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign country results
    # (USA and JPN should both be in the results since `recipient_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "geo_layer_filters": ["JPN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
            {
                "aggregated_amount": 5555550.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Subawards

    # Get only foreign country results
    # (USA results should be excluded since `recipient_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "spending_level": "subawards",
                "geo_layer_filters": ["JPN", "USA"],
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                    "recipient_scope": "foreign",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 678910.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (JPN results should be excluded since `recipient_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "geo_layer_filters": ["JPN", "USA"],
                "spending_level": "subawards",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "recipient_scope": "domestic",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 66666.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign country results
    # (USA and JPN should both be in the results since `recipient_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "geo_layer_filters": ["JPN", "USA"],
                "spending_level": "subawards",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 678910.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
            {
                "aggregated_amount": 66666.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def test_correct_response_without_geo_filters(
    client, monkeypatch, elasticsearch_subaward_index, elasticsearch_transaction_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    test_cases = [
        _test_correct_response_for_place_of_performance_county_without_geo_filters,
        _test_correct_response_for_place_of_performance_district_without_geo_filters,
        _test_correct_response_for_place_of_performance_state_without_geo_filters,
        _test_correct_response_for_place_of_performance_country_without_geo_filters,
        _test_correct_response_for_recipient_location_county_without_geo_filters,
        _test_correct_response_for_recipient_location_district_without_geo_filters,
        _test_correct_response_for_recipient_location_state_without_geo_filters,
        _test_correct_response_for_recipient_location_country_without_geo_filters,
        _test_correct_response_for_place_of_performance_state_without_country_code,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_place_of_performance_county_without_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 550005.0,
                "display_name": "Charleston",
                "per_capita": 550005.0,
                "population": 1,
                "shape_code": "45001",
            },
            {
                "aggregated_amount": 50.0,
                "display_name": "Test Name",
                "per_capita": 5.0,
                "population": 10,
                "shape_code": "45005",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "Test Name",
                "per_capita": 55.0,
                "population": 100,
                "shape_code": "53005",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_district_without_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "district",
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "district",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 500000.0,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": "",
            },
            {
                "aggregated_amount": 50005.0,
                "display_name": "SC-10",
                "per_capita": 5000.5,
                "population": 10,
                "shape_code": "4510",
            },
            {
                "aggregated_amount": 50.0,
                "display_name": "SC-51",
                "per_capita": 0.5,
                "population": 100,
                "shape_code": "4551",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "WA-51",
                "per_capita": 2.75,
                "population": 2000,
                "shape_code": "5351",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_state_without_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 10.0,
                "display_name": "North Dakota",
                "per_capita": 1.0,
                "population": 10,
                "shape_code": "ND",
            },
            {
                "aggregated_amount": 550055.0,
                "display_name": "South Carolina",
                "per_capita": 550.06,
                "population": 1000,
                "shape_code": "SC",
            },
            {
                "aggregated_amount": 20.0,
                "display_name": "Texas",
                "per_capita": 0.2,
                "population": 100,
                "shape_code": "TX",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "Washington",
                "per_capita": 0.55,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_country_without_geo_filters(client):
    # Get only foreign country results
    # (USA results should be excluded since `place_of_performance_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "place_of_performance_scope": "foreign",
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (CAN results should be excluded since `place_of_performance_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "filters": {
                    "place_of_performance_scope": "domestic",
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5555555.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            }
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign results
    # (USA and CAN should both be in the results since `place_of_performance_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
            {
                "aggregated_amount": 5555555.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Subawards

    # Get only foreign country results
    # (USA results should be excluded since `place_of_performance_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": "subawards",
                "filters": {
                    "place_of_performance_scope": "foreign",
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 12345.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (CAN results should be excluded since `place_of_performance_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": "subawards",
                "filters": {
                    "place_of_performance_scope": "domestic",
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 733231.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign results
    # (USA and CAN should both be in the results since `place_of_performance_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": "subawards",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 12345.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
            {
                "aggregated_amount": 733231.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_place_of_performance_state_without_country_code(client):
    # Ensure that ALL domestic ES records are returned for a given state/territory even if
    #   there's no pop_country_code on that record. Any pop_state_code is enough to know
    #   that the record is a domestic record.

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "filters": {
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2022-09-30"}],
                },
                "subawards": False,
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 10.0,
                "display_name": "North Dakota",
                "per_capita": 1.0,
                "population": 10,
                "shape_code": "ND",
            },
            {
                "aggregated_amount": 550055.0,
                "display_name": "South Carolina",
                "per_capita": 550.06,
                "population": 1000,
                "shape_code": "SC",
            },
            {
                "aggregated_amount": 20.0,
                "display_name": "Texas",
                "per_capita": 0.2,
                "population": 100,
                "shape_code": "TX",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "Washington",
                "per_capita": 0.55,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_county_without_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5000550.0,
                "display_name": "Charleston",
                "per_capita": 5000550.0,
                "population": 1,
                "shape_code": "45001",
            },
            {
                "aggregated_amount": 500000.0,
                "display_name": "Test Name",
                "per_capita": 50000.0,
                "population": 10,
                "shape_code": "45005",
            },
            {
                "aggregated_amount": 55000.0,
                "display_name": "Test Name",
                "per_capita": 550.0,
                "population": 100,
                "shape_code": "53005",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_district_without_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "district",
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "district",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 50.0,
                "display_name": None,
                "per_capita": None,
                "population": None,
                "shape_code": "",
            },
            {
                "aggregated_amount": 5000000.0,
                "display_name": "SC-11",
                "per_capita": 500000.0,
                "population": 10,
                "shape_code": "4511",
            },
            {
                "aggregated_amount": 500500.0,
                "display_name": "SC-51",
                "per_capita": 5005.0,
                "population": 100,
                "shape_code": "4551",
            },
            {
                "aggregated_amount": 55000.0,
                "display_name": "WA-51",
                "per_capita": 27.5,
                "population": 2000,
                "shape_code": "5351",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_state_without_geo_filters(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "state",
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5500550.0,
                "display_name": "South Carolina",
                "per_capita": 5500.55,
                "population": 1000,
                "shape_code": "SC",
            },
            {
                "aggregated_amount": 55000.0,
                "display_name": "Washington",
                "per_capita": 5.5,
                "population": 10000,
                "shape_code": "WA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def _test_correct_response_for_recipient_location_country_without_geo_filters(client):
    # Prime awards

    # Get only foreign country results
    # (USA results should be excluded since `recipient_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "recipient_scope": "foreign",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (CAN and JPN results should be excluded since `recipient_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "recipient_scope": "domestic",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5555550.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign country results
    # (USA, CAN and JPN should all be in the results since `recipient_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5.0,
                "display_name": "Canada",
                "per_capita": None,
                "population": None,
                "shape_code": "CAN",
            },
            {
                "aggregated_amount": 5000000.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
            {
                "aggregated_amount": 5555550.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Subawards

    # Get only foreign country results
    # (USA results should be excluded since `recipient_scope` is set to "foreign")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "spending_level": "subawards",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2022-09-30"}],
                    "recipient_scope": "foreign",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 678910.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get only domestic results
    # (JPN results should be excluded since `recipient_scope` is set to "domestic")
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "spending_level": "subawards",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                    "recipient_scope": "domestic",
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 66666.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Get both domestic and foreign country results
    # (USA and JPN should both be in the results since `recipient_scope` is excluded)
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "country",
                "spending_level": "subawards",
                "filters": {
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "country",
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 678910.0,
                "display_name": "Japan",
                "per_capita": None,
                "population": None,
                "shape_code": "JPN",
            },
            {
                "aggregated_amount": 66666.0,
                "display_name": "United States",
                "per_capita": None,
                "population": None,
                "shape_code": "USA",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Place of Performance - County
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Place of Performance - District
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "district",
                "filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "district",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Place of Performance - State
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Recipient Location - County
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Recipient Location - District
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "district",
                "filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "district",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Recipient Location - State
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "state",
                "filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "state",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_with_date_type(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Empty response
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2019-12-30", "end_date": "2020-01-02"}]
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2019-12-30", "end_date": "2020-01-15"}]
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5.0,
                "display_name": "Charleston",
                "per_capita": 5.0,
                "population": 1,
                "shape_code": "45001",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def test_correct_response_new_awards_only(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Test case when action date out of bounds but date signed in bounds
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-02", "end_date": "2020-01-15"}
                    ]
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response

    # Test case when action date and date signed in bounds
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2019-12-30", "end_date": "2020-01-15"}
                    ]
                },
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "spending_level": "transactions",
        "results": [
            {
                "aggregated_amount": 5.0,
                "display_name": "Charleston",
                "per_capita": 5.0,
                "population": 1,
                "shape_code": "45001",
            },
        ],
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


@pytest.mark.django_db
def test_spending_by_geo_program_activity_subawards(
    client, monkeypatch, elasticsearch_subaward_index, elasticsearch_award_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Program Activites filter test
    test_payload = {
        "spending_level": "subawards",
        "scope": "recipient_location",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {"program_activities": [{"name": "program_activity_123"}]},
    }
    expected_response = [
        {
            "aggregated_amount": 54321.00,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "subawards",
        "scope": "recipient_location",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {"program_activities": [{"name": "program_activity_1234"}]},
    }
    expected_response = []

    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "subawards",
        "scope": "recipient_location",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
        },
    }
    expected_response = [
        {
            "aggregated_amount": 54321.00,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "subawards",
        "scope": "recipient_location",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {
            "program_activities": [{"name": "program_activity_12345"}],
        },
    }
    expected_response = []

    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_geo_program_activity_name_case(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Program Activites filter test
    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "filters": {
            "program_activities": [{"name": "ProGram_aCtiVity_123"}],
        },
    }
    expected_response = [
        {
            "aggregated_amount": 5.0,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_geo_program_activity(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Program Activites filter test
    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
        },
    }
    expected_response = [
        {
            "aggregated_amount": 5.0,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {"program_activities": [{"name": "program_activity_1234"}]},
    }
    expected_response = []

    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
        },
    }
    expected_response = [
        {
            "aggregated_amount": 5.0,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {
            "program_activities": [{"name": "program_activity_12345"}],
        },
    }
    expected_response = []

    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # Testing Program Activity Code
    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {
            "program_activities": [{"code": "123"}],
        },
    }
    expected_response = [
        {
            "aggregated_amount": 5.0,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "subawards": False,
        "scope": "place_of_performance",
        "geo_layer": "country",
        "geo_layer_filters": ["USA"],
        "filters": {
            "program_activities": [{"code": "0123"}],
        },
    }
    expected_response = [
        {
            "aggregated_amount": 5.0,
            "display_name": "United States",
            "population": None,
            "per_capita": None,
            "shape_code": "USA",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_geography", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


spending_level_test_params = [
    (
        "awards",
        [
            {
                "shape_code": "CAN",
                "display_name": "Test Canada",
                "aggregated_amount": 600.0,
                "total_outlays": 1000.0,
                "population": 200,
                "per_capita": 3.0,
            },
            {
                "shape_code": "USA",
                "display_name": "Test United States",
                "aggregated_amount": 1000.0,
                "total_outlays": 1200.0,
                "population": 500,
                "per_capita": 2.0,
            },
        ],
    ),
    (
        "subawards",
        [
            {
                "shape_code": "CAN",
                "display_name": "Test Canada",
                "aggregated_amount": 400.0,
                "population": 200,
                "per_capita": 2.0,
            },
            {
                "shape_code": "USA",
                "display_name": "Test United States",
                "aggregated_amount": 1500.0,
                "population": 500,
                "per_capita": 3.0,
            },
        ],
    ),
    (
        "transactions",
        [
            {
                "shape_code": "CAN",
                "display_name": "Test Canada",
                "aggregated_amount": 500.0,
                "population": 200,
                "per_capita": 2.5,
            },
            {
                "shape_code": "USA",
                "display_name": "Test United States",
                "aggregated_amount": 500.0,
                "population": 500,
                "per_capita": 1.0,
            },
        ],
    ),
]


@pytest.mark.django_db
@pytest.mark.parametrize("spending_level,expected_results", spending_level_test_params)
def test_correct_response_with_spending_level(
    spending_level,
    expected_results,
    client,
    monkeypatch,
    elasticsearch_award_index,
    elasticsearch_subaward_index,
    elasticsearch_transaction_index,
    spending_level_test_data,
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "country",
                "spending_level": spending_level,
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "country",
        "spending_level": spending_level,
        "results": expected_results,
        "messages": [get_time_period_message(), spending_level_deprecation_message],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response
