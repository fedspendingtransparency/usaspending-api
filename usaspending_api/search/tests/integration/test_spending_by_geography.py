import json
import pytest

from rest_framework import status

from django.conf import settings
from usaspending_api.common.experimental_api_flags import EXPERIMENTAL_API_HEADER, ELASTICSEARCH_HEADER_VALUE
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters


@pytest.mark.django_db
def test_spending_by_geography_state_success(client):
    # test for required filters
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "filters": {"recipient_locations": [{"country": "ABC"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["WA"],
                "filters": non_legacy_filters(),
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_county_success(client):
    # test for required filters
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "county",
                "filters": {"recipient_locations": [{"country": "ABC"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "county",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_congressional_success(client):
    # test for required filters
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "district",
                "filters": {"recipient_locations": [{"country": "ABC"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "district",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_failure(client):
    """Verify error on bad autocomplete request for budget function."""

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
def test_spending_by_geography_incorrect_district(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography/",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "district",
                "geo_layer_filters": ["01"],
                "filters": non_legacy_filters(),
            }
        ),
    )

    assert len(resp.data["results"]) == 0


"""
As of 04/14/2020 these are intended for the experimental Elasticsearch functionality that lives alongside the Postgres
implementation. These tests verify that ES performs as expected, but that it also respects the header put in place
to trigger the experimental functionality. When ES for spending_by_geography is used as the primary implementation for
the endpoint these tests should be updated to reflect the change.
"""


def _get_shape_code_for_sort(result_dict):
    return result_dict["shape_code"]


def _setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements, **options):
    monkeypatch.setattr(
        "usaspending_api.search.v2.views.spending_by_geography.logger.info",
        lambda message: logging_statements.append(message),
    )
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )

    elasticsearch_transaction_index.update_index(**options)


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    logging_statements = []
    _setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

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

    assert len(logging_statements) == len(test_cases), "Elasticsearch was not used for one of the tests"


def _test_success_with_all_filters_place_of_performance_county(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "place_of_performance", "geo_layer": "county", "filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_place_of_performance_district(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "place_of_performance", "geo_layer": "district", "filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_place_of_performance_state(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "place_of_performance", "geo_layer": "state", "filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_recipient_location_county(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "recipient_location", "geo_layer": "county", "filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_recipient_location_district(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "recipient_location", "geo_layer": "district", "filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def _test_success_with_all_filters_recipient_location_state(client):
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps({"scope": "recipient_location", "geo_layer": "state", "filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response_with_geo_filters(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions
):

    logging_statements = []
    _setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

    test_cases = [
        _test_correct_response_for_place_of_performance_county_with_geo_filters,
        _test_correct_response_for_place_of_performance_district_with_geo_filters,
        _test_correct_response_for_place_of_performance_state_with_geo_filters,
        _test_correct_response_for_recipient_location_county_with_geo_filters,
        _test_correct_response_for_recipient_location_district_with_geo_filters,
        _test_correct_response_for_recipient_location_state_with_geo_filters,
    ]

    for test in test_cases:
        test(client)

    assert len(logging_statements) == len(test_cases), "Elasticsearch was not used for one of the tests"


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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
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
        "messages": [get_time_period_message()],
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
                "geo_layer_filters": ["5350", "4550"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "district",
        "results": [
            {
                "aggregated_amount": 50.0,
                "display_name": "SC-50",
                "per_capita": 0.5,
                "population": 100,
                "shape_code": "4550",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "WA-50",
                "per_capita": 5.5,
                "population": 1000,
                "shape_code": "5350",
            },
        ],
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "results": [
            {
                "aggregated_amount": 550055.0,
                "display_name": "South Carolina",
                "per_capita": 550.06,
                "population": 1000,
                "shape_code": "SC",
            },
        ],
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "county",
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
        "messages": [get_time_period_message()],
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
                "geo_layer_filters": ["4510", "4550", "5350"],
                "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]},
            }
        ),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "district",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "SC-10",
                "per_capita": None,
                "population": None,
                "shape_code": "4510",
            },
            {
                "aggregated_amount": 500500.0,
                "display_name": "SC-50",
                "per_capita": 5005.0,
                "population": 100,
                "shape_code": "4550",
            },
            {
                "aggregated_amount": 55000.0,
                "display_name": "WA-50",
                "per_capita": 55.0,
                "population": 1000,
                "shape_code": "5350",
            },
        ],
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "state",
        "results": [
            {
                "aggregated_amount": 55000.0,
                "display_name": "Washington",
                "per_capita": 5.5,
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


def test_correct_response_without_geo_filters(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions
):

    logging_statements = []
    _setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

    test_cases = [
        _test_correct_response_for_place_of_performance_county_without_geo_filters,
        _test_correct_response_for_place_of_performance_district_without_geo_filters,
        _test_correct_response_for_place_of_performance_state_without_geo_filters,
        _test_correct_response_for_recipient_location_county_without_geo_filters,
        _test_correct_response_for_recipient_location_district_without_geo_filters,
        _test_correct_response_for_recipient_location_state_without_geo_filters,
    ]

    for test in test_cases:
        test(client)

    assert len(logging_statements) == len(test_cases), "Elasticsearch was not used for one of the tests"


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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
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
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "district",
        "results": [
            {
                "aggregated_amount": 50005.0,
                "display_name": "SC-10",
                "per_capita": None,
                "population": None,
                "shape_code": "4510",
            },
            {
                "aggregated_amount": 50.0,
                "display_name": "SC-50",
                "per_capita": 0.5,
                "population": 100,
                "shape_code": "4550",
            },
            {
                "aggregated_amount": 500000.0,
                "display_name": "SC-90",
                "per_capita": 500000.0,
                "population": 1,
                "shape_code": "4590",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "WA-50",
                "per_capita": 5.5,
                "population": 1000,
                "shape_code": "5350",
            },
        ],
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "results": [
            {
                "aggregated_amount": 550055.0,
                "display_name": "South Carolina",
                "per_capita": 550.06,
                "population": 1000,
                "shape_code": "SC",
            },
            {
                "aggregated_amount": 5500.0,
                "display_name": "Washington",
                "per_capita": 0.55,
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "county",
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
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "district",
        "results": [
            {
                "aggregated_amount": 5000000.0,
                "display_name": "SC-10",
                "per_capita": None,
                "population": None,
                "shape_code": "4510",
            },
            {
                "aggregated_amount": 500500.0,
                "display_name": "SC-50",
                "per_capita": 5005.0,
                "population": 100,
                "shape_code": "4550",
            },
            {
                "aggregated_amount": 50.0,
                "display_name": "SC-90",
                "per_capita": 50.0,
                "population": 1,
                "shape_code": "4590",
            },
            {
                "aggregated_amount": 55000.0,
                "display_name": "WA-50",
                "per_capita": 55.0,
                "population": 1000,
                "shape_code": "5350",
            },
        ],
        "messages": [get_time_period_message()],
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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "state",
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
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    resp_json = resp.json()
    resp_json["results"].sort(key=_get_shape_code_for_sort)
    assert resp_json == expected_response


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    logging_statements = []
    _setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response

    logging_statements.clear()

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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "district",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response

    logging_statements.clear()

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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response

    logging_statements.clear()

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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "county",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response

    logging_statements.clear()

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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "district",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response

    logging_statements.clear()

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
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "scope": "recipient_location",
        "geo_layer": "state",
        "results": [],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response
