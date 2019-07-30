import json
import pytest

from rest_framework import status
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.tests.test_mock_data_search import all_filters

from django_mock_queries.query import MockModel


@pytest.mark.django_db
def test_spending_by_award_type_success(client, refresh_matviews):

    # test small request
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps({"fields": ["Award ID", "Recipient Name"], "filters": {"award_type_codes": ["A", "B", "C"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test IDV award types
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Award ID", "Recipient Name"],
                "filters": {
                    "award_type_codes": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"]
                },
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all features
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"fields": ["Award ID", "Recipient Name"], "filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test subawards
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"fields": ["Sub-Award ID"], "filters": all_filters(), "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_type_failure(client, refresh_matviews):

    # test incomplete IDV award types
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Award ID", "Recipient Name"],
                "filters": {"award_type_codes": ["IDV_A", "IDV_B_A", "IDV_C", "IDV_D", "IDV_A_A"]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    # test bad autocomplete request for budget function
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps({"filters": {}})
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_spending_by_award_pop_zip_filter(client, mock_matviews_qs):
    """ Test that filtering by pop zips works"""
    mock_model_1 = MockModel(
        pop_zip5="00501",
        pop_country_code="USA",
        award_id=1,
        piid=None,
        fain="abc",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    mock_model_2 = MockModel(
        pop_zip5="00502",
        pop_country_code="USA",
        award_id=2,
        piid=None,
        fain="abd",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    mock_model_3 = MockModel(
        pop_zip5="00503",
        pop_country_code="USA",
        award_id=3,
        piid=None,
        fain="abe",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    # test simple, single zip
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "place_of_performance_locations": [{"country": "USA", "zip": "00501"}],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {"internal_id": 1, "Place of Performance Zip5": "00501"}

    # test that adding a zip that has no results doesn't remove the results from the first zip
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "place_of_performance_locations": [
                        {"country": "USA", "zip": "00501"},
                        {"country": "USA", "zip": "10000"},
                    ],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {"internal_id": 1, "Place of Performance Zip5": "00501"}

    # test that we get 2 results with 2 valid zips
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "place_of_performance_locations": [
                        {"country": "USA", "zip": "00501"},
                        {"country": "USA", "zip": "00502"},
                    ],
                },
            }
        ),
    )
    possible_results = (
        {"internal_id": 1, "Place of Performance Zip5": "00501"},
        {"internal_id": 2, "Place of Performance Zip5": "00502"},
    )
    assert len(resp.data["results"]) == 2
    assert resp.data["results"][0] in possible_results
    assert resp.data["results"][1] in possible_results
    # Just to make sure it isn't returning the same thing twice somehow
    assert resp.data["results"][0] != resp.data["results"][1]


@pytest.mark.django_db
def test_spending_by_award_recipient_zip_filter(client, mock_matviews_qs):
    """ Test that filtering by recipient zips works"""
    mock_model_1 = MockModel(
        recipient_location_zip5="00501",
        recipient_location_country_code="USA",
        pop_zip5="00001",
        award_id=1,
        piid=None,
        fain="abc",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    mock_model_2 = MockModel(
        recipient_location_zip5="00502",
        recipient_location_country_code="USA",
        pop_zip5="00002",
        award_id=2,
        piid=None,
        fain="abd",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    mock_model_3 = MockModel(
        recipient_location_zip5="00503",
        recipient_location_country_code="USA",
        pop_zip5="00003",
        award_id=3,
        piid=None,
        fain="abe",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    # test simple, single zip
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [{"country": "USA", "zip": "00501"}],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {"internal_id": 1, "Place of Performance Zip5": "00001"}

    # test that adding a zip that has no results doesn't remove the results from the first zip
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [{"country": "USA", "zip": "00501"}, {"country": "USA", "zip": "10000"}],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {"internal_id": 1, "Place of Performance Zip5": "00001"}

    # test that we get 2 results with 2 valid zips
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [{"country": "USA", "zip": "00501"}, {"country": "USA", "zip": "00502"}],
                },
            }
        ),
    )
    possible_results = (
        {"internal_id": 1, "Place of Performance Zip5": "00001"},
        {"internal_id": 2, "Place of Performance Zip5": "00002"},
    )
    assert len(resp.data["results"]) == 2
    assert resp.data["results"][0] in possible_results
    assert resp.data["results"][1] in possible_results
    # Just to make sure it isn't returning the same thing twice somehow
    assert resp.data["results"][0] != resp.data["results"][1]


@pytest.mark.django_db
def test_spending_by_award_both_zip_filter(client, mock_matviews_qs):
    """ Test that filtering by both kinds of zips works"""
    mock_model_1 = MockModel(
        recipient_location_zip5="00501",
        recipient_location_country_code="USA",
        pop_zip5="00001",
        pop_country_code="USA",
        award_id=1,
        piid=None,
        fain="abc",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    mock_model_2 = MockModel(
        recipient_location_zip5="00502",
        recipient_location_country_code="USA",
        pop_zip5="00002",
        pop_country_code="USA",
        award_id=2,
        piid=None,
        fain="abd",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    mock_model_3 = MockModel(
        recipient_location_zip5="00503",
        recipient_location_country_code="USA",
        pop_zip5="00003",
        pop_country_code="USA",
        award_id=3,
        piid=None,
        fain="abe",
        uri=None,
        type="B",
        pulled_from="AWARD",
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])

    # test simple, single pair of zips that both match
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [{"country": "USA", "zip": "00501"}],
                    "place_of_performance_locations": [{"country": "USA", "zip": "00001"}],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {"internal_id": 1, "Place of Performance Zip5": "00001"}

    # test simple, single pair of zips that don't match
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [{"country": "USA", "zip": "00501"}],
                    "place_of_performance_locations": [{"country": "USA", "zip": "00002"}],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 0

    # test 2 pairs (only one pair can be made from this)
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [{"country": "USA", "zip": "00501"}, {"country": "USA", "zip": "00502"}],
                    "place_of_performance_locations": [
                        {"country": "USA", "zip": "00001"},
                        {"country": "USA", "zip": "00003"},
                    ],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {"internal_id": 1, "Place of Performance Zip5": "00001"}


@pytest.mark.django_db
def test_spending_by_award_foreign_filter(client, mock_matviews_qs):
    """ Verify that foreign country filter is returning the correct results """
    mock_model_0 = MockModel(
        award_id=0,
        piid=None,
        fain="aaa",
        uri=None,
        type="B",
        pulled_from="AWARD",
        recipient_location_country_name="UNITED STATES",
        recipient_location_country_code="USA",
    )
    mock_model_1 = MockModel(
        award_id=1,
        piid=None,
        fain="abc",
        uri=None,
        type="B",
        pulled_from="AWARD",
        recipient_location_country_name="",
        recipient_location_country_code="USA",
    )
    mock_model_2 = MockModel(
        award_id=2,
        piid=None,
        fain="abd",
        uri=None,
        type="B",
        pulled_from="AWARD",
        recipient_location_country_name="UNITED STATES",
        recipient_location_country_code="",
    )
    mock_model_3 = MockModel(
        award_id=3,
        piid=None,
        fain="abe",
        uri=None,
        type="B",
        pulled_from="AWARD",
        recipient_location_country_name="Gibraltar",
        recipient_location_country_code="GIB",
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_0, mock_model_1, mock_model_2, mock_model_3])
    # add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_3])

    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    # "recipient_locations": [{"country": "USA"}]
                    "recipient_scope": "domestic",
                },
                "fields": ["Award ID"],
            }
        ),
    )
    # Three results are returned when searching for "USA"-based recipients
    # e.g. "USA"; "UNITED STATES"; "USA" and "UNITED STATES";
    assert len(resp.data["results"]) == 3

    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": ["A", "B", "C", "D"], "recipient_scope": "foreign"},
                "fields": ["Award ID"],
            }
        ),
    )
    # One result is returned when searching for "Foreign" recipients
    assert len(resp.data["results"]) == 1
