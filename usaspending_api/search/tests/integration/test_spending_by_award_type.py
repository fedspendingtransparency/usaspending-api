import json
import pytest

from model_bakery import baker
from rest_framework import status
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
@pytest.mark.django_db
def test_data():

    baker.make(
        "search.AwardSearch",
        award_id=1,
        type="A",
        latest_transaction_id=1,
        generated_unique_award_id="CONT_AWD_1",
        action_date="2010-10-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_zip5="00501",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_zip5="00001",
    )
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_zip5="00501",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_zip5="00001",
    )

    baker.make(
        "search.AwardSearch",
        award_id=2,
        type="A",
        latest_transaction_id=2,
        generated_unique_award_id="CONT_AWD_2",
        action_date="2010-10-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_zip5="00502",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_zip5="00002",
    )
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=2,
        award_id=2,
        action_date="2010-10-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_zip5="00502",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_zip5="00002",
    )

    baker.make(
        "search.AwardSearch",
        award_id=3,
        type="A",
        latest_transaction_id=3,
        generated_unique_award_id="CONT_AWD_3",
        action_date="2010-10-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_zip5="00503",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_zip5="00003",
    )
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=3,
        award_id=3,
        action_date="2010-10-01",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_zip5="00503",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_zip5="00003",
    )

    baker.make(
        "search.AwardSearch",
        award_id=4,
        type="A",
        latest_transaction_id=4,
        generated_unique_award_id="CONT_AWD_4",
        action_date="2010-10-01",
        recipient_location_country_code="GIB",
        recipient_location_country_name="GIBRALTAR",
        recipient_location_zip5="00504",
        pop_country_code="GIB",
        pop_country_name="GIBRALTAR",
        pop_zip5="00004",
    )
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=4,
        award_id=4,
        action_date="2010-10-01",
        recipient_location_country_code="GIB",
        recipient_location_country_name="GIBRALTAR",
        recipient_location_zip5="00504",
        pop_country_code="GIB",
        pop_country_name="GIBRALTAR",
        pop_zip5="00004",
    )

    baker.make("references.RefCountryCode", country_code="GIB", country_name="GIBRALTAR")
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


@pytest.mark.django_db
def test_spending_by_award_type_success(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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
        data=json.dumps({"fields": ["Award ID", "Recipient Name"], "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test subawards
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"fields": ["Sub-Award ID"], "filters": non_legacy_filters(), "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_type_failure(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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
def test_spending_by_award_pop_zip_filter(client, monkeypatch, elasticsearch_award_index, test_data):
    """Test that filtering by pop zips works"""
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # test simple, single zip
    resp = client.post(
        "/api/v2/search/spending_by_award/",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Place of Performance Zip5"],
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "place_of_performance_locations": [{"country": "USA", "zip": "00001"}],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {
        "internal_id": 1,
        "generated_internal_id": "CONT_AWD_1",
        "Place of Performance Zip5": "00001",
    }

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
                        {"country": "USA", "zip": "00001"},
                        {"country": "USA", "zip": "10000"},
                    ],
                },
            }
        ),
    )
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0] == {
        "internal_id": 1,
        "generated_internal_id": "CONT_AWD_1",
        "Place of Performance Zip5": "00001",
    }

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
                        {"country": "USA", "zip": "00001"},
                        {"country": "USA", "zip": "00002"},
                    ],
                },
            }
        ),
    )
    possible_results = (
        {"internal_id": 1, "Place of Performance Zip5": "00001", "generated_internal_id": "CONT_AWD_1"},
        {"internal_id": 2, "Place of Performance Zip5": "00002", "generated_internal_id": "CONT_AWD_2"},
    )
    assert len(resp.data["results"]) == 2
    assert resp.data["results"][0] in possible_results
    assert resp.data["results"][1] in possible_results
    # Just to make sure it isn't returning the same thing twice somehow
    assert resp.data["results"][0] != resp.data["results"][1]


@pytest.mark.django_db
def test_spending_by_award_recipient_zip_filter(client, monkeypatch, elasticsearch_award_index, test_data):
    """Test that filtering by recipient zips works"""
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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
    assert resp.data["results"][0] == {
        "internal_id": 1,
        "Place of Performance Zip5": "00001",
        "generated_internal_id": "CONT_AWD_1",
    }

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
    assert resp.data["results"][0] == {
        "internal_id": 1,
        "Place of Performance Zip5": "00001",
        "generated_internal_id": "CONT_AWD_1",
    }

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
        {"internal_id": 1, "Place of Performance Zip5": "00001", "generated_internal_id": "CONT_AWD_1"},
        {"internal_id": 2, "Place of Performance Zip5": "00002", "generated_internal_id": "CONT_AWD_2"},
    )
    assert len(resp.data["results"]) == 2
    assert resp.data["results"][0] in possible_results
    assert resp.data["results"][1] in possible_results
    # Just to make sure it isn't returning the same thing twice somehow
    assert resp.data["results"][0] != resp.data["results"][1]


@pytest.mark.django_db
def test_spending_by_award_both_zip_filter(client, monkeypatch, elasticsearch_award_index, test_data):
    """Test that filtering by both kinds of zips works"""
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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
    assert resp.data["results"][0] == {
        "internal_id": 1,
        "Place of Performance Zip5": "00001",
        "generated_internal_id": "CONT_AWD_1",
    }

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
    assert resp.data["results"][0] == {
        "internal_id": 1,
        "Place of Performance Zip5": "00001",
        "generated_internal_id": "CONT_AWD_1",
    }


@pytest.mark.django_db
def test_spending_by_award_foreign_filter(client, monkeypatch, elasticsearch_award_index, test_data):
    """Verify that foreign country filter is returning the correct results"""
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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


# test subaward types
@pytest.mark.django_db
def test_spending_by_subaward_type_success(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": ["Sub-Award ID"],
                "filters": {"award_type_codes": ["10", "06", "07", "08", "09", "11"]},
                "subawards": True,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
