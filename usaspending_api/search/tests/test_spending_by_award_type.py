import json
import pytest

from django.db import connection
from model_mommy import mommy
from rest_framework import status
from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.fixture
@pytest.mark.django_db
def test_data():
    mommy.make("references.LegalEntity", legal_entity_id=1)

    mommy.make(
        "awards.Award", id=1, type="A", recipient_id=1, latest_transaction_id=1, generated_unique_award_id="CONT_AWD_1"
    )
    mommy.make("awards.TransactionNormalized", id=1, action_date="2010-10-01", award_id=1, is_fpds=True)
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        legal_entity_zip5="00501",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
        place_of_performance_zip5="00001",
    )

    mommy.make(
        "awards.Award", id=2, type="A", recipient_id=1, latest_transaction_id=2, generated_unique_award_id="CONT_AWD_2"
    )
    mommy.make("awards.TransactionNormalized", id=2, action_date="2010-10-01", award_id=2, is_fpds=True)
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        legal_entity_zip5="00502",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
        place_of_performance_zip5="00002",
    )

    mommy.make(
        "awards.Award", id=3, type="A", recipient_id=1, latest_transaction_id=3, generated_unique_award_id="CONT_AWD_3"
    )
    mommy.make("awards.TransactionNormalized", id=3, action_date="2010-10-01", award_id=3, is_fpds=True)
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        legal_entity_country_code="USA",
        legal_entity_country_name="UNITED STATES",
        legal_entity_zip5="00503",
        place_of_perform_country_c="USA",
        place_of_perform_country_n="UNITED STATES",
        place_of_performance_zip5="00003",
    )

    mommy.make(
        "awards.Award", id=4, type="A", recipient_id=1, latest_transaction_id=4, generated_unique_award_id="CONT_AWD_4"
    )
    mommy.make("awards.TransactionNormalized", id=4, action_date="2010-10-01", award_id=4, is_fpds=True)
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        legal_entity_country_code="GIB",
        legal_entity_country_name="GIBRALTAR",
        legal_entity_zip5="00504",
        place_of_perform_country_c="GIB",
        place_of_perform_country_n="GIBRALTAR",
        place_of_performance_zip5="00004",
    )

    with connection.cursor() as cursor:
        cursor.execute("refresh materialized view concurrently mv_contract_award_search")


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
def test_spending_by_award_pop_zip_filter(client, test_data):
    """ Test that filtering by pop zips works"""

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
def test_spending_by_award_recipient_zip_filter(client, test_data):
    """ Test that filtering by recipient zips works"""

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
def test_spending_by_award_both_zip_filter(client, test_data):
    """ Test that filtering by both kinds of zips works"""

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
def test_spending_by_award_foreign_filter(client, test_data):
    """ Verify that foreign country filter is returning the correct results """

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
def test_spending_by_subaward_type_success(client, refresh_matviews):
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
