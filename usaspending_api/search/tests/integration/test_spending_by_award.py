import json
import pytest
from datetime import datetime

from django.db import connection
from model_mommy import mommy
from rest_framework import status

from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters, legacy_filters
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings


@pytest.mark.django_db
def test_spending_by_award_subaward_success(client, spending_by_award_test_data, refresh_matviews):

    # Testing all filters
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {"subawards": True, "fields": ["Sub-Award ID"], "sort": "Sub-Award ID", "filters": non_legacy_filters()}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Testing contents of what is returned
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": True,
                "fields": [
                    "Sub-Award ID",
                    "Sub-Awardee Name",
                    "Sub-Award Date",
                    "Sub-Award Amount",
                    "Awarding Agency",
                    "Awarding Sub Agency",
                    "Prime Award ID",
                    "Prime Recipient Name",
                    "recipient_id",
                    "prime_award_recipient_id",
                ],
                "sort": "Sub-Award ID",
                "filters": {"award_type_codes": ["A"]},
                "limit": 2,
                "page": 1,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["page_metadata"]["hasNext"]
    assert resp.json()["limit"] == 2
    assert len(resp.json()["results"]) == 2
    assert resp.json()["results"][0] == {
        "Awarding Agency": "awarding toptier 8006",
        "Awarding Sub Agency": "awarding subtier 8006",
        "Prime Award ID": "PIID6003",
        "Prime Recipient Name": "recipient_name_for_award_1003",
        "Sub-Award Amount": 60000.0,
        "Sub-Award Date": "2019-01-01",
        "Sub-Award ID": "66666",
        "Sub-Awardee Name": "RECIPIENT_NAME_FOR_AWARD_1003",
        "prime_award_internal_id": 3,
        "internal_id": "66666",
        "prime_award_recipient_id": "28aae030-b4b4-4494-8a75-3356208469cf-R",
        "recipient_id": None,
        "prime_award_generated_internal_id": "CONT_AWD_TESTING_3",
    }
    assert resp.json()["results"][1] == {
        "Awarding Agency": "awarding toptier 8003",
        "Awarding Sub Agency": "awarding subtier 8003",
        "Prime Award ID": "PIID3002",
        "Prime Recipient Name": "recipient_name_for_award_1002",
        "Sub-Award Amount": 30000.0,
        "Sub-Award Date": "2016-01-01",
        "Sub-Award ID": "33333",
        "Sub-Awardee Name": "RECIPIENT_NAME_FOR_AWARD_1002",
        "prime_award_internal_id": 2,
        "internal_id": "33333",
        "prime_award_recipient_id": "180bddfc-67f0-42d6-8279-a014d1062d65-R",
        "recipient_id": None,
        "prime_award_generated_internal_id": "CONT_AWD_TESTING_2",
    }


@pytest.mark.django_db
def test_spending_by_award_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": non_legacy_filters()}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_legacy_filters(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_no_intersection(client):

    mommy.make("awards.Award", id=1, type="A", latest_transaction_id=1)
    mommy.make("awards.TransactionNormalized", id=1, action_date="2010-10-01", award_id=1, is_fpds=True)
    mommy.make("awards.TransactionFPDS", transaction_id=1)

    with connection.cursor() as cursor:
        cursor.execute("refresh materialized view concurrently mv_contract_award_search")

    request = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {"award_type_codes": ["A", "B", "C", "D"]},
    }

    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(request))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps(request))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0, "Results returned, there should be 0"


@pytest.fixture
def awards_over_different_date_ranges():
    award_category_list = ["contracts", "direct_payments", "grants", "idvs", "loans", "other_financial_assistance"]

    # The date ranges for the different awards are setup to cover possible intersection points by the
    # different date ranges being searched. The comments on each line specify where the date ranges are
    # suppose to overlap the searched for date ranges. The search for date ranges are:
    #    - {"start_date": "2015-01-01", "end_date": "2015-12-31"}
    #    - {"start_date": "2017-02-01", "end_date": "2017-11-30"}
    date_range_list = [
        # Intersect only one of the date ranges searched for
        {"date_signed": datetime(2014, 1, 1), "action_date": datetime(2014, 5, 1)},  # Before both
        {"date_signed": datetime(2014, 3, 1), "action_date": datetime(2015, 4, 15)},  # Beginning of first
        {"date_signed": datetime(2015, 2, 1), "action_date": datetime(2015, 7, 1)},  # Middle of first
        {"date_signed": datetime(2015, 2, 1), "action_date": datetime(2015, 4, 17)},
        {"date_signed": datetime(2014, 12, 1), "action_date": datetime(2016, 1, 1)},  # All of first
        {"date_signed": datetime(2015, 11, 1), "action_date": datetime(2016, 3, 1)},  # End of first
        {"date_signed": datetime(2016, 2, 23), "action_date": datetime(2016, 7, 19)},  # Between both
        {"date_signed": datetime(2016, 11, 26), "action_date": datetime(2017, 3, 1)},  # Beginning of second
        {"date_signed": datetime(2017, 5, 1), "action_date": datetime(2017, 7, 1)},  # Middle of second
        {"date_signed": datetime(2017, 1, 1), "action_date": datetime(2017, 12, 1)},  # All of second
        {"date_signed": datetime(2017, 9, 1), "action_date": datetime(2017, 12, 17)},  # End of second
        {"date_signed": datetime(2018, 2, 1), "action_date": datetime(2018, 7, 1)},  # After both
        # Intersect both date ranges searched for
        {"date_signed": datetime(2014, 12, 1), "action_date": datetime(2017, 12, 5)},  # Completely both
        {"date_signed": datetime(2015, 7, 1), "action_date": datetime(2017, 5, 1)},  # Partially both
        {"date_signed": datetime(2014, 10, 3), "action_date": datetime(2017, 4, 8)},  # All first; partial second
        {"date_signed": datetime(2015, 8, 1), "action_date": datetime(2018, 1, 2)},  # Partial first; all second
    ]

    award_id = 0

    for award_category in award_category_list:
        for date_range in date_range_list:
            award_id += 1
            guai = "AWARD_{}".format(award_id)
            award_type_list = all_award_types_mappings[award_category]
            award_type = award_type_list[award_id % len(award_type_list)]
            award = mommy.make(
                "awards.Award",
                id=award_id,
                generated_unique_award_id=guai,
                type=award_type,
                category=award_category,
                latest_transaction_id=1000 + award_id,
                date_signed=date_range["date_signed"],
                piid="abcdefg{}".format(award_id),
                fain="xyz{}".format(award_id),
                uri="abcxyx{}".format(award_id),
            )
            mommy.make(
                "awards.TransactionNormalized", id=1000 + award_id, award=award, action_date=date_range["action_date"]
            )


@pytest.mark.django_db
def test_date_range_search_with_one_range(client, awards_over_different_date_ranges, refresh_matviews):
    contract_type_list = all_award_types_mappings["contracts"]
    grants_type_list = all_award_types_mappings["grants"]

    # Test with contracts
    request_with_contracts = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [{"start_date": "2015-01-01", "end_date": "2015-12-31"}],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_with_contracts)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 9

    # Test with grants
    request_with_grants = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [{"start_date": "2017-02-01", "end_date": "2017-11-30"}],
            "award_type_codes": grants_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_with_grants)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 8

    # Test with only one specific award showing
    request_for_one_award = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [{"start_date": "2014-01-03", "end_date": "2014-01-08"}],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_one_award)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"] == [{"Award ID": "abcdefg1", "internal_id": 1, "generated_internal_id": "AWARD_1"}]

    # Test with no award showing
    request_for_no_awards = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [{"start_date": "2013-01-03", "end_date": "2013-01-08"}],
            "award_type_codes": grants_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_no_awards)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0


@pytest.mark.django_db
def test_date_range_search_with_two_ranges(client, awards_over_different_date_ranges, refresh_matviews):
    contract_type_list = all_award_types_mappings["contracts"]
    grants_type_list = all_award_types_mappings["grants"]

    # Test with contracts
    request_with_contracts = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31"},
                {"start_date": "2017-02-01", "end_date": "2017-11-30"},
            ],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_with_contracts)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 13

    # Test with grants
    request_with_grants = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31"},
                {"start_date": "2017-02-01", "end_date": "2017-11-30"},
            ],
            "award_type_codes": grants_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_with_grants)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 13

    # Test with two specific awards showing
    request_for_two_awards = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2014-01-03", "end_date": "2014-01-08"},
                {"start_date": "2018-06-01", "end_date": "2018-06-23"},
            ],
            "award_type_codes": grants_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_two_awards)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2
    assert resp.data["results"] == [
        {"Award ID": "xyz44", "internal_id": 44, "generated_internal_id": "AWARD_44"},
        {"Award ID": "xyz33", "internal_id": 33, "generated_internal_id": "AWARD_33"},
    ]

    # Test with no award showing
    request_for_no_awards = {
        "subawards": False,
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2013-01-03", "end_date": "2013-01-08"},
                {"start_date": "2019-06-01", "end_date": "2019-06-23"},
            ],
            "award_type_codes": grants_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_no_awards)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0
