# Stdlib imports
import pytest
import json
from rest_framework import status
from datetime import datetime

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_award_subaward_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {"subawards": True, "fields": ["Sub-Award ID"], "sort": "Sub-Award ID", "filters": all_filters()}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_no_intersection(client, mock_matviews_qs):
    mock_model_1 = MockModel(
        award_ts_vector="",
        type="A",
        type_of_contract_pricing="",
        naics_code="98374",
        cfda_number="987.0",
        pulled_from=None,
        uri=None,
        piid="djsd",
        fain=None,
        award_id=90,
        awarding_toptier_agency_name="Department of Pizza",
        awarding_toptier_agency_abbreviation="DOP",
        generated_pragmatic_obligation=10,
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1])

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
def awards_over_different_date_ranges(mock_matviews_qs):
    award_category_list = ["contracts", "direct_payments", "grants", "idvs", "loans", "other_financial_assistance"]

    # The date ranges for the different awards are setup to cover possible intersection points by the
    # different date ranges being searched. The comments on each line specify where the date ranges are
    # suppose to overlap the searched for date ranges. The search for date ranges are:
    #    - {"start_date": "2015-01-01", "end_date": "2015-12-31"}
    #    - {"start_date": "2017-02-01", "end_date": "2017-11-30"}
    date_range_list = [
        # Intersect only one of the date ranges searched for
        {"earliest_action_date": datetime(2014, 1, 1), "action_date": datetime(2014, 5, 1)},  # Before both
        {"earliest_action_date": datetime(2014, 3, 1), "action_date": datetime(2015, 4, 15)},  # Beginning of first
        {"earliest_action_date": datetime(2015, 2, 1), "action_date": datetime(2015, 7, 1)},  # Middle of first
        {"earliest_action_date": datetime(2015, 2, 1), "action_date": datetime(2015, 4, 17)},
        {"earliest_action_date": datetime(2014, 12, 1), "action_date": datetime(2016, 1, 1)},  # All of first
        {"earliest_action_date": datetime(2015, 11, 1), "action_date": datetime(2016, 3, 1)},  # End of first
        {"earliest_action_date": datetime(2016, 2, 23), "action_date": datetime(2016, 7, 19)},  # Between both
        {"earliest_action_date": datetime(2016, 11, 26), "action_date": datetime(2017, 3, 1)},  # Beginning of second
        {"earliest_action_date": datetime(2017, 5, 1), "action_date": datetime(2017, 7, 1)},  # Middle of second
        {"earliest_action_date": datetime(2017, 1, 1), "action_date": datetime(2017, 12, 1)},  # All of second
        {"earliest_action_date": datetime(2017, 9, 1), "action_date": datetime(2017, 12, 17)},  # End of second
        {"earliest_action_date": datetime(2018, 2, 1), "action_date": datetime(2018, 7, 1)},  # After both
        # Intersect both date ranges searched for
        {"earliest_action_date": datetime(2014, 12, 1), "action_date": datetime(2017, 12, 5)},  # Completely both
        {"earliest_action_date": datetime(2015, 7, 1), "action_date": datetime(2017, 5, 1)},  # Partially both
        {
            "earliest_action_date": datetime(2014, 10, 3),  # All first; partial second
            "action_date": datetime(2017, 4, 8),
        },
        {
            "earliest_action_date": datetime(2015, 8, 1),  # Partial first; all second
            "action_date": datetime(2018, 1, 2),
        },
    ]

    award_id = 0
    mock_model_list = []

    for award_category in award_category_list:
        for date_range in date_range_list:
            award_id += 1
            award_type_list = all_award_types_mappings[award_category]
            award_type = award_type_list[award_id % len(award_type_list)]
            mock_model_list.append(
                MockModel(
                    award_ts_vector="",
                    type=award_type,
                    category=award_category,
                    type_of_contract_pricing="",
                    naics_code=str(9000 + award_id),
                    cfda_number=str(900 + award_id),
                    pulled_from=None,
                    uri=None,
                    piid="abcdefg{}".format(award_id),
                    fain=None,
                    award_id=award_id,
                    awarding_toptier_agency_name="Department of {}".format(award_id),
                    awarding_toptier_agency_abbreviation="DOP",
                    generated_pragmatic_obligation=10,
                    earliest_action_date=date_range["earliest_action_date"],
                    action_date=date_range["action_date"],
                )
            )

    add_to_mock_objects(mock_matviews_qs, mock_model_list)


@pytest.mark.django_db
def test_date_range_search_with_one_range(client, awards_over_different_date_ranges):
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
    assert resp.data["results"] == [{"Award ID": "abcdefg1", "internal_id": 1}]

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
def test_date_range_search_with_two_ranges(client, awards_over_different_date_ranges):
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
        {"Award ID": "abcdefg44", "internal_id": 44},
        {"Award ID": "abcdefg33", "internal_id": 33},
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
