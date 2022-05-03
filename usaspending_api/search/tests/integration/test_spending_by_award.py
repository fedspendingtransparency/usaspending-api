import json
import pytest

from datetime import datetime
from model_bakery import baker
from rest_framework import status
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters, legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.mark.django_db
def test_spending_by_award_subaward_success(client, spending_by_award_test_data):

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
        "prime_award_recipient_id": "41874914-2c27-813b-1505-df94f35b42dc-R",
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
        "prime_award_recipient_id": "0c324830-6283-38d3-d52e-00a71847d92d-R",
        "recipient_id": None,
        "prime_award_generated_internal_id": "CONT_AWD_TESTING_2",
    }


@pytest.mark.django_db
def test_spending_by_award_legacy_filters(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_no_intersection(client, monkeypatch, elasticsearch_award_index):

    baker.make("awards.Award", id=1, type="A", latest_transaction_id=1)
    baker.make("awards.TransactionNormalized", id=1, action_date="2010-10-01", award_id=1, is_fpds=True)
    baker.make("awards.TransactionFPDS", transaction_id=1)

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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
            award = baker.make(
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
            baker.make(
                "awards.TransactionNormalized", id=1000 + award_id, award=award, action_date=date_range["action_date"]
            )


@pytest.mark.django_db
def test_date_range_search_with_one_range(
    client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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
def test_date_range_search_with_two_ranges(
    client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

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


@pytest.mark.django_db
def test_success_with_all_filters(client, monkeypatch, elasticsearch_award_index):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": non_legacy_filters(),
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK, f"Failed to return 200 Response"


@pytest.mark.django_db
def test_inclusive_naics_code(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    """
    Verify use of built query_string boolean logic for NAICS code inclusions/exclusions executes as expected on ES
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "naics_codes": {"require": ["1122"]},
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2


@pytest.mark.django_db
def test_exclusive_naics_code(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    """
    Verify use of built query_string boolean logic for NAICS code inclusions/exclusions executes as expected on ES
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "naics_codes": {"require": ["999990"]},
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 0


@pytest.mark.django_db
def test_mixed_naics_codes(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    """
    Verify use of built query_string boolean logic for NAICS code inclusions/exclusions executes as expected on ES
    """

    baker.make(
        "awards.Award",
        id=5,
        type="A",
        category="contract",
        fain="abc444",
        earliest_transaction_id=8,
        latest_transaction_id=8,
        generated_unique_award_id="ASST_NON_TESTING_4",
        date_signed="2019-01-01",
        total_obligation=12.00,
    )

    baker.make("awards.TransactionNormalized", id=8, award_id=5, action_date="2019-10-1", is_fpds=True)
    baker.make("awards.TransactionFPDS", transaction_id=8, naics="222233", awardee_or_recipient_uniqu="duns_1001")

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "naics_codes": {"require": ["112233", "222233"], "exclude": ["112233"]},
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 5, "Award ID": None, "generated_internal_id": "ASST_NON_TESTING_4"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Keyword filter does not match expected result"


@pytest.mark.django_db
def test_correct_response_for_each_filter(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    """
    Verify the content of the response when using different filters. This function creates the ES Index
    and then calls each of the tests instead of recreating the ES Index multiple times with the same data.
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_cases = [
        _test_correct_response_for_keywords,
        _test_correct_response_for_time_period,
        _test_correct_response_for_award_type_codes,
        _test_correct_response_for_agencies,
        _test_correct_response_for_tas_components,
        _test_correct_response_for_pop_location,
        _test_correct_response_for_recipient_location,
        _test_correct_response_for_recipient_search_text,
        _test_correct_response_for_recipient_type_names,
        _test_correct_response_for_award_amounts,
        _test_correct_response_for_cfda_program,
        _test_correct_response_for_naics_codes,
        _test_correct_response_for_psc_code_list,
        _test_correct_response_for_psc_code_object,
        _test_correct_response_for_psc_code_list_subawards,
        _test_correct_response_for_psc_code_object_subawards,
        _test_correct_response_for_contract_pricing_type_codes,
        _test_correct_response_for_set_aside_type_codes,
        _test_correct_response_for_set_extent_competed_type_codes,
        _test_correct_response_for_recipient_id,
        _test_correct_response_for_def_codes,
        _test_correct_response_for_def_codes_subaward,
    ]

    for test in test_cases:
        test(client)


def _test_correct_response_for_keywords(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": ["A"], "keywords": ["test"]},
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Keyword filter does not match expected result"


def _test_correct_response_for_time_period(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A"],
                    "time_period": [{"start_date": "2014-01-01", "end_date": "2008-12-31"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Time Period filter does not match expected result"


def _test_correct_response_for_award_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
        {"internal_id": 3, "Award ID": "abc333", "generated_internal_id": "CONT_AWD_TESTING_3"},
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 4
    assert resp.json().get("results") == expected_result, "Award Type Codes filter does not match expected result"


def _test_correct_response_for_agencies(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "agencies": [
                        {"type": "awarding", "tier": "toptier", "name": "TOPTIER AGENCY 1"},
                        {"type": "awarding", "tier": "subtier", "name": "SUBTIER AGENCY 1"},
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Agency filter does not match expected result"


def _test_correct_response_for_tas_components(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "tas_codes": [{"aid": "097", "main": "4930"}],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2
    assert resp.json().get("results") == expected_result, "TAS Codes filter does not match expected result"


def _test_correct_response_for_pop_location(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "place_of_performance_locations": [{"country": "USA", "state": "VA", "county": "013"}],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Place of Performance filter does not match expected result"


def _test_correct_response_for_recipient_location(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_locations": [
                        {"country": "USA", "state": "VA", "county": "012"},
                        {"country": "USA", "state": "VA", "city": "Arlington"},
                    ],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "asc",
                "subawards": False,
            }
        ),
    )
    expected_result = [
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2
    assert resp.json().get("results") == expected_result, "Recipient Location filter does not match expected result"


def _test_correct_response_for_recipient_search_text(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["02", "03", "04", "05"],
                    "recipient_search_text": ["recipient_name_for_award_1001"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 4, "Award ID": "abc444", "generated_internal_id": "ASST_NON_TESTING_4"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Recipient Search Text filter does not match expected result"


def _test_correct_response_for_recipient_type_names(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_type_names": ["business_category_1_3", "business_category_2_8"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "asc",
                "subawards": False,
            }
        ),
    )
    expected_result = [
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
        {"internal_id": 3, "Award ID": "abc333", "generated_internal_id": "CONT_AWD_TESTING_3"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2
    assert resp.json().get("results") == expected_result, "Recipient Type Names filter does not match expected result"


def _test_correct_response_for_award_amounts(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "award_amounts": [{"upper_bound": 1000000}, {"lower_bound": 9013, "upper_bound": 9017}],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "asc",
                "subawards": False,
            }
        ),
    )
    expected_result = [
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 3
    assert resp.json().get("results") == expected_result, "Award Amounts filter does not match expected result"


def _test_correct_response_for_cfda_program(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["02", "03", "04", "05"],
                    "program_numbers": ["10.331"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 4, "Award ID": "abc444", "generated_internal_id": "ASST_NON_TESTING_4"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "CFDA Program filter does not match expected result"


def _test_correct_response_for_cfda_program_subawards(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["02", "03", "04", "05"],
                    "program_numbers": ["10.331"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Sub-Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Sub-Award ID",
                "order": "desc",
                "subawards": True,
            }
        ),
    )
    expected_result = [
        {
            "internal_id": "99999",
            "prime_award_internal_id": 4,
            "Sub-Award ID": "99999",
            "prime_award_generated_internal_id": "ASST_NON_TESTING_4",
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "CFDA Program filter does not match expected result"


def _test_correct_response_for_naics_codes(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "naics_codes": {"require": ["1122"], "exclude": ["112244"]},
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "NAICS Code filter does not match expected result"


def _test_correct_response_for_psc_code_list(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "psc_codes": ["PSC1"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "PSC Code filter does not match expected result"


def _test_correct_response_for_psc_code_object(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "psc_codes": {
                        "require": [["Service", "P", "PSC", "PSC1"]],
                        "exclude": [["Service", "P", "PSC", "PSC0"]],
                    },
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "PSC Code filter does not match expected result"


def _test_correct_response_for_psc_code_list_subawards(client):
    """ As of this writing, subawards query postgres whereas prime awards query elasticsearch.  Let's test both. """
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "psc_codes": ["PSC2"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Sub-Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Sub-Award ID",
                "order": "desc",
                "subawards": True,
            }
        ),
    )
    expected_result = [
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "PSC Code filter does not match expected result"


def _test_correct_response_for_psc_code_object_subawards(client):
    """ As of this writing, subawards query postgres whereas prime awards query elasticsearch.  Let's test both. """
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "psc_codes": {
                        "require": [["Service", "P", "PSC", "PSC2"]],
                        "exclude": [["Service", "P", "PSC", "PSC0"]],
                    },
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Sub-Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Sub-Award ID",
                "order": "desc",
                "subawards": True,
            }
        ),
    )
    expected_result = [
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "PSC Code filter does not match expected result"


def _test_more_sophisticated_eclipsed_psc_code_1(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "psc_codes": {
                        "require": [["Service"], ["Service", "P", "PSC"]],
                        "exclude": [["Service", "P"], ["Service", "P", "PSC", "PSC1"]],
                    },
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 0


def _test_more_sophisticated_eclipsed_psc_code_2(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "psc_codes": {
                        "require": [["Service", "P"], ["Service", "P", "PSC", "PSC1"]],
                        "exclude": [["Service"], ["Service", "P", "PSC"]],
                    },
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1


def _test_correct_response_for_contract_pricing_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "contract_pricing_type_codes": ["contract_pricing_test"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert (
        resp.json().get("results") == expected_result
    ), "Contract Pricing Type Codes filter does not match expected result"


def _test_correct_response_for_set_aside_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "set_aside_type_codes": ["type_set_aside_test"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Set Aside Type Codes filter does not match expected result"


def _test_correct_response_for_set_extent_competed_type_codes(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "extent_competed_type_codes": ["extent_competed_test"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert (
        resp.json().get("results") == expected_result
    ), "Extent Competed Type Codes filter does not match expected result"


def _test_correct_response_for_recipient_id(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["02", "03", "04", "05"],
                    "recipient_id": "51c7c0ad-a793-de3f-72ba-be5c2895a9ca",
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 4, "Award ID": "abc444", "generated_internal_id": "ASST_NON_TESTING_4"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Recipient ID filter does not match expected result"


def _test_correct_response_for_def_codes(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "def_codes": ["L", "Q"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2
    assert resp.json().get("results") == expected_result, "DEFC filter does not match expected result"

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "def_codes": ["J"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = []
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 0
    assert resp.json().get("results") == expected_result, "DEFC filter does not match expected result"


def _test_correct_response_for_def_codes_subaward(client):
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "def_codes": ["L"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Sub-Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Sub-Award ID",
                "order": "desc",
                "subawards": True,
            }
        ),
    )
    expected_result = [
        {
            "internal_id": "22222",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "22222",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        },
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2
    assert resp.json().get("results") == expected_result, "DEFC subaward filter does not match expected result"

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "def_codes": ["J"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Sub-Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Sub-Award ID",
                "order": "desc",
                "subawards": True,
            }
        ),
    )
    expected_result = []
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 0
    assert resp.json().get("results") == expected_result, "DEFC subaward filter does not match expected result"


@pytest.mark.django_db
def test_failure_with_invalid_filters(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Fails with no request data
    resp = client.post("/api/v2/search/spending_by_award", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json().get("detail") == "Missing value: 'fields' is a required field"

    # Fails with empty filters
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps({"fields": [], "filters": {}, "page": 1, "limit": 60, "subawards": False}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json().get("detail") == "Missing value: 'filters|award_type_codes' is a required field"

    # fails with empty field
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "fields": [],
                "filters": {
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                    "award_type_codes": ["A", "B", "C", "D"],
                },
                "page": 1,
                "limit": 60,
                "subawards": False,
            }
        ),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json().get("detail") == "Field 'fields' value '[]' is below min '1' items"


@pytest.mark.django_db
def test_search_after(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"award_type_codes": ["A", "B", "C", "D"]},
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "asc",
                "subawards": False,
                "last_record_unique_id": 1,
                "last_record_sort_value": "abc111",
            }
        ),
    )
    expected_result = [
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
        {"internal_id": 3, "Award ID": "abc333", "generated_internal_id": "CONT_AWD_TESTING_3"},
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 3
    assert resp.json().get("results") == expected_result, "Award Type Code filter does not match expected result"


@pytest.mark.django_db
def test_no_0_covid_amounts(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "def_codes": ["L"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "DEFC filter does not match expected result"


@pytest.mark.django_db
def test_uei_keyword_filter(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "keywords": ["testuei"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "UEI filter does not match expected result"


@pytest.mark.django_db
def test_parent_uei_keyword_filter(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "keywords": ["test_parent_uei"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "subawards": False,
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "UEI filter does not match expected result"


@pytest.mark.django_db
def test_uei_recipient_filter_subaward(client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2022-09-30"}],
                    "award_type_codes": [
                        "A",
                        "B",
                        "C",
                        "D",
                        "IDV_A",
                        "IDV_B",
                        "IDV_B_A",
                        "IDV_B_B",
                        "IDV_B_C",
                        "IDV_C",
                        "IDV_D",
                        "IDV_E",
                    ],
                    "recipient_search_text": ["uei_10010001"],
                },
                "fields": ["Sub-Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Sub-Award ID",
                "order": "desc",
                "subawards": True,
            }
        ),
    )
    expected_result = [
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "UEI Recipient subaward filter does not match expected result"
