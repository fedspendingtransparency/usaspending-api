import json
import pytest
from datetime import datetime

from model_bakery import baker
from rest_framework import status

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_data_fixture():
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        transaction_unique_id="A000_8900_DECF0000058_-NONE-",
        action_date="2009-09-10",
        award_id=48518634,
        fiscal_year=2009,
        generated_unique_award_id="ASST_NON_DECF0000058_8900",
        is_fpds=False,
        type="07",
        type_description="DIRECT LOAN (E)",
        award_category="loans",
        fain="DECF0000058",
        piid=None,
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        base_and_all_options_value=None,
        base_exercised_options_val=None,
        category="loans",
        certified_date="2018-07-12",
        create_date="2018-02-03 18:54:06.983782+00",
        data_source="DBR",
        date_signed="2009-09-10",
        description="FORD MOTOR COMPANY",
        fain="DECF0000058",
        fiscal_year=2009,
        fpds_agency_id=None,
        fpds_parent_agency_id=None,
        funding_agency_id=None,
        generated_unique_award_id="ASST_NON_DECF0000058_8900",
        award_id=48518634,
        is_fpds=False,
        last_modified_date="2018-08-02",
        latest_transaction_id=2,
        non_federal_funding_amount=0,
        parent_award_piid=None,
        period_of_performance_current_end_date="2039-09-09",
        period_of_performance_start_date="2009-09-10",
        piid=None,
        subaward_count=0,
        total_funding_amount=5907041570,
        total_loan_value=5907041570,
        total_obligation=5907041570,
        total_subaward_amount=None,
        total_subsidy_cost=3000186413,
        transaction_unique_id="A000_8900_DECF0000058_-NONE-",
        type="07",
        type_description="DIRECT LOAN (E)",
        uri=None,
        action_date="2018-10-01",
    )


@pytest.mark.django_db
def test_spending_by_award_type_success(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # test for filters
    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A", "B", "C"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_count_filters(client, monkeypatch, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award_count",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_type_failure(client, monkeypatch, elasticsearch_award_index):
    """Verify error on bad autocomplete request for budget function."""
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps({"test": {}, "filters": {}}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_by_award_no_intersection(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    request = {"subawards": False, "fields": ["Award ID"], "sort": "Award ID", "filters": {"award_type_codes": ["07"]}}

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["loans"] == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == {
        "contracts": 0,
        "idvs": 0,
        "grants": 0,
        "direct_payments": 0,
        "loans": 0,
        "other": 0,
    }, "Results returned, they should all be 0"


@pytest.mark.django_db
def test_spending_by_award_subawards_no_intersection(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index
):
    baker.make("search.AwardSearch", award_id=90, action_date="2023-01-01")
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=9999,
        prime_award_group="grant",
        prime_award_type="02",
        award_id=90,
        awarding_toptier_agency_name="Toptier Agency 1",
        awarding_toptier_agency_abbreviation="TA1",
        subaward_amount=10,
        action_date="2023-01-01",
    )

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    request = {
        "subawards": True,
        "fields": ["Sub-Award ID"],
        "sort": "Sub-Award ID",
        "filters": {"award_type_codes": ["02", "03", "04", "05"]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["subgrants"] == 1

    request["filters"]["award_type_codes"].append("no intersection")
    resp = client.post(
        "/api/v2/search/spending_by_award_count", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == {"subcontracts": 0, "subgrants": 0}, "Results returned, there should all be 0"


@pytest.fixture
def awards_over_different_date_ranges_with_different_counts():
    award_category_list_and_counts = {
        "contracts": 5,
        "direct_payments": 8,
        "grants": 16,
        "idvs": 10,
        "loans": 9,
        "other_financial_assistance": 14,
    }

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

    for award_category, count in award_category_list_and_counts.items():
        for date_range in date_range_list[:count]:
            award_id += 1
            award_type_list = all_award_types_mappings[award_category]
            award_type = award_type_list[award_id % len(award_type_list)]
            baker.make(
                "search.AwardSearch",
                award_id=award_id,
                latest_transaction_id=award_id + 1000,
                type=award_type,
                category=award_category,
                piid=f"abcdefg{award_id}",
                uri=None,
                fain=None,
                date_signed=date_range["date_signed"],
                action_date=date_range["action_date"],
            )
            baker.make(
                "search.TransactionSearch",
                transaction_id=award_id + 1000,
                award_id=award_id,
                action_date=date_range["action_date"],
                is_fpds=True,
                pulled_from=None,
            )


@pytest.mark.django_db
def test_date_range_search_counts_with_one_range(
    client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges_with_different_counts
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    request = {
        "subawards": False,
        "auditTrail": "Award Table - Tab Counts",
        "filters": {"time_period": [{"start_date": "2015-01-01", "end_date": "2015-12-31"}]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count/", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["contracts"] == 4
    assert resp.data["results"]["direct_payments"] == 5
    assert resp.data["results"]["grants"] == 9
    assert resp.data["results"]["idvs"] == 5
    assert resp.data["results"]["loans"] == 5
    assert resp.data["results"]["other"] == 7

    # Test with only one specific award showing
    request_for_one_award = {
        "subawards": False,
        "auditTrail": "Award Table - Tab Counts",
        "filters": {"time_period": [{"start_date": "2014-01-03", "end_date": "2014-01-08"}]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps(request_for_one_award),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["contracts"] == 1
    assert resp.data["results"]["direct_payments"] == 1
    assert resp.data["results"]["grants"] == 1
    assert resp.data["results"]["idvs"] == 1
    assert resp.data["results"]["loans"] == 1
    assert resp.data["results"]["other"] == 1

    # Test with no award showing
    request_for_no_awards = {
        "subawards": False,
        "auditTrail": "Award Table - Tab Counts",
        "filters": {"time_period": [{"start_date": "2013-01-03", "end_date": "2013-01-08"}]},
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps(request_for_no_awards),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["contracts"] == 0
    assert resp.data["results"]["direct_payments"] == 0
    assert resp.data["results"]["grants"] == 0
    assert resp.data["results"]["idvs"] == 0
    assert resp.data["results"]["loans"] == 0
    assert resp.data["results"]["other"] == 0


@pytest.mark.django_db
def test_date_range_search_counts_with_two_ranges(
    client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges_with_different_counts
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    request = {
        "subawards": False,
        "auditTrail": "Award Table - Tab Counts",
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31"},
                {"start_date": "2017-02-01", "end_date": "2017-11-30"},
            ]
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count/", content_type="application/json", data=json.dumps(request)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["contracts"] == 4
    assert resp.data["results"]["direct_payments"] == 6
    assert resp.data["results"]["grants"] == 13
    assert resp.data["results"]["idvs"] == 8
    assert resp.data["results"]["loans"] == 7
    assert resp.data["results"]["other"] == 11

    # Test with only one specific award showing
    request_for_one_award = {
        "subawards": False,
        "auditTrail": "Award Table - Tab Counts",
        "filters": {
            "time_period": [
                {"start_date": "2014-01-03", "end_date": "2014-01-08"},
                {"start_date": "2019-06-01", "end_date": "2019-06-23"},
            ]
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps(request_for_one_award),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["contracts"] == 1
    assert resp.data["results"]["direct_payments"] == 1
    assert resp.data["results"]["grants"] == 1
    assert resp.data["results"]["idvs"] == 1
    assert resp.data["results"]["loans"] == 1
    assert resp.data["results"]["other"] == 1

    # Test with no award showing
    request_for_no_awards = {
        "subawards": False,
        "auditTrail": "Award Table - Tab Counts",
        "filters": {
            "time_period": [
                {"start_date": "2013-01-03", "end_date": "2013-01-08"},
                {"start_date": "2019-06-01", "end_date": "2019-06-23"},
            ]
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award_count/",
        content_type="application/json",
        data=json.dumps(request_for_no_awards),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"]["contracts"] == 0
    assert resp.data["results"]["direct_payments"] == 0
    assert resp.data["results"]["grants"] == 0
    assert resp.data["results"]["idvs"] == 0
    assert resp.data["results"]["loans"] == 0
    assert resp.data["results"]["other"] == 0
