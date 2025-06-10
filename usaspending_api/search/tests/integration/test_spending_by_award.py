import json
from datetime import datetime

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.search.tests.data.search_filters_test_data import legacy_filters, non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_data_fixture(db):
    baker.make("search.TransactionSearch", transaction_id=210210210, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=321032103, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=432104321, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=543210543, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=654321065, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=765432107, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=876543210, action_date="2013-09-17")
    baker.make("search.TransactionSearch", transaction_id=987654321, action_date="2013-09-17")

    ref_program_activity1 = baker.make(
        "references.RefProgramActivity",
        id=1,
        program_activity_code=123,
        program_activity_name="PROGRAM_ACTIVITY_123",
    )
    ref_program_activity2 = baker.make(
        "references.RefProgramActivity",
        id=2,
        program_activity_code=2,
        program_activity_name="PROGRAM_ACTIVITY_2",
    )

    baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19", public_law="LAW", title="title")
    baker.make(
        "references.DisasterEmergencyFundCode", code="Z", group_name="infrastructure", public_law="LAW", title="title"
    )

    award1 = baker.make(
        "search.AwardSearch",
        category="loans",
        date_signed="2012-09-10",
        action_date="2012-09-12",
        fain="DECF0000058",
        generated_unique_award_id="ASST_NON_DECF0000058_8900",
        award_id=200,
        latest_transaction_id=210210210,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2012-09-10",
        piid=None,
        type="07",
        uri=None,
        display_award_id="award200",
        program_activities=[{"code": "0123", "name": "PROGRAM_ACTIVITY_123"}],
        spending_by_defc=None,
    )
    award2 = baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-12-10",
        action_date="2009-12-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY2_8900",
        award_id=300,
        display_award_id="award300",
        latest_transaction_id=321032103,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2014-09-10",
        piid="YUGGY2",
        type="IDV_B_A",
        uri=None,
        spending_by_defc=[
            {"defc": "L", "outlay": 100.0, "obligation": 10.0},
            {"defc": "Z", "outlay": 200.0, "obligation": 20.0},
        ],
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2015-05-10",
        action_date="2015-05-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY3_8900",
        award_id=400,
        latest_transaction_id=432104321,
        period_of_performance_current_end_date="2018-09-09",
        period_of_performance_start_date="2018-09-01",
        piid="YUGGY3",
        type="IDV_B",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-09-10",
        action_date="2009-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY_8900",
        award_id=500,
        latest_transaction_id=543210543,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2018-09-10",
        piid="YUGGY",
        type="IDV_B_C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-09-10",
        action_date="2009-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_YUGGY55_8900",
        award_id=600,
        latest_transaction_id=654321065,
        period_of_performance_current_end_date="2039-09-09",
        period_of_performance_start_date="2009-09-10",
        piid="YUGGY55",
        type="IDV_C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2009-12-20",
        action_date="2009-12-20",
        fain=None,
        generated_unique_award_id="CONT_IDV_BEANS_8900",
        award_id=700,
        latest_transaction_id=765432107,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2009-12-20",
        piid="BEANS",
        type="IDV_C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="idvs",
        date_signed="2011-09-10",
        action_date="2011-09-10",
        fain=None,
        generated_unique_award_id="CONT_IDV_BEANS55_8900",
        award_id=800,
        latest_transaction_id=876543210,
        period_of_performance_current_end_date="2020-12-09",
        period_of_performance_start_date="2011-09-10",
        piid="BEANS55",
        type="IDV_B_A",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="contracts",
        date_signed="2011-09-10",
        action_date="2011-09-10",
        fain=None,
        generated_unique_award_id="CONT_AW_BEANS55_8900",
        award_id=1000,
        latest_transaction_id=876543210,
        period_of_performance_current_end_date="2020-12-09",
        period_of_performance_start_date="2011-09-10",
        piid="BEANS55",
        type="C",
        uri=None,
    )
    baker.make(
        "search.AwardSearch",
        category="other",
        date_signed="2013-09-10",
        action_date="2013-09-10",
        fain=None,
        generated_unique_award_id="ASST_AGG_JHISUONSD_8900",
        award_id=900,
        latest_transaction_id=987654321,
        period_of_performance_current_end_date="2018-09-09",
        period_of_performance_start_date="2013-09-10",
        piid=None,
        type="11",
        uri="JHISUONSD",
    )
    baker.make(
        "search.AwardSearch",
        category="contracts",
        date_signed="2009-12-20",
        action_date="2009-12-20",
        fain=None,
        generated_unique_award_id="CONT_AW_BEANS_8900",
        award_id=1100,
        latest_transaction_id=765432107,
        period_of_performance_current_end_date="2019-09-09",
        period_of_performance_start_date="2009-12-20",
        piid="BEANS",
        type="A",
        uri=None,
    )

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award=award1,
        sub_action_date="2023-01-01",
        prime_award_group="grant",
        prime_award_type="07",
        subaward_number=99999,
        action_date="2023-01-01",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award=award1,
        sub_action_date="2023-01-01",
        prime_award_group="procurement",
        prime_award_type="08",
        subaward_number=99998,
        action_date="2023-01-01",
    )

    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=1,
        award_id=award1.award_id,
        program_activity_id=ref_program_activity1.id,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=2,
        award_id=award2.award_id,
        program_activity_id=ref_program_activity2.id,
    )


@pytest.mark.django_db
def test_spending_by_award_subaward_success(
    client, monkeypatch, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Testing all filters
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "spending_level": "subawards",
                "fields": ["Sub-Award ID"],
                "sort": "Sub-Award ID",
                "filters": non_legacy_filters(),
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK

    # Testing contents of what is returned
    spending_level_filter_list = [{"spending_level": "subawards"}, {"spending_level": "subawards"}]

    for spending_level_filter in spending_level_filter_list:
        resp = client.post(
            "/api/v2/search/spending_by_award",
            content_type="application/json",
            data=json.dumps(
                {
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
                    **spending_level_filter,
                }
            ),
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["page_metadata"]["page"] == 1
        assert resp.json()["page_metadata"]["hasNext"]
        assert resp.json()["limit"] == 2
        assert len(resp.json()["results"]) == 2
        assert resp.json()["spending_level"] == "subawards"
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
        data=json.dumps(
            {"spending_level": "awards", "fields": ["Award ID"], "sort": "Award ID", "filters": legacy_filters()}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_no_intersection(client, monkeypatch, elasticsearch_award_index):

    baker.make("search.AwardSearch", award_id=1, type="A", latest_transaction_id=1, action_date="2020-10-10")
    baker.make("search.TransactionSearch", transaction_id=1, action_date="2010-10-01", award_id=1, is_fpds=True)

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    request = {
        "spending_level": "awards",
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
            if award_category in ("contracts", "idvs"):
                display_award_id = "abcdefg{}".format(award_id)
            else:
                display_award_id = "xyz{}".format(award_id)
            award = baker.make(
                "search.AwardSearch",
                award_id=award_id,
                generated_unique_award_id=guai,
                type=award_type,
                category=award_category,
                latest_transaction_id=1000 + award_id,
                date_signed=date_range["date_signed"],
                display_award_id=display_award_id,
                piid="abcdefg{}".format(award_id),
                fain="xyz{}".format(award_id),
                uri="abcxyx{}".format(award_id),
                action_date=date_range["action_date"],
            )
            baker.make(
                "search.TransactionSearch",
                transaction_id=1000 + award_id,
                award=award,
                action_date=date_range["action_date"],
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
        "spending_level": "awards",
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
def test_date_range_with_date_signed(client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    contract_type_list = all_award_types_mappings["contracts"]

    request_for_2015 = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31", "date_type": "date_signed"},
            ],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_2015)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 5

    request_for_2016 = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2016-01-01", "end_date": "2016-12-31", "date_type": "date_signed"},
            ],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_2016)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2


@pytest.mark.django_db
def test_messages_not_nested(client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    contract_type_list = all_award_types_mappings["contracts"]

    request_for_2015 = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31", "date_type": "date_signed"},
            ],
            "award_type_codes": contract_type_list,
            "not_a_real_filter": "abc",
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_2015)
    )
    resp_json = resp.json()

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 5
    assert resp_json["messages"] == get_generic_filters_message(
        request_for_2015["filters"].keys(), {"time_period", "award_type_codes"}
    )


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
                "spending_level": "awards",
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
        "search.AwardSearch",
        award_id=5,
        type="A",
        category="contract",
        fain="abc444",
        earliest_transaction_id=8,
        latest_transaction_id=8,
        generated_unique_award_id="ASST_NON_TESTING_5",
        date_signed="2019-01-01",
        total_obligation=12.00,
        naics_code="222233",
        action_date="2019-01-01",
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=8,
        award_id=5,
        action_date="2019-10-1",
        is_fpds=True,
        naics_code="222233",
        recipient_unique_id="duns_1001",
    )

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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [{"internal_id": 5, "Award ID": None, "generated_internal_id": "ASST_NON_TESTING_5"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Keyword filter does not match expected result"


@pytest.mark.django_db
def test_correct_response_for_each_filter(
    client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index, elasticsearch_subaward_index
):
    """
    Verify the content of the response when using different filters. This function creates the ES Index
    and then calls each of the tests instead of recreating the ES Index multiple times with the same data.
    """
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 2
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
                "spending_level": "awards",
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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [
        {"internal_id": 999, "Award ID": "award999", "generated_internal_id": "ASST_NON_TESTING_999"},
        {"internal_id": 998, "Award ID": "award998", "generated_internal_id": "ASST_NON_TESTING_998"},
        {"internal_id": 997, "Award ID": "award997", "generated_internal_id": "ASST_NON_TESTING_997"},
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
        {"internal_id": 3, "Award ID": "abc333", "generated_internal_id": "CONT_AWD_TESTING_3"},
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
        {"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 7
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                    "place_of_performance_locations": [{"country": "USA", "state": "VA", "county": "014"}],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [{"internal_id": 4, "Award ID": "abc444", "generated_internal_id": "ASST_NON_TESTING_4"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "Recipient Search Text filter does not match expected result"

    # Test the results when searching for a recipient name that ends with a period
    # A search for `ACME INC` should include ACME INC, ACME INC. and ACME INC.XYZ
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_search_text": ["ACME INC"],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID", "Recipient Name"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [
        {
            "internal_id": 999,
            "Award ID": "award999",
            "Recipient Name": "ACME INC.XYZ",
            "generated_internal_id": "ASST_NON_TESTING_999",
        },
        {
            "internal_id": 998,
            "Award ID": "award998",
            "Recipient Name": "ACME INC.",
            "generated_internal_id": "ASST_NON_TESTING_998",
        },
        {
            "internal_id": 997,
            "Award ID": "award997",
            "Recipient Name": "ACME INC",
            "generated_internal_id": "ASST_NON_TESTING_997",
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == len(expected_result)
    assert resp.json().get("results") == expected_result, "Recipient Search Text filter does not match expected result"

    # A search for `ACME INC.` should include ACME INC. and ACME INC.XYZ but not ACME INC
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    "recipient_search_text": ["ACME INC."],
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
                "fields": ["Award ID", "Recipient Name"],
                "page": 1,
                "limit": 60,
                "sort": "Award ID",
                "order": "desc",
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [
        {
            "internal_id": 999,
            "Award ID": "award999",
            "Recipient Name": "ACME INC.XYZ",
            "generated_internal_id": "ASST_NON_TESTING_999",
        },
        {
            "internal_id": 998,
            "Award ID": "award998",
            "Recipient Name": "ACME INC.",
            "generated_internal_id": "ASST_NON_TESTING_998",
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == len(expected_result)
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "subawards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "PSC Code filter does not match expected result"


def _test_correct_response_for_psc_code_list_subawards(client):
    """As of this writing, subawards query postgres whereas prime awards query elasticsearch.  Let's test both."""
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
                "spending_level": "subawards",
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
    """As of this writing, subawards query postgres whereas prime awards query elasticsearch.  Let's test both."""
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
                "spending_level": "subawards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = {"internal_id": 4, "Award ID": "abc444", "generated_internal_id": "ASST_NON_TESTING_4"}
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 7
    assert resp.json().get("results")[-1] == expected_result, "Recipient ID filter does not match expected result"


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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "subawards",
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
                "spending_level": "subawards",
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
        data=json.dumps({"fields": [], "filters": {}, "page": 1, "limit": 60, "spending_level": "awards"}),
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
                "spending_level": "awards",
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
                "spending_level": "awards",
                "last_record_unique_id": 1,
                "last_record_sort_value": "abc111",
            }
        ),
    )
    expected_result = [
        {"internal_id": 2, "Award ID": "abc222", "generated_internal_id": "CONT_AWD_TESTING_2"},
        {"internal_id": 3, "Award ID": "abc333", "generated_internal_id": "CONT_AWD_TESTING_3"},
        {"internal_id": 5, "Award ID": "abcdef123", "generated_internal_id": "CONT_AWD_TESTING_5"},
        {"internal_id": 997, "Award ID": "award997", "generated_internal_id": "ASST_NON_TESTING_997"},
        {"internal_id": 998, "Award ID": "award998", "generated_internal_id": "ASST_NON_TESTING_998"},
        {"internal_id": 999, "Award ID": "award999", "generated_internal_id": "ASST_NON_TESTING_999"},
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == len(expected_result)
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
                "spending_level": "awards",
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
                "spending_level": "awards",
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
                "spending_level": "awards",
            }
        ),
    )
    expected_result = [{"internal_id": 1, "Award ID": "abc111", "generated_internal_id": "CONT_AWD_TESTING_1"}]
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json().get("results")) == 1
    assert resp.json().get("results") == expected_result, "UEI filter does not match expected result"


@pytest.mark.django_db
def test_uei_recipient_filter_subaward(
    client, monkeypatch, spending_by_award_test_data, elasticsearch_award_index, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

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
                "spending_level": "subawards",
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


@pytest.mark.django_db
def test_date_range_with_new_awards_only(
    client, monkeypatch, elasticsearch_award_index, awards_over_different_date_ranges, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    contract_type_list = all_award_types_mappings["contracts"]

    request_for_2015 = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31", "date_type": "new_awards_only"},
            ],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_2015)
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 5

    request_for_2015 = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "sort": "Sub-Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "time_period": [
                {"start_date": "2015-01-01", "end_date": "2015-12-31", "date_type": "new_awards_only"},
            ],
            "award_type_codes": contract_type_list,
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_for_2015)
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.json().get("detail")
        == "Field 'filters|time_period' is outside valid values ['action_date', 'last_modified_date', 'date_signed', 'sub_action_date']"
    )


@pytest.mark.django_db
def test_spending_by_award_program_activity_subawards(
    client, monkeypatch, elasticsearch_award_index, spending_by_award_test_data, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Program Activites filter test
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = [
        {
            "internal_id": "99999",
            "prime_award_internal_id": 4,
            "Sub-Award ID": "99999",
            "prime_award_generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123"}, {"code": "123"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = [
        {
            "internal_id": "99999",
            "prime_award_internal_id": 4,
            "Sub-Award ID": "99999",
            "prime_award_generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = []
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_program_activity(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # Program Activites filter test
    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = [
        {
            "internal_id": 200,
            "Award ID": "award200",
            "generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = []
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "123"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = [
        {
            "internal_id": 200,
            "Award ID": "award200",
            "generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {
            "program_activities": [{"name": "program_activity_123"}, {"code": "123"}],
            "award_type_codes": [
                "07",
            ],
        },
    }
    expected_response = [
        {
            "internal_id": 200,
            "Award ID": "award200",
            "generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_subawards_award_id_filter(
    client, monkeypatch, spending_by_award_test_data, elasticsearch_subaward_index
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Test finding a Subaward by it's `subaward_number`
    payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "award_type_codes": ["07"],
            "award_ids": ["99999"],
        },
    }
    expected_response = [
        {
            "internal_id": "99999",
            "prime_award_internal_id": 4,
            "Sub-Award ID": "99999",
            "prime_award_generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post("/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # Test finding a Subaward by it's `award_piid_fain`
    payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "award_type_codes": ["07"],
            "award_ids": ["PIID6003"],
        },
    }
    expected_response = [
        {
            "internal_id": "99999",
            "prime_award_internal_id": 4,
            "Sub-Award ID": "99999",
            "prime_award_generated_internal_id": "ASST_NON_DECF0000058_8900",
        }
    ]
    resp = client.post("/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_unique_id_award(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Test with a real award_unique_id
    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "award_unique_id": "CONT_AWD_TESTING_1",
        },
    }
    expected_response = [
        {
            "internal_id": 1,
            "Award ID": "abc111",
            "generated_internal_id": "CONT_AWD_TESTING_1",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # Test with an undefined award_unique_id
    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "award_unique_id": "CONT_AWD_TESTING_4",
        },
    }
    expected_response = []
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_unique_id_subaward(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # Test with multiple subawards
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "award_unique_id": "CONT_AWD_TESTING_1",
        },
    }
    expected_response = [
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
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # Test with a single subaward
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "award_unique_id": "CONT_AWD_TESTING_2",
        },
    }
    expected_response = [
        {
            "internal_id": "33333",
            "prime_award_internal_id": 2,
            "Sub-Award ID": "33333",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_2",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # Test with no subawards
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "award_unique_id": "CONT_AWD_TESTING_4",
        },
    }
    expected_response = []
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_description_specificity(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # get award with description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "description": "the test"},
    }
    expected_response = [
        {
            "internal_id": 1,
            "Award ID": "abc111",
            "generated_internal_id": "CONT_AWD_TESTING_1",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # get subaward with description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "description": "the test"},
    }
    expected_response = [
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # ensure only queries for text in the correct order
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "description": "test the"},
    }
    expected_response = []
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_keyword_specificity(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # get award with naics_description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "keyword": "the test"},
    }
    expected_response = [
        {
            "internal_id": 1,
            "Award ID": "abc111",
            "generated_internal_id": "CONT_AWD_TESTING_1",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # get subaward with product_or_service_description "the test test test" and not
    # "the description for test"
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "keyword": "the test"},
    }
    expected_response = [
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    # ensure only queries for text in the correct order
    test_payload = {
        "spending_level": "subawards",
        "fields": ["Sub-Award ID"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "keyword": "test the"},
    }
    expected_response = []
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_new_subcontract_fields(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # get award with naics_description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Award Description",
            "Sub-Recipient UEI",
            "Sub-Recipient Location",
            "Sub-Award Primary Place of Performance",
            "Prime Award Recipient UEI",
            "NAICS",
            "PSC",
            "sub_award_recipient_id",
        ],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "keyword": "the test"},
    }
    expected_response = [
        {
            "internal_id": "11111",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "11111",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
            "Sub-Award Description": "the test test test",
            "Sub-Recipient UEI": "UEI_10010001",
            "Sub-Recipient Location": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "013",
                "county_name": "ARLINGTON",
                "address_line1": "1 Memorial Drive",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
                "foreign_postal_code": "55455",
            },
            "Sub-Award Primary Place of Performance": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "013",
                "county_name": "ARLINGTON",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
            },
            "Prime Award Recipient UEI": "testuei",
            "NAICS": {"code": "112233", "description": "the test test test"},
            "PSC": {"code": "PSC2", "description": "the test test test"},
            "sub_award_recipient_id": "EXAM-PLE-ID-P",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_new_subgrant_fields(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    # get award with naics_description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Award Description",
            "Sub-Recipient UEI",
            "Sub-Recipient Location",
            "Sub-Award Primary Place of Performance",
            "Prime Award Recipient UEI",
            "Assistance Listing",
            "sub_award_recipient_id",
        ],
        "filters": {"award_type_codes": ["08"], "award_ids": ["45509"]},
    }
    expected_response = [
        {
            "internal_id": "45509",
            "prime_award_internal_id": 1,
            "Sub-Award ID": "45509",
            "prime_award_generated_internal_id": "CONT_AWD_TESTING_1",
            "Sub-Award Description": "sub description 1",
            "Sub-Recipient UEI": "UEI_10010001",
            "Sub-Recipient Location": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "013",
                "county_name": "ARLINGTON",
                "address_line1": "1 Memorial Drive",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
                "foreign_postal_code": "55455",
            },
            "Sub-Award Primary Place of Performance": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "013",
                "county_name": "ARLINGTON",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
            },
            "Prime Award Recipient UEI": "uei 1",
            "Assistance Listing": {"cfda_number": "1.234", "cfda_program_title": "test cfda"},
            "sub_award_recipient_id": "EXAM-PLE-ID-P",
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_new_contract_fields(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # get award with naics_description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "awards",
        "fields": ["Award ID", "Recipient UEI", "Recipient Location", "Primary Place of Performance", "NAICS", "PSC"],
        "filters": {"award_type_codes": ["A", "B", "C", "D"], "keyword": "the test"},
    }
    expected_response = [
        {
            "internal_id": 1,
            "Award ID": "abc111",
            "generated_internal_id": "CONT_AWD_TESTING_1",
            "Recipient UEI": "testuei",
            "Recipient Location": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "014",
                "county_name": "ARLINGTON",
                "address_line1": "1 Memorial Drive",
                "address_line2": "Room 324",
                "address_line3": "Desk 5",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
                "foreign_postal_code": "55455",
                "foreign_province": "Manitoba",
            },
            "Primary Place of Performance": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "014",
                "county_name": "ARLINGTON",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
            },
            "NAICS": {"code": "112233", "description": "the test test test"},
            "PSC": {"code": "PSC1", "description": "the test test test"},
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_new_assistance_fields(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # get award with naics_description "the test test test" and not "the description for test"
    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "Recipient UEI",
            "Recipient Location",
            "Primary Place of Performance",
            "Assistance Listings",
            "primary_assistance_listing",
        ],
        "filters": {"award_type_codes": ["11"]},
    }
    expected_response = [
        {
            "internal_id": 5145,
            "Award ID": "award5145",
            "generated_internal_id": "ASST_NON_TESTING_5145",
            "Recipient UEI": "acmeuei",
            "Recipient Location": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "013",
                "county_name": "ARLINGTON",
                "address_line1": "1 Memorial Drive",
                "address_line2": "Room 324",
                "address_line3": "Desk 5",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
                "foreign_postal_code": "55455",
                "foreign_province": "Manitoba",
            },
            "Primary Place of Performance": {
                "location_country_code": "USA",
                "country_name": "UNITED STATES",
                "state_code": "VA",
                "state_name": "Virginia",
                "city_name": "ARLINGTON",
                "county_code": "013",
                "county_name": "ARLINGTON",
                "congressional_code": "08",
                "zip4": "9040",
                "zip5": "55455",
            },
            "Assistance Listings": [
                {"cfda_number": "64.114", "cfda_program_title": "VETERANS HOUSING GUARANTEED AND INSURED LOANS"}
            ],
            "primary_assistance_listing": {
                "cfda_number": "64.114",
                "cfda_program_title": "VETERANS HOUSING GUARANTEED AND INSURED LOANS",
            },
        },
    ]
    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


def test_spending_by_award_sort_recipient_location(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "Recipient Location",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Recipient Location",
        "order": "asc",
    }

    recipient_location4 = {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "NE",
        "state_name": "Nebraska",
        "city_name": "OMAHA",
        "county_code": "013",
        "county_name": "OMAHA",
        "address_line1": "1 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    recipient_location2 = {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "MO",
        "state_name": "Missouri",
        "city_name": "KANSAS CITY",
        "county_code": "013",
        "county_name": "KANSAS CITY",
        "address_line1": "1 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    recipient_location3 = {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "MO",
        "state_name": "Missouri",
        "city_name": "KANSAS CITY",
        "county_code": "013",
        "county_name": "KANSAS CITY",
        "address_line1": "7 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    recipient_location5 = {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "CO",
        "state_name": "Colorado",
        "city_name": None,
        "county_code": "013",
        "county_name": None,
        "address_line1": "1 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    recipient_location6 = {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "IL",
        "state_name": "Illinois",
        "city_name": None,
        "county_code": None,
        "county_name": None,
        "address_line1": "1 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    recipient_location7 = {
        "location_country_code": "BAH",
        "country_name": "Bahamas",
        "state_code": None,
        "state_name": None,
        "city_name": None,
        "county_name": None,
        "county_code": "013",
        "address_line1": "1 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    recipient_location8 = {
        "location_country_code": "ITA",
        "country_name": "Italy",
        "state_code": None,
        "state_name": None,
        "city_name": None,
        "county_name": None,
        "county_code": "013",
        "address_line1": "1 Memorial Drive",
        "address_line2": "Room 324",
        "address_line3": "Desk 5",
        "congressional_code": "08",
        "zip4": "9040",
        "zip5": "55455",
        "foreign_postal_code": "55455",
        "foreign_province": "Manitoba",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Recipient Location"] == recipient_location2
    assert results[1]["Recipient Location"] == recipient_location3
    assert results[2]["Recipient Location"] == recipient_location4
    assert results[3]["Recipient Location"] == recipient_location5
    assert results[4]["Recipient Location"] == recipient_location6
    assert results[5]["Recipient Location"] == recipient_location7
    assert results[6]["Recipient Location"] == recipient_location8

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "Recipient Location",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Recipient Location",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Recipient Location"] == recipient_location4
    assert results[1]["Recipient Location"] == recipient_location3
    assert results[2]["Recipient Location"] == recipient_location2
    assert results[3]["Recipient Location"] == recipient_location6
    assert results[4]["Recipient Location"] == recipient_location5
    assert results[5]["Recipient Location"] == recipient_location8
    assert results[6]["Recipient Location"] == recipient_location7


def test_spending_by_primary_place_of_performance(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "Primary Place of Performance",
        ],
        "filters": {"award_type_codes": ["09"]},
        "sort": "Primary Place of Performance",
        "order": "asc",
    }

    pop1 = {
        "location_country_code": "USA",
        "country_name": None,
        "state_code": "VA",
        "state_name": "Virginia",
        "city_name": "ARLINGTON",
        "county_code": "013",
        "county_name": None,
        "congressional_code": None,
        "zip4": None,
        "zip5": None,
    }

    pop2 = {
        "location_country_code": "USA",
        "country_name": None,
        "state_code": "VA",
        "state_name": "Virginia",
        "city_name": "Z CITY",
        "county_code": "013",
        "county_name": None,
        "congressional_code": None,
        "zip4": None,
        "zip5": None,
    }

    pop3 = {
        "location_country_code": "USA",
        "country_name": None,
        "state_code": "IL",
        "state_name": "Illinois",
        "city_name": None,
        "county_code": "013",
        "county_name": None,
        "congressional_code": None,
        "zip4": None,
        "zip5": None,
    }

    pop4 = {
        "location_country_code": "USA",
        "country_name": None,
        "state_code": "VA",
        "state_name": "Virginia",
        "city_name": None,
        "county_code": "013",
        "county_name": None,
        "congressional_code": None,
        "zip4": None,
        "zip5": None,
    }

    pop5 = {
        "location_country_code": "BAH",
        "country_name": "BAHAMAS",
        "state_code": None,
        "state_name": None,
        "city_name": None,
        "county_code": "013",
        "county_name": None,
        "congressional_code": None,
        "zip4": None,
        "zip5": None,
    }

    pop6 = {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": None,
        "state_name": None,
        "city_name": None,
        "county_code": "013",
        "county_name": None,
        "congressional_code": None,
        "zip4": None,
        "zip5": None,
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 6
    assert results[0]["Primary Place of Performance"] == pop1
    assert results[1]["Primary Place of Performance"] == pop2
    assert results[2]["Primary Place of Performance"] == pop3
    assert results[3]["Primary Place of Performance"] == pop4
    assert results[4]["Primary Place of Performance"] == pop5
    assert results[5]["Primary Place of Performance"] == pop6


def test_spending_by_award_sort_naics(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "NAICS",
        ],
        "filters": {"award_type_codes": ["05"]},
        "sort": "NAICS",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    naics_1 = {"code": "123456", "description": "1"}

    naics_2 = {"code": "123456", "description": "2"}

    naics_3 = {"code": "123457", "description": "naics description 1"}

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 3
    assert results[0]["NAICS"] == naics_1
    assert results[1]["NAICS"] == naics_2
    assert results[2]["NAICS"] == naics_3

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "NAICS",
        ],
        "filters": {"award_type_codes": ["05"]},
        "sort": "NAICS",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 3
    assert results[0]["NAICS"] == naics_3
    assert results[1]["NAICS"] == naics_2
    assert results[2]["NAICS"] == naics_1


def test_spending_by_award_sort_psc(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "PSC",
        ],
        "filters": {"award_type_codes": ["05"]},
        "sort": "PSC",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    psc1 = {"code": "PSC1", "description": "PSC description 1"}

    psc2 = {"code": "PSC1", "description": "PSC description 2"}

    psc3 = {"code": "PSC2", "description": "PSC description 1"}

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 3
    assert results[0]["PSC"] == psc1
    assert results[1]["PSC"] == psc2
    assert results[2]["PSC"] == psc3

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "PSC",
        ],
        "filters": {"award_type_codes": ["05"]},
        "sort": "PSC",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 3
    assert results[0]["PSC"] == psc3
    assert results[1]["PSC"] == psc2
    assert results[2]["PSC"] == psc1


def test_spending_by_award_assistance_listings(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "Assistance Listings",
        ],
        "filters": {"award_type_codes": ["03"]},
        "sort": "Assistance Listings",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assisance_listing1 = [{"cfda_number": "12", "cfda_program_title": "program1"}]

    assisance_listing2 = [{"cfda_number": "12", "cfda_program_title": "program2"}]

    assisance_listing3 = [{"cfda_number": "13", "cfda_program_title": "program1"}]

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 3
    assert results[0]["Assistance Listings"] == assisance_listing1
    assert results[1]["Assistance Listings"] == assisance_listing2
    assert results[2]["Assistance Listings"] == assisance_listing3

    test_payload = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "Assistance Listings",
        ],
        "filters": {"award_type_codes": ["03"]},
        "sort": "Assistance Listings",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 3
    assert results[0]["Assistance Listings"] == assisance_listing3
    assert results[1]["Assistance Listings"] == assisance_listing2
    assert results[2]["Assistance Listings"] == assisance_listing1


def test_spending_by_award_sort_sub_recipient_locations(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Recipient Location",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Recipient Location",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Recipient Location"]["city_name"] == "ARLINGTON"
    assert results[0]["Sub-Recipient Location"]["address_line1"] == "1 Memorial Drive"
    assert results[1]["Sub-Recipient Location"]["city_name"] == "ARLINGTON"
    assert results[1]["Sub-Recipient Location"]["address_line1"] == "600 CALIFORNIA STREET FL 18"
    assert results[2]["Sub-Recipient Location"]["city_name"] == "SAN FRANCISCO"
    assert results[3]["Sub-Recipient Location"]["state_code"] == "CA"
    assert results[3]["Sub-Recipient Location"]["city_name"] is None
    assert results[4]["Sub-Recipient Location"]["state_code"] == "NE"
    assert results[4]["Sub-Recipient Location"]["city_name"] is None
    assert results[5]["Sub-Recipient Location"]["country_name"] == "ARUBA"
    assert results[5]["Sub-Recipient Location"]["state_code"] is None
    assert results[6]["Sub-Recipient Location"]["country_name"] == "UNITED STATES"
    assert results[6]["Sub-Recipient Location"]["state_code"] is None

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Recipient Location",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Recipient Location",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Recipient Location"]["city_name"] == "SAN FRANCISCO"
    assert results[1]["Sub-Recipient Location"]["city_name"] == "ARLINGTON"
    assert results[1]["Sub-Recipient Location"]["address_line1"] == "600 CALIFORNIA STREET FL 18"
    assert results[2]["Sub-Recipient Location"]["city_name"] == "ARLINGTON"
    assert results[2]["Sub-Recipient Location"]["address_line1"] == "1 Memorial Drive"
    assert results[3]["Sub-Recipient Location"]["state_code"] == "NE"
    assert results[4]["Sub-Recipient Location"]["state_code"] == "CA"
    assert results[5]["Sub-Recipient Location"]["country_name"] == "UNITED STATES"
    assert results[6]["Sub-Recipient Location"]["country_name"] == "ARUBA"


def test_spending_by_award_sort_sub_pop_location(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Award Primary Place of Performance",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Award Primary Place of Performance",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Award Primary Place of Performance"]["city_name"] == "ARLINGTON"
    assert results[1]["Sub-Award Primary Place of Performance"]["city_name"] == "ARLINGTON"
    assert results[2]["Sub-Award Primary Place of Performance"]["city_name"] == "LOS ANGELES"
    assert results[3]["Sub-Award Primary Place of Performance"]["city_name"] is None
    assert results[3]["Sub-Award Primary Place of Performance"]["state_code"] == "IL"
    assert results[4]["Sub-Award Primary Place of Performance"]["city_name"] is None
    assert results[4]["Sub-Award Primary Place of Performance"]["state_code"] == "VA"
    assert results[5]["Sub-Award Primary Place of Performance"]["state_code"] is None
    assert results[5]["Sub-Award Primary Place of Performance"]["country_name"] == "LAOS"
    assert results[6]["Sub-Award Primary Place of Performance"]["state_code"] is None
    assert results[6]["Sub-Award Primary Place of Performance"]["country_name"] == "UNITED STATES"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Award Primary Place of Performance",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Award Primary Place of Performance",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Award Primary Place of Performance"]["city_name"] == "LOS ANGELES"
    assert results[1]["Sub-Award Primary Place of Performance"]["city_name"] == "ARLINGTON"
    assert results[2]["Sub-Award Primary Place of Performance"]["city_name"] == "ARLINGTON"
    assert results[3]["Sub-Award Primary Place of Performance"]["state_code"] == "VA"
    assert results[4]["Sub-Award Primary Place of Performance"]["state_code"] == "IL"
    assert results[5]["Sub-Award Primary Place of Performance"]["country_name"] == "UNITED STATES"
    assert results[6]["Sub-Award Primary Place of Performance"]["country_name"] == "LAOS"


def test_spending_by_award_sort_sub_assistance_listing(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Assistance Listing",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Assistance Listing",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Assistance Listing"]["cfda_number"] == "1.234"
    assert results[0]["Assistance Listing"]["cfda_program_title"] == "test cfda"
    assert results[1]["Assistance Listing"]["cfda_number"] == "1234"
    assert results[1]["Assistance Listing"]["cfda_program_title"] == "cfda titles 1"
    assert results[2]["Assistance Listing"]["cfda_number"] == "1234"
    assert results[2]["Assistance Listing"]["cfda_program_title"] == "cfda titles 2"
    assert results[3]["Assistance Listing"]["cfda_number"] == "9876"
    assert results[3]["Assistance Listing"]["cfda_program_title"] == "cfda titles 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Assistance Listing",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Assistance Listing",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Assistance Listing"]["cfda_number"] == "9876"
    assert results[0]["Assistance Listing"]["cfda_program_title"] == "cfda titles 1"
    assert results[1]["Assistance Listing"]["cfda_number"] == "1234"
    assert results[1]["Assistance Listing"]["cfda_program_title"] == "cfda titles 2"
    assert results[2]["Assistance Listing"]["cfda_number"] == "1234"
    assert results[2]["Assistance Listing"]["cfda_program_title"] == "cfda titles 1"


def test_spending_by_award_sort_sub_naics(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "NAICS",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "NAICS",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["NAICS"]["code"] == "1234"
    assert results[0]["NAICS"]["description"] == "naics description 1"
    assert results[1]["NAICS"]["code"] == "1234"
    assert results[1]["NAICS"]["description"] == "naics description 2"
    assert results[2]["NAICS"]["code"] == "9876"
    assert results[2]["NAICS"]["description"] == "naics description 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "NAICS",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "NAICS",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["NAICS"]["code"] == "9876"
    assert results[0]["NAICS"]["description"] == "naics description 1"
    assert results[1]["NAICS"]["code"] == "1234"
    assert results[1]["NAICS"]["description"] == "naics description 2"
    assert results[2]["NAICS"]["code"] == "1234"
    assert results[2]["NAICS"]["description"] == "naics description 1"


def test_spending_by_award_sort_sub_psc(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "PSC",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "PSC",
        "order": "asc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["PSC"]["code"] == "1234"
    assert results[0]["PSC"]["description"] == "psc description 1"
    assert results[1]["PSC"]["code"] == "1234"
    assert results[1]["PSC"]["description"] == "psc description 2"
    assert results[2]["PSC"]["code"] == "9876"
    assert results[2]["PSC"]["description"] == "psc description 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "PSC",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "PSC",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["PSC"]["code"] == "9876"
    assert results[0]["PSC"]["description"] == "psc description 1"
    assert results[1]["PSC"]["code"] == "1234"
    assert results[1]["PSC"]["description"] == "psc description 2"
    assert results[2]["PSC"]["code"] == "1234"
    assert results[2]["PSC"]["description"] == "psc description 1"


def test_spending_by_subaward_new_sort_fields(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, spending_by_award_test_data
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Prime Recipient Name",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Prime Recipient Name",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Prime Recipient Name"] == "name 7"
    assert results[6]["Prime Recipient Name"] == "name 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Prime Award Recipient UEI",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Prime Award Recipient UEI",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Prime Award Recipient UEI"] == "uei 7"
    assert results[6]["Prime Award Recipient UEI"] == "uei 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Awarding Agency",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Awarding Agency",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Awarding Agency"] == "agency 7"
    assert results[6]["Awarding Agency"] == "agency 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Awarding Sub Agency",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Awarding Sub Agency",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Awarding Sub Agency"] == "sub agency 7"
    assert results[6]["Awarding Sub Agency"] == "sub agency 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Awardee Name",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Awardee Name",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Awardee Name"] == "sub awardee 7"
    assert results[6]["Sub-Awardee Name"] == "sub awardee 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Award Description",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Award Description",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Award Description"] == "sub description 7"
    assert results[6]["Sub-Award Description"] == "sub description 1"

    test_payload = {
        "spending_level": "subawards",
        "fields": [
            "Sub-Award ID",
            "Sub-Award Type",
        ],
        "filters": {"award_type_codes": ["08"]},
        "sort": "Sub-Award Type",
        "order": "desc",
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    results = resp.json().get("results")
    assert len(results) == 7
    assert results[0]["Sub-Award Type"] == "sub type 7"
    assert results[6]["Sub-Award Type"] == "sub type 1"


@pytest.mark.django_db
def test_covid_and_iija_values(client, monkeypatch, elasticsearch_award_index, award_data_fixture):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    request_body = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "COVID-19 Obligations",
            "COVID-19 Outlays",
            "Infrastructure Obligations",
            "Infrastructure Outlays",
        ],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "award_ids": ["award200"],
            "time_period": [{"start_date": "2008-01-01", "end_date": "2015-12-31"}],
            "award_type_codes": ["07"],
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_body)
    )
    expected_result = [
        {
            "internal_id": 200,
            "generated_internal_id": "ASST_NON_DECF0000058_8900",
            "Award ID": "award200",
            "COVID-19 Obligations": None,
            "COVID-19 Outlays": None,
            "Infrastructure Obligations": None,
            "Infrastructure Outlays": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == expected_result

    request_body = {
        "spending_level": "awards",
        "fields": [
            "Award ID",
            "COVID-19 Obligations",
            "COVID-19 Outlays",
            "Infrastructure Obligations",
            "Infrastructure Outlays",
        ],
        "sort": "Award ID",
        "limit": 50,
        "page": 1,
        "filters": {
            "award_ids": ["award300"],
            "time_period": [{"start_date": "2008-01-01", "end_date": "2015-12-31"}],
            "award_type_codes": ["IDV_B_A"],
        },
    }

    resp = client.post(
        "/api/v2/search/spending_by_award/", content_type="application/json", data=json.dumps(request_body)
    )
    expected_result = [
        {
            "internal_id": 300,
            "generated_internal_id": "CONT_IDV_YUGGY2_8900",
            "Award ID": "award300",
            "COVID-19 Obligations": 10.0,
            "COVID-19 Outlays": 100.0,
            "Infrastructure Obligations": 20.0,
            "Infrastructure Outlays": 200.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["results"] == expected_result
