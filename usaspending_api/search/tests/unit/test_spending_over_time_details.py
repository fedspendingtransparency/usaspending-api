import json
import pytest

from datetime import datetime
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def populate_models(db):
    baker.make("awards.Award", id=1, latest_transaction_id=1)
    baker.make("awards.Award", id=2, latest_transaction_id=2)
    baker.make("awards.Award", id=3, latest_transaction_id=3)
    baker.make("awards.Award", id=4, latest_transaction_id=4)
    baker.make("awards.Award", id=5, latest_transaction_id=5)
    baker.make("awards.Award", id=6, latest_transaction_id=6)
    baker.make("awards.Award", id=7, latest_transaction_id=7)
    baker.make("awards.Award", id=8, latest_transaction_id=8)

    baker.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        action_date=datetime(2010, 3, 1),
        federal_action_obligation=100.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        action_date=datetime(2011, 3, 1),
        federal_action_obligation=110.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=3,
        award_id=3,
        action_date=datetime(2012, 3, 1),
        federal_action_obligation=120.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=4,
        award_id=4,
        action_date=datetime(2013, 3, 1),
        federal_action_obligation=130.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=5,
        award_id=5,
        action_date=datetime(2014, 3, 1),
        federal_action_obligation=140.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=6,
        award_id=6,
        action_date=datetime(2015, 3, 1),
        federal_action_obligation=150.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=7,
        award_id=7,
        action_date=datetime(2016, 3, 1),
        federal_action_obligation=160.0,
    )
    baker.make(
        "awards.TransactionNormalized",
        id=8,
        award_id=8,
        action_date=datetime(2017, 3, 1),
        federal_action_obligation=170.0,
    )


@pytest.fixture
def pragmatic_fixture():
    baker.make(
        "awards.TransactionNormalized",
        id=1,
        action_date="2010-10-01",
        award_id=1,
        is_fpds=True,
        original_loan_subsidy_cost=0,
        federal_action_obligation=9900,
        type="A",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=2,
        action_date="2019-01-19",
        award_id=2,
        is_fpds=False,
        original_loan_subsidy_cost=1000000,
        federal_action_obligation=0,
        type="07",
    )
    baker.make("awards.TransactionFPDS", transaction_id=1, piid="piiiiid", federal_action_obligation=9900)
    baker.make(
        "awards.TransactionFABS",
        transaction_id=2,
        fain="faiiin",
        original_loan_subsidy_cost=1000000,
        federal_action_obligation=None,
    )
    baker.make(
        "awards.Award",
        id=1,
        latest_transaction_id=1,
        piid="piiiiid",
        type="A",
        category="not loans",
        total_subsidy_cost=-1,
    )
    baker.make(
        "awards.Award",
        id=2,
        latest_transaction_id=2,
        piid="faiiin",
        type="07",
        category="loans",
        total_subsidy_cost=444444444,
    )


def get_spending_over_time_url():
    return "/api/v2/search/spending_over_time/"


def confirm_proper_ordering(group, results):
    fiscal_year, period = 0, 0
    for result in results:
        assert int(result["time_period"]["fiscal_year"]) >= fiscal_year, "Fiscal Year is out of order!"
        if int(result["time_period"]["fiscal_year"]) > fiscal_year:
            fiscal_year = int(result["time_period"]["fiscal_year"])
            period = 0
        if group != "fiscal_year":
            assert int(result["time_period"]["fiscal_year"]) >= period, "{} is out of order!".format(group)
            if int(result["time_period"][group]) > period:
                period = int(result["time_period"][group])


def test_spending_over_time_fy_ordering(client, monkeypatch, elasticsearch_transaction_index, populate_models):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    group = "fiscal_year"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {"start_date": "2009-10-01", "end_date": "2017-09-30"},
                {"start_date": "2017-10-01", "end_date": "2018-09-30"},
            ]
        },
    }

    expected_response = {
        "group": group,
        "results": [
            {"aggregated_amount": 100.0, "time_period": {"fiscal_year": "2010"}},
            {"aggregated_amount": 110.0, "time_period": {"fiscal_year": "2011"}},
            {"aggregated_amount": 120.0, "time_period": {"fiscal_year": "2012"}},
            {"aggregated_amount": 130.0, "time_period": {"fiscal_year": "2013"}},
            {"aggregated_amount": 140.0, "time_period": {"fiscal_year": "2014"}},
            {"aggregated_amount": 150.0, "time_period": {"fiscal_year": "2015"}},
            {"aggregated_amount": 160.0, "time_period": {"fiscal_year": "2016"}},
            {"aggregated_amount": 170.0, "time_period": {"fiscal_year": "2017"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2018"}},
        ],
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


def test_spending_over_time_month_ordering(client, monkeypatch, elasticsearch_transaction_index, populate_models):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {"start_date": "2010-10-01", "end_date": "2011-09-30"},
                {"start_date": "2012-10-01", "end_date": "2013-09-30"},
            ]
        },
    }
    expected_response = {
        "group": group,
        "results": [
            {"time_period": {"fiscal_year": "2011", "month": "1"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "2"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "3"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "4"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "5"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "6"}, "aggregated_amount": 110.0},
            {"time_period": {"fiscal_year": "2011", "month": "7"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "8"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "9"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "10"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "11"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2011", "month": "12"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "1"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "2"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "3"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "4"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "5"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "6"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "7"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "8"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "9"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "10"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "11"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2012", "month": "12"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "1"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "2"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "3"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "4"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "5"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "6"}, "aggregated_amount": 130.0},
            {"time_period": {"fiscal_year": "2013", "month": "7"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "8"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "9"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "10"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "11"}, "aggregated_amount": 0},
            {"time_period": {"fiscal_year": "2013", "month": "12"}, "aggregated_amount": 0},
        ],
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


def test_spending_over_time_funny_dates_ordering(client, monkeypatch, elasticsearch_transaction_index, populate_models):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {"start_date": "2010-02-01", "end_date": "2010-03-31"},
                {"start_date": "2011-02-01", "end_date": "2011-03-31"},
            ]
        },
        "messages": [get_time_period_message()],
    }

    expected_response = {
        "results": [
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "5"}},
            {"aggregated_amount": 100.0, "time_period": {"fiscal_year": "2010", "month": "6"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "7"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "8"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "9"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "10"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "11"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "12"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011", "month": "1"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011", "month": "2"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011", "month": "3"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011", "month": "4"}},
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2011", "month": "5"}},
            {"aggregated_amount": 110.0, "time_period": {"fiscal_year": "2011", "month": "6"}},
        ],
        "group": "month",
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])
