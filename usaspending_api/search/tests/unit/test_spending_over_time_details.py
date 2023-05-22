import json
import pytest

from datetime import datetime
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def populate_models(db):
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1)
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2)
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=3)
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=4)
    baker.make("search.AwardSearch", award_id=5, latest_transaction_id=5)
    baker.make("search.AwardSearch", award_id=6, latest_transaction_id=6)
    baker.make("search.AwardSearch", award_id=7, latest_transaction_id=7)
    baker.make("search.AwardSearch", award_id=8, latest_transaction_id=8)

    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        action_date=datetime(2010, 3, 1),
        fiscal_action_date=datetime(2010, 6, 1),
        federal_action_obligation=100.00,
        generated_pragmatic_obligation=100.00,
        award_date_signed=datetime(2010, 2, 15),
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        action_date=datetime(2011, 3, 1),
        fiscal_action_date=datetime(2011, 6, 1),
        federal_action_obligation=110.00,
        generated_pragmatic_obligation=110.00,
        award_date_signed=datetime(2012, 2, 15),
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        action_date=datetime(2012, 3, 1),
        fiscal_action_date=datetime(2012, 6, 1),
        federal_action_obligation=120.00,
        generated_pragmatic_obligation=120.00,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        action_date=datetime(2013, 3, 1),
        fiscal_action_date=datetime(2013, 6, 1),
        federal_action_obligation=130.00,
        generated_pragmatic_obligation=130.00,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=5,
        award_id=5,
        action_date=datetime(2014, 3, 1),
        fiscal_action_date=datetime(2014, 6, 1),
        federal_action_obligation=140.00,
        generated_pragmatic_obligation=140.00,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=6,
        award_id=6,
        action_date=datetime(2015, 3, 1),
        fiscal_action_date=datetime(2015, 6, 1),
        federal_action_obligation=150.00,
        generated_pragmatic_obligation=150.00,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=7,
        award_id=7,
        action_date=datetime(2016, 3, 1),
        fiscal_action_date=datetime(2016, 6, 1),
        federal_action_obligation=160.00,
        generated_pragmatic_obligation=160.00,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=8,
        award_id=8,
        action_date=datetime(2017, 3, 1),
        fiscal_action_date=datetime(2017, 6, 1),
        federal_action_obligation=170.00,
        generated_pragmatic_obligation=170.00,
    )


@pytest.fixture
def pragmatic_fixture():
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        action_date="2010-10-01",
        fiscal_action_date="2011-01-01",
        award_id=1,
        is_fpds=True,
        original_loan_subsidy_cost=0.00,
        federal_action_obligation=9900.00,
        generated_pragmatic_obligation=9900.00,
        type="A",
        piid="piiiiid",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        action_date="2019-01-19",
        fiscal_action_date="2019-04-19",
        award_id=2,
        is_fpds=False,
        original_loan_subsidy_cost=1000000.00,
        federal_action_obligation=0.00,
        generated_pragmatic_obligation=1000000.00,
        type="07",
        fain="faiiin",
    )
    baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=1,
        piid="piiiiid",
        type="A",
        category="not loans",
        total_subsidy_cost=-1,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        latest_transaction_id=2,
        piid="faiiin",
        type="07",
        category="loans",
        total_subsidy_cost=444444444,
    )


def get_spending_over_time_url():
    return "/api/v2/search/spending_over_time/"


def confirm_proper_ordering(group, results):
    fiscal_year, period = 0.00, 0.00
    for result in results:
        assert int(result["time_period"]["fiscal_year"]) >= fiscal_year, "Fiscal Year is out of order!"
        if int(result["time_period"]["fiscal_year"]) > fiscal_year:
            fiscal_year = int(result["time_period"]["fiscal_year"])
            period = 0.00
        if group != "fiscal_year":
            assert int(result["time_period"]["fiscal_year"]) >= period, "{} is out of order!".format(group)
            if int(result["time_period"][group]) > period:
                period = int(result["time_period"][group])


@pytest.mark.django_db(transaction=True)
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
            {"aggregated_amount": 100.00, "time_period": {"fiscal_year": "2010"}},
            {"aggregated_amount": 110.00, "time_period": {"fiscal_year": "2011"}},
            {"aggregated_amount": 120.00, "time_period": {"fiscal_year": "2012"}},
            {"aggregated_amount": 130.00, "time_period": {"fiscal_year": "2013"}},
            {"aggregated_amount": 140.00, "time_period": {"fiscal_year": "2014"}},
            {"aggregated_amount": 150.00, "time_period": {"fiscal_year": "2015"}},
            {"aggregated_amount": 160.00, "time_period": {"fiscal_year": "2016"}},
            {"aggregated_amount": 170.00, "time_period": {"fiscal_year": "2017"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2018"}},
        ],
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db(transaction=True)
def test_spending_over_time_default_date_type(client, monkeypatch, elasticsearch_transaction_index, populate_models):
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
            {"aggregated_amount": 100.00, "time_period": {"fiscal_year": "2010"}},
            {"aggregated_amount": 110.00, "time_period": {"fiscal_year": "2011"}},
            {"aggregated_amount": 120.00, "time_period": {"fiscal_year": "2012"}},
            {"aggregated_amount": 130.00, "time_period": {"fiscal_year": "2013"}},
            {"aggregated_amount": 140.00, "time_period": {"fiscal_year": "2014"}},
            {"aggregated_amount": 150.00, "time_period": {"fiscal_year": "2015"}},
            {"aggregated_amount": 160.00, "time_period": {"fiscal_year": "2016"}},
            {"aggregated_amount": 170.00, "time_period": {"fiscal_year": "2017"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2018"}},
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
            {"time_period": {"fiscal_year": "2011", "month": "1"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "2"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "3"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "4"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "5"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "6"}, "aggregated_amount": 110.00},
            {"time_period": {"fiscal_year": "2011", "month": "7"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "8"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "9"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "10"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "11"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2011", "month": "12"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "1"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "2"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "3"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "4"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "5"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "6"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "7"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "8"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "9"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "10"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "11"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2012", "month": "12"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "1"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "2"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "3"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "4"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "5"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "6"}, "aggregated_amount": 130.00},
            {"time_period": {"fiscal_year": "2013", "month": "7"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "8"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "9"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "10"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "11"}, "aggregated_amount": 0.00},
            {"time_period": {"fiscal_year": "2013", "month": "12"}, "aggregated_amount": 0.00},
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
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "5"}},
            {"aggregated_amount": 100.00, "time_period": {"fiscal_year": "2010", "month": "6"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "7"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "8"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "9"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "10"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "11"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "12"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "1"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "2"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "3"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "4"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "5"}},
            {"aggregated_amount": 110.00, "time_period": {"fiscal_year": "2011", "month": "6"}},
        ],
        "group": "month",
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


def test_spending_over_time_new_awards_only_filter(
    client, monkeypatch, elasticsearch_transaction_index, populate_models
):
    # Testing the ability for spending over time to consume the "date_type": "date_signed" value
    # The business considers any awards whose base transaction falls within two dates as a new award
    # See, [DEV-8603] (https://federal-spending-transparency.atlassian.net/browse/DEV-8603)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Test where awards have a base transaction date within the specified time period filter bounds
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {"date_type": "date_signed", "start_date": "2010-02-01", "end_date": "2010-03-31"},
            ]
        },
        "messages": [get_time_period_message()],
    }

    expected_response = {
        "results": [
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "5"}},
            {"aggregated_amount": 100.00, "time_period": {"fiscal_year": "2010", "month": "6"}},
        ],
        "group": "month",
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])

    # Test where awards do not a base transaction date within the specified time period filter bounds
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {"date_type": "date_signed", "start_date": "2010-02-01", "end_date": "2010-02-02"},
            ]
        },
        "messages": [get_time_period_message()],
    }

    expected_response = {
        "results": [
            {"aggregated_amount": 0, "time_period": {"fiscal_year": "2010", "month": "5"}},
        ],
        "group": "month",
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])

    # Test where awards multiple time period filter bounds provided
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {"date_type": "date_signed", "start_date": "2010-02-01", "end_date": "2010-03-31"},
                {"start_date": "2011-02-01", "end_date": "2011-03-31"},
            ]
        },
        "messages": [get_time_period_message()],
    }

    expected_response = {
        "results": [
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "5"}},
            {"aggregated_amount": 100.00, "time_period": {"fiscal_year": "2010", "month": "6"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "7"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "8"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "9"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "10"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "11"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2010", "month": "12"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "1"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "2"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "3"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "4"}},
            {"aggregated_amount": 0.00, "time_period": {"fiscal_year": "2011", "month": "5"}},
            {"aggregated_amount": 110.00, "time_period": {"fiscal_year": "2011", "month": "6"}},
        ],
        "group": "month",
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_spending_over_time_url(), content_type="application/json", data=json.dumps(test_payload))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])
