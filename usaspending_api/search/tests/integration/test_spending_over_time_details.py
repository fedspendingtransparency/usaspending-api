import json
from datetime import datetime

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year, generate_date_range
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

expected_messages = [
    get_time_period_message(),
    (
        "The 'subawards' field will be deprecated in the future. "
        "Set 'spending_level' to 'subawards' instead. See documentation for more information."
    ),
    (
        "You may see additional month, quarter and year results when searching for "
        "Awards or Subawards. This is due to Awards or Subawards overlapping with the "
        "time period specified but having an 'action date' outside of that time period."
    ),
]


@pytest.fixture
def populate_models(db):
    ts1 = baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        award_category="direct payment",
        action_date=datetime(2010, 3, 1),
        fiscal_action_date=datetime(2010, 6, 1),
        federal_action_obligation=100.00,
        generated_pragmatic_obligation=100.00,
        award_date_signed=datetime(2010, 2, 15),
    )
    ts2 = baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        award_category="idv",
        action_date=datetime(2011, 3, 1),
        fiscal_action_date=datetime(2011, 6, 1),
        federal_action_obligation=110.00,
        generated_pragmatic_obligation=110.00,
        award_date_signed=datetime(2012, 2, 15),
    )
    ts3 = baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        award_category="loans",
        action_date=datetime(2012, 3, 1),
        fiscal_action_date=datetime(2012, 6, 1),
        federal_action_obligation=120.00,
        generated_pragmatic_obligation=120.00,
    )
    ts4 = baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        award_category="other",
        action_date=datetime(2013, 3, 1),
        fiscal_action_date=datetime(2013, 6, 1),
        federal_action_obligation=130.00,
        generated_pragmatic_obligation=130.00,
    )
    ts5 = baker.make(
        "search.TransactionSearch",
        transaction_id=5,
        award_id=5,
        award_category="grant",
        action_date=datetime(2014, 3, 1),
        fiscal_action_date=datetime(2014, 6, 1),
        federal_action_obligation=140.00,
        generated_pragmatic_obligation=140.00,
    )
    ts6 = baker.make(
        "search.TransactionSearch",
        transaction_id=6,
        award_id=6,
        award_category="grant",
        action_date=datetime(2015, 3, 1),
        fiscal_action_date=datetime(2015, 6, 1),
        federal_action_obligation=150.00,
        generated_pragmatic_obligation=150.00,
    )
    ts7 = baker.make(
        "search.TransactionSearch",
        transaction_id=7,
        award_id=7,
        award_category="grant",
        action_date=datetime(2016, 3, 1),
        fiscal_action_date=datetime(2016, 6, 1),
        federal_action_obligation=160.00,
        generated_pragmatic_obligation=160.00,
    )
    ts8 = baker.make(
        "search.TransactionSearch",
        transaction_id=8,
        award_id=8,
        award_category="loans",
        action_date=datetime(2017, 3, 1),
        fiscal_action_date=datetime(2017, 6, 1),
        federal_action_obligation=170.00,
        generated_pragmatic_obligation=170.00,
    )

    baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=1,
        latest_transaction_search=ts1,
        category="direct payment",
        action_date=ts1.action_date,
        date_signed=datetime(2010, 3, 1),
        fiscal_year=generate_fiscal_year(ts1.fiscal_action_date),
        total_outlays=0.00,
        generated_pragmatic_obligation=ts1.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        latest_transaction_id=2,
        latest_transaction_search=ts2,
        category="idv",
        action_date=ts2.action_date,
        date_signed=datetime(2010, 3, 1),
        fiscal_year=generate_fiscal_year(ts2.fiscal_action_date),
        total_outlays=10.00,
        generated_pragmatic_obligation=ts2.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        latest_transaction_id=3,
        latest_transaction_search=ts3,
        category="loans",
        action_date=ts3.action_date,
        date_signed=datetime(2011, 3, 1),
        fiscal_year=generate_fiscal_year(ts3.fiscal_action_date),
        total_outlays=20.00,
        generated_pragmatic_obligation=ts3.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=4,
        latest_transaction_id=4,
        latest_transaction_search=ts4,
        category="other",
        action_date=ts4.action_date,
        date_signed=datetime(2012, 3, 1),
        fiscal_year=generate_fiscal_year(ts4.fiscal_action_date),
        total_outlays=30.00,
        generated_pragmatic_obligation=ts4.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=5,
        latest_transaction_id=5,
        latest_transaction_search=ts5,
        category="grant",
        action_date=ts5.action_date,
        date_signed=datetime(2013, 3, 1),
        fiscal_year=generate_fiscal_year(ts5.fiscal_action_date),
        total_outlays=40.00,
        generated_pragmatic_obligation=ts5.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=6,
        latest_transaction_id=6,
        latest_transaction_search=ts6,
        category="grant",
        action_date=ts6.action_date,
        date_signed=datetime(2014, 3, 1),
        fiscal_year=generate_fiscal_year(ts6.fiscal_action_date),
        total_outlays=50.00,
        generated_pragmatic_obligation=ts6.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=7,
        latest_transaction_id=7,
        latest_transaction_search=ts7,
        category="grant",
        action_date=ts7.action_date,
        date_signed=datetime(2015, 3, 1),
        fiscal_year=generate_fiscal_year(ts7.fiscal_action_date),
        total_outlays=60.00,
        generated_pragmatic_obligation=ts7.generated_pragmatic_obligation,
    )
    baker.make(
        "search.AwardSearch",
        award_id=8,
        latest_transaction_id=8,
        latest_transaction_search=ts8,
        category="loans",
        action_date=ts8.action_date,
        date_signed=datetime(2016, 3, 1),
        fiscal_year=generate_fiscal_year(ts8.fiscal_action_date),
        total_outlays=70.00,
        generated_pragmatic_obligation=ts8.generated_pragmatic_obligation,
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


def shared_results_transactions():
    return [
        {
            "aggregated_amount": 100.00,
            "time_period": {"fiscal_year": "2010"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 100.0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 110.00,
            "time_period": {"fiscal_year": "2011"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 110.0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 120.00,
            "time_period": {"fiscal_year": "2012"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 120.0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 130.00,
            "time_period": {"fiscal_year": "2013"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 130.0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 140.00,
            "time_period": {"fiscal_year": "2014"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 140.0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 150.00,
            "time_period": {"fiscal_year": "2015"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 150.0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 160.00,
            "time_period": {"fiscal_year": "2016"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 160.0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 170.00,
            "time_period": {"fiscal_year": "2017"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 170.0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0,
            "time_period": {"fiscal_year": "2018"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
    ]


def shared_results_2():
    return [
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "5"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 100.00,
            "time_period": {"fiscal_year": "2010", "month": "6"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 100.0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "7"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "8"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "9"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "10"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "11"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2010", "month": "12"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2011", "month": "1"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2011", "month": "2"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2011", "month": "3"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2011", "month": "4"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 0.00,
            "time_period": {"fiscal_year": "2011", "month": "5"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
        {
            "aggregated_amount": 110.00,
            "time_period": {"fiscal_year": "2011", "month": "6"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 110.0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": None,
            "Contract_Outlays": None,
            "Direct_Outlays": None,
            "Grant_Outlays": None,
            "Idv_Outlays": None,
            "Loan_Outlays": None,
            "Other_Outlays": None,
        },
    ]


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
def test_spending_over_time_fy_ordering_transactions(
    client, monkeypatch, elasticsearch_transaction_index, populate_models
):
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
        "results": shared_results_transactions(),
        "spending_level": "transactions",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db
def test_spending_over_time_fy_ordering_awards(client, monkeypatch, elasticsearch_award_index, populate_models):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    group = "fiscal_year"

    test_payload = {
        "group": group,
        "spending_level": "awards",
        "filters": {
            "time_period": [
                {"start_date": "2009-10-01", "end_date": "2010-09-30"},
                {"start_date": "2010-10-01", "end_date": "2011-09-30"},
            ]
        },
    }

    expected_response = {
        "group": group,
        "spending_level": "awards",
        "results": [
            {
                "Contract_Obligations": 0,
                "Contract_Outlays": 0,
                "Direct_Obligations": 100.0,
                "Direct_Outlays": 0.0,
                "Grant_Obligations": 0,
                "Grant_Outlays": 0,
                "Idv_Obligations": 0,
                "Idv_Outlays": 0,
                "Loan_Obligations": 0,
                "Loan_Outlays": 0,
                "Other_Obligations": 0,
                "Other_Outlays": 0,
                "aggregated_amount": 100.0,
                "time_period": {"fiscal_year": "2010"},
                "total_outlays": 0.0,
            },
            {
                "Contract_Obligations": 0,
                "Contract_Outlays": 0,
                "Direct_Obligations": 0,
                "Direct_Outlays": 0,
                "Grant_Obligations": 0,
                "Grant_Outlays": 0,
                "Idv_Obligations": 110.0,
                "Idv_Outlays": 10.0,
                "Loan_Obligations": 0,
                "Loan_Outlays": 0,
                "Other_Obligations": 0,
                "Other_Outlays": 0,
                "aggregated_amount": 110.0,
                "time_period": {"fiscal_year": "2011"},
                "total_outlays": 10.0,
            },
            {
                "Contract_Obligations": 0,
                "Contract_Outlays": 0,
                "Direct_Obligations": 0,
                "Direct_Outlays": 0,
                "Grant_Obligations": 0,
                "Grant_Outlays": 0,
                "Idv_Obligations": 0,
                "Idv_Outlays": 0,
                "Loan_Obligations": 120.0,
                "Loan_Outlays": 20.0,
                "Other_Obligations": 0,
                "Other_Outlays": 0,
                "aggregated_amount": 120.0,
                "time_period": {"fiscal_year": "2012"},
                "total_outlays": 20.0,
            },
        ],
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json(), "Unexpected or missing content!"

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
        "spending_level": "transactions",
        "results": shared_results_transactions(),
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db
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
        "spending_level": "transactions",
        "results": [
            {
                "time_period": {"fiscal_year": "2011", "month": "1"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "2"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "3"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "4"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "5"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "6"},
                "aggregated_amount": 110.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 110.0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "7"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "8"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "9"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "10"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "11"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2011", "month": "12"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "1"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "2"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "3"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "4"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "5"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "6"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "7"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "8"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "9"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "10"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "11"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2012", "month": "12"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "1"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "2"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "3"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "4"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "5"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "6"},
                "aggregated_amount": 130.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 130.0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "7"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "8"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "9"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "10"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "11"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "time_period": {"fiscal_year": "2013", "month": "12"},
                "aggregated_amount": 0.00,
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
        ],
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db
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
        "messages": expected_messages,
    }

    expected_response = {
        "results": shared_results_2(),
        "spending_level": "transactions",
        "group": "month",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db
def test_spending_over_time_new_awards_only_filter(
    client, monkeypatch, elasticsearch_transaction_index, populate_models
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Test where awards have a base transaction date within the specified time period filter bounds
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {
                    "date_type": "date_signed",
                    "start_date": "2010-02-01",
                    "end_date": "2010-03-31",
                },
            ]
        },
        "messages": expected_messages,
    }

    expected_response = {
        "results": [
            {
                "aggregated_amount": 0,
                "time_period": {"fiscal_year": "2010", "month": "5"},
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "aggregated_amount": 100.00,
                "time_period": {"fiscal_year": "2010", "month": "6"},
                "Contract_Obligations": 0,
                "Direct_Obligations": 100.0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
        ],
        "group": "month",
        "spending_level": "transactions",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])

    # Test where awards do not have a base transaction date within the specified time period filter bounds
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {
                    "date_type": "date_signed",
                    "start_date": "2010-02-01",
                    "end_date": "2010-02-02",
                },
            ]
        },
        "messages": expected_messages,
    }

    expected_response = {
        "results": [
            {
                "aggregated_amount": 0,
                "time_period": {"fiscal_year": "2010", "month": "5"},
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
        ],
        "group": "month",
        "spending_level": "transactions",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

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
                {
                    "date_type": "date_signed",
                    "start_date": "2010-02-01",
                    "end_date": "2010-03-31",
                },
                {"start_date": "2011-02-01", "end_date": "2011-03-31"},
            ]
        },
        "messages": expected_messages,
    }

    expected_response = {
        "results": shared_results_2(),
        "group": "month",
        "spending_level": "transactions",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])

    # Test that new awards only set to true requires action date to be in bounds
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {
                    "date_type": "new_awards_only",
                    "start_date": "2010-02-14",
                    "end_date": "2010-02-16",
                },
            ]
        },
        "messages": expected_messages,
    }

    expected_response = {
        "results": [
            {
                "aggregated_amount": 0.00,
                "time_period": {"fiscal_year": "2010", "month": "5"},
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
        ],
        "group": "month",
        "spending_level": "transactions",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])

    # Test that new awards only set to false doesn't require action date to be in bounds
    group = "month"
    test_payload = {
        "group": group,
        "subawards": False,
        "filters": {
            "time_period": [
                {
                    "date_type": "new_awards_only",
                    "start_date": "2010-02-13",
                    "end_date": "2010-03-02",
                },
            ]
        },
        "messages": expected_messages,
    }

    expected_response = {
        "results": [
            {
                "aggregated_amount": 0.00,
                "time_period": {"fiscal_year": "2010", "month": "5"},
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
            {
                "aggregated_amount": 100.00,
                "time_period": {"fiscal_year": "2010", "month": "6"},
                "Contract_Obligations": 0,
                "Direct_Obligations": 100.0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Direct_Outlays": None,
                "Grant_Outlays": None,
                "Idv_Outlays": None,
                "Loan_Outlays": None,
                "Other_Outlays": None,
            },
        ],
        "group": "month",
        "spending_level": "transactions",
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db
def test_spending_over_time_month_awards(client, monkeypatch, elasticsearch_award_index, populate_models):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    group = "month"

    test_payload = {
        "group": group,
        "spending_level": "awards",
        "filters": {
            "time_period": [
                {"start_date": "2009-10-01", "end_date": "2010-09-30"},
            ]
        },
    }
    populated_results = {
        (2010, 6): {
            "aggregated_amount": 100.0,
            "time_period": {"fiscal_year": "2010", "month": "6"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 100.0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": 0.0,
            "Contract_Outlays": 0,
            "Direct_Outlays": 0,
            "Grant_Outlays": 0,
            "Idv_Outlays": 0,
            "Loan_Outlays": 0,
            "Other_Outlays": 0,
        },
        (2011, 6): {
            "aggregated_amount": 110.0,
            "time_period": {"fiscal_year": "2011", "month": "6"},
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 110.0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
            "total_outlays": 10.0,
            "Contract_Outlays": 0,
            "Direct_Outlays": 0,
            "Grant_Outlays": 0,
            "Idv_Outlays": 10.0,
            "Loan_Outlays": 0,
            "Other_Outlays": 0,
        },
    }
    expected_response = {
        "group": group,
        "spending_level": "awards",
        "results": [
            populated_results.get(
                (date_range["fiscal_year"], date_range["fiscal_month"]),
                {
                    "aggregated_amount": 0,
                    "time_period": {
                        "fiscal_year": str(date_range["fiscal_year"]),
                        "month": str(date_range["fiscal_month"]),
                    },
                    "Contract_Obligations": 0,
                    "Direct_Obligations": 0,
                    "Grant_Obligations": 0,
                    "Idv_Obligations": 0,
                    "Loan_Obligations": 0,
                    "Other_Obligations": 0,
                    "total_outlays": 0.0,
                    "Contract_Outlays": 0,
                    "Direct_Outlays": 0,
                    "Grant_Outlays": 0,
                    "Idv_Outlays": 0,
                    "Loan_Outlays": 0,
                    "Other_Outlays": 0,
                },
            )
            for date_range in generate_date_range(datetime(2009, 10, 1), datetime(2011, 6, 1), group)
        ],
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json(), "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])


@pytest.mark.django_db
def test_spending_over_time_month_subawards(client, monkeypatch, elasticsearch_subaward_index, populate_models):
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        action_date="2010-11-01",
        sub_action_date="2010-11-01",
        subaward_amount=500.00,
        subaward_type="grant",
    )
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    group = "month"

    test_payload = {
        "group": group,
        "spending_level": "subawards",
        "filters": {
            "time_period": [
                {"start_date": "2010-10-01", "end_date": "2011-09-30"},
            ]
        },
    }

    expected_response = {
        "group": group,
        "spending_level": "subawards",
        "results": [
            {
                "aggregated_amount": 500.0 if month == 2 else 0,
                "time_period": {"fiscal_year": "2011", "month": str(month)},
                "Contract_Obligations": 0,
                "Grant_Obligations": 500.0 if month == 2 else 0,
                "total_outlays": None,
                "Contract_Outlays": None,
                "Grant_Outlays": None,
            }
            for month in range(1, 13)
        ],
        "messages": expected_messages,
    }

    resp = client.post(
        get_spending_over_time_url(),
        content_type="application/json",
        data=json.dumps(test_payload),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json(), "Unexpected or missing content!"

    # ensure ordering is correct
    confirm_proper_ordering(group, resp.data["results"])
