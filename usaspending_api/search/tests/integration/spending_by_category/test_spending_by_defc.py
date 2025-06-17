import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_financial_data():
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="L",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-06",
        title="DEFC L",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        group_name="covid_19",
        earliest_public_law_enactment_date="2020-03-06",
        title="DEFC M",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Q",
        group_name=None,
        earliest_public_law_enactment_date="2020-03-06",
        title="DEFC Q",
    )
    baker.make(
        "references.DisasterEmergencyFundCode",
        code="Z",
        group_name="infrastructure",
        earliest_public_law_enactment_date="2020-03-06",
        title="DEFC Z",
    )

    baker.make(
        "search.AwardSearch",
        award_id=1,
        display_award_id=1,
        action_date="2020-01-01",
        spending_by_defc=None,
        disaster_emergency_fund_codes=None,
        generated_pragmatic_obligation=1000,
        total_outlays=10000,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        display_award_id=2,
        action_date="2020-01-01",
        spending_by_defc=[{"defc": "L", "outlay": 100.0, "obligation": 10.0}],
        disaster_emergency_fund_codes=["L"],
        total_covid_obligation=10.0,
        total_covid_outlay=100.0,
        total_iija_obligation=0.0,
        total_iija_outlay=0.0,
        generated_pragmatic_obligation=10.0,
        total_outlays=100.0,
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        display_award_id=3,
        action_date="2020-01-01",
        spending_by_defc=[
            {"defc": "L", "outlay": 200.0, "obligation": 20.0},
            {"defc": "M", "outlay": 300.0, "obligation": 30.0},
        ],
        disaster_emergency_fund_codes=["L", "M"],
        total_covid_obligation=50.0,
        total_covid_outlay=500.0,
        total_iija_obligation=0.0,
        total_iija_outlay=0.0,
        generated_pragmatic_obligation=50.0,
        total_outlays=500.0,
    )
    baker.make(
        "search.AwardSearch",
        award_id=4,
        display_award_id=4,
        action_date="2020-01-01",
        spending_by_defc=[
            {"defc": "M", "outlay": 400.0, "obligation": 40.0},
            {"defc": "Q", "outlay": 500.0, "obligation": 50.0},
        ],
        disaster_emergency_fund_codes=["M", "Q"],
        total_covid_obligation=40.0,
        total_covid_outlay=400.0,
        total_iija_obligation=0.0,
        total_iija_outlay=0.0,
        generated_pragmatic_obligation=90.0,
        total_outlays=900.0,
    )
    baker.make(
        "search.AwardSearch",
        award_id=5,
        display_award_id=5,
        action_date="2020-01-01",
        spending_by_defc=[
            {"defc": "Z", "outlay": 600.0, "obligation": 60.0},
            {"defc": "Q", "outlay": 700.0, "obligation": 70.0},
        ],
        disaster_emergency_fund_codes=["Z", "Q"],
        total_covid_obligation=0.0,
        total_covid_outlay=0.0,
        total_iija_obligation=60.0,
        total_iija_outlay=600.0,
        generated_pragmatic_obligation=130.0,
        total_outlays=1300.0,
    )


def _expected_messages():
    expected_messages = [get_time_period_message()]
    expected_messages.append(
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
        "See documentation for more information. "
    )
    return expected_messages


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]}}),
    )
    expected_response = {
        "category": "defc",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


@pytest.mark.django_db
def test_award_financial_spending_level(client, monkeypatch, elasticsearch_award_index, award_financial_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps({"spending_level": "award_financial", "filters": {"award_ids": ["1", "2", "3", "4", "5"]}}),
    )
    expected_result = [
        {
            "id": None,
            "code": "Q",
            "name": "DEFC Q",
            "amount": 120.0,
            "total_outlays": 1200.0,
        },
        {
            "id": None,
            "code": "M",
            "name": "DEFC M",
            "amount": 70.0,
            "total_outlays": 700.0,
        },
        {
            "id": None,
            "code": "Z",
            "name": "DEFC Z",
            "amount": 60.0,
            "total_outlays": 600.0,
        },
        {
            "id": None,
            "code": "L",
            "name": "DEFC L",
            "amount": 30.0,
            "total_outlays": 300.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json()["results"] == expected_result

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps(
            {
                "spending_level": "award_financial",
                "filters": {"award_ids": ["1", "2", "3", "4", "5"], "def_codes": ["Q", "L"]},
            }
        ),
    )
    expected_result = [
        {
            "id": None,
            "code": "Q",
            "name": "DEFC Q",
            "amount": 120.0,
            "total_outlays": 1200.0,
        },
        {
            "id": None,
            "code": "L",
            "name": "DEFC L",
            "amount": 30.0,
            "total_outlays": 300.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json()["results"] == expected_result

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps({"spending_level": "award_financial", "filters": {"award_ids": ["5"]}}),
    )
    expected_result = [
        {
            "id": None,
            "code": "Q",
            "name": "DEFC Q",
            "amount": 70.0,
            "total_outlays": 700.0,
        },
        {
            "id": None,
            "code": "Z",
            "name": "DEFC Z",
            "amount": 60.0,
            "total_outlays": 600.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json()["results"] == expected_result


def test_awards_spending_level(client, monkeypatch, elasticsearch_award_index, award_financial_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps({"spending_level": "awards", "filters": {"award_ids": ["1", "2", "3", "4", "5"]}}),
    )
    expected_result = [
        {
            "id": None,
            "code": "Q",
            "name": "DEFC Q",
            "amount": 220.0,
            "total_outlays": 2200.0,
        },
        {
            "id": None,
            "code": "M",
            "name": "DEFC M",
            "amount": 140.0,
            "total_outlays": 1400.0,
        },
        {
            "id": None,
            "code": "Z",
            "name": "DEFC Z",
            "amount": 130.0,
            "total_outlays": 1300.0,
        },
        {
            "id": None,
            "code": "L",
            "name": "DEFC L",
            "amount": 60.0,
            "total_outlays": 600.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json()["results"] == expected_result

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps(
            {"spending_level": "awards", "filters": {"award_ids": ["1", "2", "3", "4", "5"], "def_codes": ["Q"]}}
        ),
    )
    expected_result = [
        {
            "id": None,
            "code": "Q",
            "name": "DEFC Q",
            "amount": 220.0,
            "total_outlays": 2200.0,
        },
        {
            "id": None,
            "code": "Z",
            "name": "DEFC Z",
            "amount": 130.0,
            "total_outlays": 1300.0,
        },
        {
            "id": None,
            "code": "M",
            "name": "DEFC M",
            "amount": 90.0,
            "total_outlays": 900.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json()["results"] == expected_result

    resp = client.post(
        "/api/v2/search/spending_by_category/defc",
        content_type="application/json",
        data=json.dumps({"spending_level": "awards", "filters": {"award_ids": ["5"]}}),
    )
    expected_result = [
        {
            "id": None,
            "code": "Q",
            "name": "DEFC Q",
            "amount": 130.0,
            "total_outlays": 1300.0,
        },
        {
            "id": None,
            "code": "Z",
            "name": "DEFC Z",
            "amount": 130.0,
            "total_outlays": 1300.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json()["results"] == expected_result
