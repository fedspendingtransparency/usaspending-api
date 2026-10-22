import json
from datetime import datetime

import pytest
from model_bakery import baker

from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def get_new_awards_over_time_url():
    return "/api/v2/search/new_awards_over_time/"


def catch_filter_errors(viewset, filters, expected_exception):
    try:
        viewset.validate_api_request(filters)
    except UnprocessableEntityException:
        if expected_exception == "UnprocessableEntityException":
            assert True
        else:
            raise AssertionError("UnprocessableEntityException error unexpected") from None
    except InvalidParameterException:
        if expected_exception == "InvalidParameterException":
            assert True
        else:
            raise AssertionError("InvalidParameterException error unexpected") from None
    except Exception as e:
        print(e)
        raise AssertionError("Incorrect Exception raised") from e
    else:
        raise AssertionError("Filters should have produced an exception and didn't")


@pytest.fixture
def add_award_recipients(db):
    current_id = 1
    new_award_count = 12
    baker.make(
        "recipient.RecipientLookup",
        uei="HX3VU12NNWN9",
        legal_business_name="Sample Recipient",
        recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
    )

    baker.make(
        "recipient.RecipientProfile",
        uei="HX3VU12NNWN9",
        recipient_level="R",
        recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
        recipient_name="Sample Recipient",
    )
    baker.make(
        "recipient.RecipientLookup",
        uei="K87WE4KQLBG4",
        legal_business_name="Sample Recipient",
        recipient_hash="6a1765a8-6948-6ae8-ee2a-1cfc72de739d",
    )

    baker.make(
        "recipient.RecipientProfile",
        uei="K87WE4KQLBG4",
        recipient_level="R",
        recipient_hash="6a1765a8-6948-6ae8-ee2a-1cfc72de739d",
        recipient_name="Sample Recipient",
    )
    for i in range(current_id, current_id + new_award_count):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            date_signed=datetime(2009, 5, 30),
            latest_transaction_id=i,
            earliest_transaction_id=i,
            type="A",
            action_date=datetime(2009, 5, 30),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
            recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=i,
            award_id=i,
            is_fpds=True,
            action_date=datetime(2009, 5, 30),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
        )
    current_id += new_award_count
    new_award_count = 3
    for i in range(current_id, current_id + new_award_count):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            date_signed=datetime(2009, 5, 1),
            latest_transaction_id=i,
            earliest_transaction_id=i,
            type="A",
            action_date=datetime(2009, 5, 1),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
            recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=i,
            award_id=i,
            is_fpds=True,
            action_date=datetime(2009, 5, 1),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
        )
    current_id += new_award_count
    new_award_count = 1
    for i in range(current_id, current_id + new_award_count):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            date_signed=datetime(2009, 7, 2),
            latest_transaction_id=i,
            earliest_transaction_id=i,
            type="A",
            action_date=datetime(2009, 7, 2),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
            recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=i,
            award_id=i,
            is_fpds=True,
            action_date=datetime(2009, 7, 2),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
        )
    current_id += new_award_count
    new_award_count = 2
    for i in range(current_id, current_id + new_award_count):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            date_signed=datetime(2008, 1, 10),
            latest_transaction_id=i,
            earliest_transaction_id=i,
            type="A",
            action_date=datetime(2008, 1, 10),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
            recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=i,
            award_id=i,
            is_fpds=True,
            action_date=datetime(2008, 1, 10),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
        )
    current_id += new_award_count
    new_award_count = 6
    for i in range(current_id, current_id + new_award_count):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            date_signed=datetime(2009, 7, 30),
            latest_transaction_id=i,
            earliest_transaction_id=i,
            type="A",
            action_date=datetime(2009, 7, 30),
            recipient_uei="K87WE4KQLBG4",
            parent_uei=None,
            recipient_hash="6a1765a8-6948-6ae8-ee2a-1cfc72de739d",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=i,
            award_id=i,
            is_fpds=True,
            action_date=datetime(2009, 7, 30),
            recipient_uei="K87WE4KQLBG4",
            parent_uei=None,
        )
    current_id += new_award_count
    # Awards signed in the Oct-Dec calendar window belong to the *next* fiscal year.
    # 2008-12-15 is calendar year 2008 but fiscal year 2009, Q1, fiscal month 3.
    new_award_count = 4
    for i in range(current_id, current_id + new_award_count):
        baker.make(
            "search.AwardSearch",
            award_id=i,
            date_signed=datetime(2008, 12, 15),
            latest_transaction_id=i,
            earliest_transaction_id=i,
            type="A",
            action_date=datetime(2008, 12, 15),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
            recipient_hash="63248e89-7fb7-2d51-4085-8163798379d9",
        )
        baker.make(
            "search.TransactionSearch",
            transaction_id=i,
            award_id=i,
            is_fpds=True,
            action_date=datetime(2008, 12, 15),
            recipient_uei="HX3VU12NNWN9",
            parent_uei=None,
        )


def _build_expected_results(
    year_counts: dict[str, dict[int, int]],
    period_key: str = "month",
    period_count: int = 12,
) -> list[dict]:
    """
    Build expected new-award-count results grouped by fiscal-year period.

    year_counts maps fiscal_year -> {period_number: new_award_count};
    period numbers not listed default to 0. Collapses repeated if/elif
    branching into a single dict lookup.

    period_key: "month" or "quarter" (the key used in the time_period dict).
    period_count: 12 for months, 4 for quarters.
    """
    results = []
    for fiscal_year, period_overrides in year_counts.items():
        for period in range(1, period_count + 1):
            results.append(
                {
                    "time_period": {"fiscal_year": fiscal_year, period_key: str(period)},
                    "new_award_count_in_period": period_overrides.get(period, 0),
                }
            )
    return results


@pytest.mark.django_db
def test_new_awards_month(client, monkeypatch, add_award_recipients, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # First payload - FY 2009-2010
    test_payload = {
        "group": "month",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "63248e89-7fb7-2d51-4085-8163798379d9-R",
        },
    }
    expected_results = _build_expected_results({"2009": {3: 4, 8: 15, 10: 1}, "2010": {}})

    expected_response = {"group": "month", "results": expected_results, "messages": [get_time_period_message()]}

    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "month"
    assert expected_response == resp.data

    # Second payload - FY 2008-2010
    test_payload["filters"]["time_period"] = [{"start_date": "2007-10-01", "end_date": "2010-09-30"}]
    expected_results = _build_expected_results({"2008": {4: 2}, "2009": {3: 4, 8: 15, 10: 1}, "2010": {}})

    expected_response = {"group": "month", "results": expected_results, "messages": [get_time_period_message()]}

    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "month"
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards_quarter(client, monkeypatch, add_award_recipients, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    # First payload - FY 2009-2010
    test_payload = {
        "group": "quarter",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "63248e89-7fb7-2d51-4085-8163798379d9-R",
        },
    }
    expected_results = _build_expected_results(
        {
            "2009": {1: 4, 3: 15, 4: 1},
            "2010": {},
        },
        period_key="quarter",
        period_count=4,
    )
    expected_response = {"group": "quarter", "results": expected_results, "messages": [get_time_period_message()]}

    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "quarter"
    assert expected_response == resp.data

    # Second payload - FY 2008-2010
    test_payload["filters"]["time_period"] = [{"start_date": "2007-10-01", "end_date": "2010-09-30"}]
    expected_results = _build_expected_results(
        {
            "2008": {2: 2},
            "2009": {1: 4, 3: 15, 4: 1},
            "2010": {},
        },
        period_key="quarter",
        period_count=4,
    )
    expected_response = {"group": "quarter", "results": expected_results, "messages": [get_time_period_message()]}

    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards_fiscal_year(client, monkeypatch, add_award_recipients, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    test_payload = {
        "group": "fiscal_year",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "63248e89-7fb7-2d51-4085-8163798379d9-R",
        },
    }
    expected_response = {
        "group": "fiscal_year",
        "results": [
            {"time_period": {"fiscal_year": "2009"}, "new_award_count_in_period": 20},
            {"time_period": {"fiscal_year": "2010"}, "new_award_count_in_period": 0},
        ],
        "messages": [get_time_period_message()],
    }

    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "fiscal_year"
    assert expected_response == resp.data

    test_payload["filters"]["time_period"] = [{"start_date": "2007-10-01", "end_date": "2010-09-30"}]

    expected_response = {
        "group": "fiscal_year",
        "results": [
            {"time_period": {"fiscal_year": "2008"}, "new_award_count_in_period": 2},
            {"time_period": {"fiscal_year": "2009"}, "new_award_count_in_period": 20},
            {"time_period": {"fiscal_year": "2010"}, "new_award_count_in_period": 0},
        ],
        "messages": [get_time_period_message()],
    }
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "fiscal_year"
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards_failures(client, monkeypatch, add_award_recipients, elasticsearch_award_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    test_payload = {
        "group": "quarter",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "63248e89-7fb7-2d51-4085-8163798379d9-P",
        },
    }
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 400  # No parent records found, return 400

    test_payload["filters"]["recipient_hash"] = "enriwerniewrn"
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 400  # might reject text as non UUID in future?
