import json

from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def _expected_messages():
    expected_messages = [get_time_period_message()]
    expected_messages.append(
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
        "See documentation for more information. "
    )
    return expected_messages


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, basic_award):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response_with_more_awards(
    client, monkeypatch, elasticsearch_transaction_index, basic_award, subagency_award
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    baker.make("submissions.SubmissionAttributes", toptier_code="001")
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Toptier Agency 3",
                "code": "TA3",
                "id": 1003,
                "agency_slug": None,
                "total_outlays": None,
            },
            {
                "amount": 5.0,
                "name": "Awarding Toptier Agency 1",
                "code": "TA1",
                "id": 1001,
                "agency_slug": None,
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, basic_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 5.0,
                "name": "Awarding Toptier Agency 1",
                "code": "TA1",
                "id": 1001,
                "agency_slug": None,
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_with_date_type(client, monkeypatch, elasticsearch_transaction_index, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    baker.make("submissions.SubmissionAttributes", toptier_code="001")
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2020-01-01", "end_date": "2020-01-01"}]
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {
            "page": 1,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2020-01-01", "end_date": "2020-01-16"}]
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Toptier Agency 3",
                "code": "TA3",
                "id": 1003,
                "agency_slug": None,
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_with_new_awards_only(client, monkeypatch, elasticsearch_transaction_index, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    baker.make("submissions.SubmissionAttributes", toptier_code="001")
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-01", "end_date": "2020-01-01"}
                    ]
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-01", "end_date": "2020-01-16"}
                    ]
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Toptier Agency 3",
                "code": "TA3",
                "id": 1003,
                "agency_slug": None,
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Tests the requirement that action date must be in bounds too
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_agency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-03", "end_date": "2020-01-16"}
                    ]
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_agency",
        "limit": 10,
        "page_metadata": {
            "page": 1,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
