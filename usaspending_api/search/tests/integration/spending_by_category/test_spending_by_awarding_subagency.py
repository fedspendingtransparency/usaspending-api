import json

import pytest
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


@pytest.mark.django_db
def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, basic_award):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


@pytest.mark.django_db
def test_additional_fields_response(client, monkeypatch, elasticsearch_transaction_index, basic_award, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data={
            "filters": {
                "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                "agencies": [
                    {
                        "type": "awarding",
                        "tier": "subtier",
                        "name": "Awarding Subtier Agency 5",
                        "toptier_name": "Awarding Toptier Agency 3",
                    }
                ],
            }
        },
    )
    assert resp.status_code == status.HTTP_200_OK

    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Subtier Agency 5",
                "code": "SA5",
                "id": 1005,
                "subagency_slug": "awarding-subtier-agency-5",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }

    assert expected_response["results"][0]["amount"] == resp.data["results"][0]["amount"]
    assert resp.data == {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Subtier Agency 5",
                "code": "SA5",
                "id": 1005,
                "subagency_slug": "awarding-subtier-agency-5",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }


@pytest.mark.django_db
def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, basic_award, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Subtier Agency 5",
                "code": "SA5",
                "id": 1005,
                "subagency_slug": "awarding-subtier-agency-5",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            },
            {
                "amount": 5.0,
                "name": "Awarding Subtier Agency 1",
                "code": "SA1",
                "id": 1001,
                "subagency_slug": "awarding-subtier-agency-1",
                "agency_id": 2001,
                "agency_abbreviation": "TA1",
                "agency_name": "Awarding Toptier Agency 1",
                "agency_slug": "awarding-toptier-agency-1",
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


@pytest.mark.django_db
def test_filtering_subtier_with_toptier(
    client, monkeypatch, elasticsearch_transaction_index, basic_award, subagency_award
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data={
            "filters": {
                "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                "agencies": [
                    {
                        "type": "awarding",
                        "tier": "subtier",
                        "name": "Awarding Subtier Agency 5",
                        "toptier_name": "Awarding Toptier Agency 3",
                    }
                ],
            }
        },
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Subtier Agency 5",
                "code": "SA5",
                "id": 1005,
                "subagency_slug": "awarding-subtier-agency-5",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }


@pytest.mark.django_db
def test_filtering_subtier_with_bogus_toptier(
    client, monkeypatch, elasticsearch_transaction_index, basic_award, subagency_award
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data={
            "filters": {
                "time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}],
                "agencies": [
                    {
                        "type": "awarding",
                        "tier": "subtier",
                        "name": "Awarding Subtier Agency 5",
                        "toptier_name": "bogus toptier name",
                    }
                ],
            }
        },
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }


def test_correct_response_with_date_type(client, monkeypatch, elasticsearch_transaction_index, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2020-01-01", "end_date": "2020-01-01"}],
                    "agencies": [
                        {
                            "type": "awarding",
                            "tier": "subtier",
                            "name": "Awarding Subtier Agency 5",
                            "toptier_name": "Awarding Toptier Agency 3",
                        }
                    ],
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2020-01-01", "end_date": "2020-01-16"}],
                    "agencies": [
                        {
                            "type": "awarding",
                            "tier": "subtier",
                            "name": "Awarding Subtier Agency 5",
                            "toptier_name": "Awarding Toptier Agency 3",
                        }
                    ],
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Subtier Agency 5",
                "code": "SA5",
                "id": 1005,
                "subagency_slug": "awarding-subtier-agency-5",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"

    assert resp.json()["results"] == expected_response["results"]
    assert resp.json() == expected_response


def test_correct_response_with_new_awards_only(client, monkeypatch, elasticsearch_transaction_index, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    # Tests where no new awards fall into range
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-01", "end_date": "2020-01-01"}
                    ],
                    "agencies": [
                        {
                            "type": "awarding",
                            "tier": "subtier",
                            "name": "Awarding Subtier Agency 5",
                            "toptier_name": "Awarding Toptier Agency 3",
                        }
                    ],
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Tests where records do fall into range
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-01", "end_date": "2020-01-16"}
                    ],
                    "agencies": [
                        {
                            "type": "awarding",
                            "tier": "subtier",
                            "name": "Awarding Subtier Agency 5",
                            "toptier_name": "Awarding Toptier Agency 3",
                        }
                    ],
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Awarding Subtier Agency 5",
                "code": "SA5",
                "id": 1005,
                "subagency_slug": "awarding-subtier-agency-5",
                "agency_id": 2003,
                "agency_abbreviation": "TA3",
                "agency_name": "Awarding Toptier Agency 3",
                "agency_slug": "awarding-toptier-agency-3",
                "total_outlays": None,
            }
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Tests where records fall into range with date signed but not action date
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-03", "end_date": "2020-01-16"}
                    ],
                    "agencies": [
                        {
                            "type": "awarding",
                            "tier": "subtier",
                            "name": "Awarding Subtier Agency 5",
                            "toptier_name": "Awarding Toptier Agency 3",
                        }
                    ],
                }
            }
        ),
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
