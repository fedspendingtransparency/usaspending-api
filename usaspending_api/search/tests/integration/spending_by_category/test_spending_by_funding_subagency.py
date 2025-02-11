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
        "/api/v2/search/spending_by_category/funding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


@pytest.mark.django_db
def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, basic_award, subagency_award):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/funding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "funding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {
                "amount": 10.0,
                "name": "Funding Subtier Agency 6",
                "code": "SA6",
                "id": 1006,
                "subagency_slug": "funding-subtier-agency-6",
                "agency_id": 2002,
                "agency_abbreviation": "TA2",
                "agency_name": "Funding Toptier Agency 2",
                "agency_slug": "funding-toptier-agency-2",
                "total_outlays": None,
            },
            {
                "amount": 5.0,
                "name": "Funding Subtier Agency 4",
                "code": "SA4",
                "id": 1004,
                "subagency_slug": "funding-subtier-agency-4",
                "agency_id": 2004,
                "agency_abbreviation": "TA4",
                "agency_name": "Funding Toptier Agency 4",
                "agency_slug": "funding-toptier-agency-4",
                "total_outlays": None,
            },
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
