# Stdlib imports
from datetime import datetime
import json
import pytest

# Core Django imports
from rest_framework import status

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects


@pytest.fixture
def populate_models(mock_matviews_qs):
    mock_0 = MockModel(action_date=datetime(2010, 6, 12), category="idv", counts=2, type="IDV_A")
    mock_1 = MockModel(action_date=datetime(2011, 6, 12), category="idv", counts=3, type="IDV_B_C")
    mock_2 = MockModel(action_date=datetime(2012, 6, 12), category="loans", counts=1, type="07")
    mock_3 = MockModel(action_date=datetime(2013, 6, 12), category="loans", counts=2, type="08")
    mock_4 = MockModel(action_date=datetime(2014, 6, 12), category="direct payment", counts=9, type="06")
    mock_5 = MockModel(action_date=datetime(2015, 6, 12), category="contract", counts=20, type="C")
    mock_6 = MockModel(action_date=datetime(2016, 6, 12), category="other", counts=8, type="11")
    mock_7 = MockModel(action_date=datetime(2017, 6, 12), category=None, counts=2, type=None)

    add_to_mock_objects(mock_matviews_qs, [mock_0, mock_1, mock_2, mock_3, mock_4, mock_5, mock_6, mock_7])


def get_spending_by_award_count_url():
    return "/api/v2/search/spending_by_award_count/"


@pytest.mark.django_db
def test_spending_by_award_count(populate_models, client, mock_matviews_qs):
    test_payload = {
        "subawards": False,
        "filters": {
            "time_period": [
                {"start_date": "2009-10-01", "end_date": "2017-09-30"},
                {"start_date": "2017-10-01", "end_date": "2018-09-30"},
            ]
        },
    }

    expected_response = {
        "results": {"contracts": 20, "idvs": 5, "loans": 3, "direct_payments": 9, "grants": 0, "other": 10}
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"


@pytest.mark.django_db
def test_spending_by_award_count_idvs(populate_models, client, mock_matviews_qs):
    test_payload = {
        "subawards": False,
        "filters": {
            "award_type_codes": ["IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C"],
            "time_period": [{"start_date": "2009-10-01", "end_date": "2018-09-30"}],
        },
    }

    expected_response = {
        "results": {"contracts": 0, "idvs": 3, "loans": 0, "direct_payments": 0, "grants": 0, "other": 0}
    }

    resp = client.post(
        get_spending_by_award_count_url(), content_type="application/json", data=json.dumps(test_payload)
    )
    print(json.dumps(resp.data))

    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.data, "Unexpected or missing content!"
