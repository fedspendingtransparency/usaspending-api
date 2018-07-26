# Stdlib imports
from datetime import datetime
import json
import pytest

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.v2.views.new_awards_over_time import NewAwardsOverTimeVisualizationViewSet


def get_new_awards_over_time_url():
    return "/api/v2/search/new_awards_over_time/"


@pytest.fixture
def add_award_recipients(mock_matviews_qs, refresh_matviews):
    mock_awards_1 = MockModel(
        recipient_hash="oiwehfwefw",
        parent_recipient_unique_id=None,
        counts=12,
        date_signed=datetime(2009, 5, 30),
        action_date="2009-05-30",
    )
    mock_awards_2 = MockModel(
        recipient_hash="uwqhewneuwe",
        parent_recipient_unique_id=None,
        counts=6,
        date_signed=datetime(2009, 7, 30),
        action_date="2009-05-30",
    )
    add_to_mock_objects(mock_matviews_qs, [mock_awards_1, mock_awards_2])


@pytest.mark.skip
@pytest.mark.django_db
def test_new_awards_simple_case(add_award_recipients, client):
    test_payload = {
        "group": "quarter",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "oiwehfwefw-R",
        },
    }
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200

    expected_response = {
        "group": "quarter",
        "results": [{"time_period": {"fiscal_year": 2009, "quarter": 3}, "new_award_count_in_period": 12}],
    }

    assert resp.data["group"] == "quarter"
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards():
    A = NewAwardsOverTimeVisualizationViewSet()
    filters = {"group": "baseball"}
    try:
        A.validate_api_request(filters)
    except UnprocessableEntityException as e:
        assert True
    except Exception:
        assert False, "Incorrect Exception raised"
    else:
        assert False, "Filters should have produced an exception and didn't"
