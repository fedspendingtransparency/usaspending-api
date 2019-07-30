# Stdlib imports
from datetime import datetime
import json
import pytest

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel, MockSet

# Imports from your apps
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.v2.views.new_awards_over_time import NewAwardsOverTimeVisualizationViewSet


def get_new_awards_over_time_url():
    return "/api/v2/search/new_awards_over_time/"


def catch_filter_errors(viewset, filters, expected_exception):
    try:
        viewset.validate_api_request(filters)
    except UnprocessableEntityException:
        if expected_exception == "UnprocessableEntityException":
            assert True
        else:
            assert False, "UnprocessableEntityException error unexpected"
    except InvalidParameterException:
        if expected_exception == "InvalidParameterException":
            assert True
        else:
            assert False, "InvalidParameterException error unexpected"
    except Exception as e:
        print(e)
        assert False, "Incorrect Exception raised"
    else:
        assert False, "Filters should have produced an exception and didn't"


@pytest.fixture
def add_award_recipients(monkeypatch):
    mock_awards_recipients = MockSet()
    monkeypatch.setattr("usaspending_api.recipient.models.SummaryAwardRecipient.objects", mock_awards_recipients)
    mock_model_list = []
    current_id = 1
    new_award_count = 12
    for i in range(current_id, current_id + new_award_count):
        mock_model_list.append(
            MockModel(
                award_id=i,
                recipient_hash="21a1b0df-e7cd-349b-b948-60ed0ac1e6a0",
                parent_recipient_unique_id=None,
                action_date=datetime(2009, 5, 30),
            )
        )
    current_id += new_award_count
    new_award_count = 3
    for i in range(current_id, current_id + new_award_count):
        mock_model_list.append(
            MockModel(
                award_id=i,
                recipient_hash="21a1b0df-e7cd-349b-b948-60ed0ac1e6a0",
                parent_recipient_unique_id=None,
                action_date=datetime(2009, 5, 1),
            )
        )
    current_id += new_award_count
    new_award_count = 1
    for i in range(current_id, current_id + new_award_count):
        mock_model_list.append(
            MockModel(
                award_id=i,
                recipient_hash="21a1b0df-e7cd-349b-b948-60ed0ac1e6a0",
                parent_recipient_unique_id=None,
                action_date=datetime(2009, 7, 2),
            )
        )
    current_id += new_award_count
    new_award_count = 2
    for i in range(current_id, current_id + new_award_count):
        mock_model_list.append(
            MockModel(
                award_id=i,
                recipient_hash="21a1b0df-e7cd-349b-b948-60ed0ac1e6a0",
                parent_recipient_unique_id=None,
                action_date=datetime(2008, 1, 10),
            )
        )
    current_id += new_award_count
    new_award_count = 6
    for i in range(current_id, current_id + new_award_count):
        mock_model_list.append(
            MockModel(
                award_id=i,
                recipient_hash="4e418651-4b83-8722-ab4e-e68d80bfb3b3",
                parent_recipient_unique_id=None,
                action_date=datetime(2009, 7, 30),
            )
        )
    add_to_mock_objects(mock_awards_recipients, mock_model_list)


@pytest.mark.django_db
def test_new_awards_month(add_award_recipients, client):
    test_payload = {
        "group": "month",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "21a1b0df-e7cd-349b-b948-60ed0ac1e6a0-R",
        },
    }
    expected_results = []
    # 2009
    for i in range(1, 13):
        new_award_count = 0
        if i == 8:
            new_award_count = 15
        elif i == 10:
            new_award_count = 1
        expected_results.append(
            {"time_period": {"fiscal_year": "2009", "month": str(i)}, "new_award_count_in_period": new_award_count}
        )
    # 2010
    for i in range(1, 13):
        new_award_count = 0
        expected_results.append(
            {"time_period": {"fiscal_year": "2010", "month": str(i)}, "new_award_count_in_period": new_award_count}
        )
    expected_response = {"group": "month", "results": expected_results}

    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "month"
    assert expected_response == resp.data

    test_payload["filters"]["time_period"] = [{"start_date": "2007-10-01", "end_date": "2010-09-30"}]
    expected_results = []
    # 2008
    for i in range(1, 13):
        new_award_count = 0
        if i == 4:
            new_award_count = 2
        expected_results.append(
            {"time_period": {"fiscal_year": "2008", "month": str(i)}, "new_award_count_in_period": new_award_count}
        )
    # 2009
    for i in range(1, 13):
        new_award_count = 0
        if i == 8:
            new_award_count = 15
        elif i == 10:
            new_award_count = 1
        expected_results.append(
            {"time_period": {"fiscal_year": "2009", "month": str(i)}, "new_award_count_in_period": new_award_count}
        )
    # 2010
    for i in range(1, 13):
        new_award_count = 0
        expected_results.append(
            {"time_period": {"fiscal_year": "2010", "month": str(i)}, "new_award_count_in_period": new_award_count}
        )
    expected_response = {"group": "month", "results": expected_results}
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "month"
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards_quarter(add_award_recipients, client):
    test_payload = {
        "group": "quarter",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "21a1b0df-e7cd-349b-b948-60ed0ac1e6a0-R",
        },
    }
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200

    expected_results = []
    # 2009
    for i in range(1, 5):
        new_award_count = 0
        if i == 3:
            new_award_count = 15
        elif i == 4:
            new_award_count = 1
        expected_results.append(
            {"time_period": {"fiscal_year": "2009", "quarter": str(i)}, "new_award_count_in_period": new_award_count}
        )
    # 2010
    for i in range(1, 5):
        new_award_count = 0
        expected_results.append(
            {"time_period": {"fiscal_year": "2010", "quarter": str(i)}, "new_award_count_in_period": new_award_count}
        )
    expected_response = {"group": "quarter", "results": expected_results}
    assert resp.data["group"] == "quarter"
    assert expected_response == resp.data

    expected_results = []
    # 2008
    for i in range(1, 5):
        new_award_count = 0
        if i == 2:
            new_award_count = 2
        expected_results.append(
            {"time_period": {"fiscal_year": "2008", "quarter": str(i)}, "new_award_count_in_period": new_award_count}
        )
    # 2009
    for i in range(1, 5):
        new_award_count = 0
        if i == 3:
            new_award_count = 15
        elif i == 4:
            new_award_count = 1
        expected_results.append(
            {"time_period": {"fiscal_year": "2009", "quarter": str(i)}, "new_award_count_in_period": new_award_count}
        )
    # 2010
    for i in range(1, 5):
        new_award_count = 0
        expected_results.append(
            {"time_period": {"fiscal_year": "2010", "quarter": str(i)}, "new_award_count_in_period": new_award_count}
        )

    expected_response = {"group": "quarter", "results": expected_results}

    test_payload["filters"]["time_period"] = [{"start_date": "2007-10-01", "end_date": "2010-09-30"}]
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards_fiscal_year(add_award_recipients, client):
    test_payload = {
        "group": "fiscal_year",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "21a1b0df-e7cd-349b-b948-60ed0ac1e6a0-R",
        },
    }
    expected_response = {
        "group": "fiscal_year",
        "results": [
            {"time_period": {"fiscal_year": "2009"}, "new_award_count_in_period": 16},
            {"time_period": {"fiscal_year": "2010"}, "new_award_count_in_period": 0},
        ],
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
            {"time_period": {"fiscal_year": "2009"}, "new_award_count_in_period": 16},
            {"time_period": {"fiscal_year": "2010"}, "new_award_count_in_period": 0},
        ],
    }
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 200
    assert resp.data["group"] == "fiscal_year"
    assert expected_response == resp.data


@pytest.mark.django_db
def test_new_awards_failures(add_award_recipients, client):
    test_payload = {
        "group": "quarter",
        "filters": {
            "time_period": [{"start_date": "2008-10-01", "end_date": "2010-09-30"}],
            "recipient_id": "21a1b0df-e7cd-349b-b948-60ed0ac1e6a0-P",
        },
    }
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 400  # No parent records found, return 400

    test_payload["filters"]["recipient_hash"] = "enriwerniewrn"
    resp = client.post(get_new_awards_over_time_url(), content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == 400  # might reject text as non UUID in future?


@pytest.mark.django_db
def test_new_awards_filter_errors():
    new_awards_viewset = NewAwardsOverTimeVisualizationViewSet()
    filters = {"group": "baseball"}
    catch_filter_errors(new_awards_viewset, filters, "UnprocessableEntityException")
    filters = {"group": "baseball", "filters": {"recipient_id": "", "time_period": []}}
    catch_filter_errors(new_awards_viewset, filters, "InvalidParameterException")
    filters = {"group": "month", "filters": {"recipient_id": "", "time_period": []}}
    catch_filter_errors(new_awards_viewset, filters, "UnprocessableEntityException")
    filters = {"group": "month", "filters": {"recipient_id": "", "time_period": [{"start_date": ""}]}}
    catch_filter_errors(new_awards_viewset, filters, "UnprocessableEntityException")
