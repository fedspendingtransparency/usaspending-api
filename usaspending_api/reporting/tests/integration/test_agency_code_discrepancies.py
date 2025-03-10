import pytest
from model_bakery import baker
from rest_framework import status


url = "/api/v2/reporting/agencies/123/discrepancies/"


@pytest.fixture
def setup_test_data(db):
    """Insert test data into DB"""
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2020,
        submission_fiscal_month=6,
        submission_fiscal_quarter=2,
        is_quarter=True,
        submission_reveal_date="2020-04-01",
        period_start_date="2020-04-01",
    )
    baker.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=2,
        tas_rendering_label="TAS 1",
        obligated_amount=10.0,
    )
    baker.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=123,
        fiscal_year=2020,
        fiscal_period=2,
        tas_rendering_label="TAS 2",
        obligated_amount=1.0,
    )
    baker.make(
        "reporting.ReportingAgencyMissingTas",
        toptier_code=321,
        fiscal_year=2020,
        fiscal_period=2,
        tas_rendering_label="TAS 2",
        obligated_amount=12.0,
    )


def test_basic_success(setup_test_data, client):
    resp = client.get(url + "?fiscal_year=2020&fiscal_period=3")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0

    resp = client.get(url + "?fiscal_year=2020&fiscal_period=2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [{"tas": "TAS 1", "amount": 10.0}, {"tas": "TAS 2", "amount": 1.0}]
    assert response["results"] == expected_results


def test_pagination(setup_test_data, client):
    resp = client.get(url + "?fiscal_year=2020&fiscal_period=2&limit=1")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [{"tas": "TAS 1", "amount": 10.0}]
    assert response["results"] == expected_results

    resp = client.get(url + "?fiscal_year=2020&fiscal_period=2&limit=1&page=2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [{"tas": "TAS 2", "amount": 1.0}]
    assert response["results"] == expected_results


def test_sort(setup_test_data, client):
    resp = client.get(url + "?fiscal_year=2020&fiscal_period=2&sort=tas")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    expected_results = [{"tas": "TAS 2", "amount": 1.0}, {"tas": "TAS 1", "amount": 10.0}]
    assert response["results"] == expected_results

    resp = client.get(url + "?fiscal_year=2020&fiscal_period=2&order=asc&limit=1&page=2")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    expected_results = [{"tas": "TAS 1", "amount": 10.0}]
    assert response["results"] == expected_results


def test_fiscal_period_without_revealed_submissions_fails(setup_test_data, client):
    resp = client.get(f"{url}?fiscal_year=2020&fiscal_period=12")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json()["detail"] == "Value for fiscal_period is outside the range of current submissions"
