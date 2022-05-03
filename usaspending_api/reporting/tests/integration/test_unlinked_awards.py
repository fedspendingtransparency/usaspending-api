import pytest
from model_bakery import baker
from rest_framework import status


url = "/api/v2/reporting/agencies/{toptier_code}/{fiscal_year}/{fiscal_period}/unlinked_awards/{type}/"


@pytest.fixture
def setup_test_data(db):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2020,
        submission_fiscal_month=8,
        is_quarter=False,
        submission_reveal_date="2020-06-01",
        period_start_date="2020-04-01",
    )
    baker.make(
        "reporting.ReportingAgencyOverview",
        toptier_code="043",
        fiscal_year=2020,
        fiscal_period=8,
        unlinked_assistance_c_awards=12,
        unlinked_assistance_d_awards=24,
        unlinked_procurement_c_awards=14,
        unlinked_procurement_d_awards=28,
        linked_assistance_awards=6,
        linked_procurement_awards=7,
    )


def test_assistance_success(setup_test_data, client):
    resp = client.get(url.format(toptier_code="043", fiscal_year=2020, fiscal_period=8, type="assistance"))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()

    expected_results = {
        "unlinked_file_c_award_count": 12,
        "unlinked_file_d_award_count": 24,
        "total_linked_award_count": 6,
    }

    assert response == expected_results


def test_procurement_success(setup_test_data, client):
    resp = client.get(url.format(toptier_code="043", fiscal_year=2020, fiscal_period=8, type="procurement"))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()

    expected_results = {
        "unlinked_file_c_award_count": 14,
        "unlinked_file_d_award_count": 28,
        "total_linked_award_count": 7,
    }

    assert response == expected_results


def test_no_result_found(setup_test_data, client):
    resp = client.get(url.format(toptier_code="045", fiscal_year=2020, fiscal_period=8, type="procurement"))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()

    expected_results = {
        "unlinked_file_c_award_count": 0,
        "unlinked_file_d_award_count": 0,
        "total_linked_award_count": 0,
    }

    assert expected_results == response


def test_invalid_type(client):
    # trailing S on procurement
    resp = client.get(url.format(toptier_code="043", fiscal_year=2020, fiscal_period=8, type="procurementS"))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    response = resp.json()
    detail = response["detail"]

    assert detail == "Field 'type' is outside valid values ['assistance', 'procurement']"


def test_too_high_year(client):
    resp = client.get(url.format(toptier_code="043", fiscal_year=2100, fiscal_period=8, type="procurement"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    response = resp.json()
    detail = response["detail"]

    assert "Field 'fiscal_year' value '2100' is above max" in detail


def test_too_high_period(client):
    resp = client.get(url.format(toptier_code="043", fiscal_year=2020, fiscal_period=13, type="procurement"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    response = resp.json()
    detail = response["detail"]

    assert detail == "Field 'fiscal_period' value '13' is above max '12'"
