import pytest
from model_mommy import mommy
from rest_framework import status


url = "/api/v2/reporting/agencies/{agency_data}/submission_history/"


@pytest.fixture
def setup_test_data(db):
    """ Insert data into DB for testing """
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=6,
        published_date="2019-07-16 16:09:52.125837-04",
        certified_date="2019-07-16 16:09:52.125837-04",
        toptier_code="020",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        published_date="2020-08-17 18:37:21.605023-04",
        certified_date="2020-08-17 18:37:21.605023-04",
        toptier_code="020",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=3,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        published_date="2017-08-14 14:17:00.729315-04",
        certified_date="2017-08-14 14:17:00.729315-04",
        toptier_code="020",
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=4,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        published_date="2017-08-14 14:17:00.729315-04",
        certified_date="2017-08-14 14:17:00.729315-04",
        toptier_code="075",
    )


def test_basic_success(client, setup_test_data):
    resp = client.get(url.format(agency_data="020/2019/6"))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == [
        {"published_date": "2019-07-16T20:09:52.125837Z", "certified_date": "2019-07-16T20:09:52.125837Z"}
    ]


def test_multiple_submissions(client, setup_test_data):
    resp = client.get(url.format(agency_data="020/2019/7"))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    assert response["results"] == [
        {"published_date": "2020-08-17T22:37:21.605023Z", "certified_date": "2020-08-17T22:37:21.605023Z"},
        {"published_date": "2017-08-14T18:17:00.729315Z", "certified_date": "2017-08-14T18:17:00.729315Z"},
    ]
    resp = client.get(url.format(agency_data="075/2019/7"))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == [
        {"published_date": "2017-08-14T18:17:00.729315Z", "certified_date": "2017-08-14T18:17:00.729315Z"}
    ]
