import json
import pytest

from model_bakery import baker
from rest_framework import status


url = "/api/v2/reporting/agencies/{agency}/{fy}/{period}/submission_history/"


@pytest.fixture
def setup_test_data(db):
    """Insert data into DB for testing"""
    dsws1 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2021,
        submission_fiscal_month=4,
        submission_fiscal_quarter=2,
        is_quarter=False,
        submission_reveal_date="2020-02-01",
        period_start_date="2020-01-01",
    )
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=6,
        published_date="2019-07-16 16:09:52.125837-04",
        certified_date="2019-07-16 16:09:52.125837-04",
        toptier_code="020",
        history=json.loads(
            '[{"certified_date": "2019-07-16T16:09:52.125837Z", "published_date": "2019-07-16T16:09:52.125837Z"}]'
        ),
        submission_window=dsws1,
    )
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        published_date="2020-08-17 18:37:21.605023-04",
        certified_date="2020-08-17 18:37:21.605023-04",
        toptier_code="020",
        history=json.loads(
            '[{"certified_date": "2020-08-17T18:37:21.605023Z", "published_date": "2020-08-17T18:37:21.605023Z"},'
            + '{"certified_date": "2017-08-14T14:17:00.729315Z", "published_date": "2017-08-14T14:17:00.729315Z"}]'
        ),
        submission_window=dsws1,
    )
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=3,
        reporting_fiscal_year=2019,
        reporting_fiscal_period=7,
        published_date="2017-08-14 14:17:00.729315-04",
        certified_date="2017-08-14 14:17:00.729315-04",
        toptier_code="075",
        history=json.loads(
            '[{"certified_date": "2017-08-14T14:17:00.729315Z", "published_date": "2017-08-14T14:17:00.729315Z"}]'
        ),
        submission_window=dsws1,
    )
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=10,
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        published_date="2021-02-16 18:20:00.729315-04",
        certified_date="2021-02-16 18:17:00.729315-04",
        toptier_code="222",
        history=json.loads(
            '[{"certified_date": null, "published_date": "2020-01-17T18:37:21.605023Z"},'
            + '{"certified_date": null, "published_date": "2020-01-14T14:17:00.729315Z"},'
            + '{"certified_date": "2021-02-14T14:17:00.729315Z", "published_date": "2021-02-14T14:16:00.729315Z"},'
            + '{"certified_date": "2021-02-16T14:17:00.729315Z", "published_date": "2021-02-16T14:16:00.729315Z"}]'
        ),
        submission_window=dsws1,
    )


def test_basic_success(client, setup_test_data):
    resp = client.get(url.format(agency="020", fy=2019, period=6))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == [
        {"publication_date": "2019-07-16T16:09:52.125837Z", "certification_date": "2019-07-16T16:09:52.125837Z"}
    ]

    resp = client.get(url.format(agency="075", fy=2019, period=7))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 1
    assert response["results"] == [
        {"publication_date": "2017-08-14T14:17:00.729315Z", "certification_date": "2017-08-14T14:17:00.729315Z"}
    ]


def test_multiple_submissions(client, setup_test_data):
    resp = client.get(url.format(agency="020", fy=2019, period=7))
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    assert response["results"] == [
        {"publication_date": "2020-08-17T18:37:21.605023Z", "certification_date": "2020-08-17T18:37:21.605023Z"},
        {"publication_date": "2017-08-14T14:17:00.729315Z", "certification_date": "2017-08-14T14:17:00.729315Z"},
    ]

    resp = client.get(url.format(agency="020", fy=2019, period=7) + "?sort=publication_date&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 2
    assert response["results"] == [
        {"publication_date": "2017-08-14T14:17:00.729315Z", "certification_date": "2017-08-14T14:17:00.729315Z"},
        {"publication_date": "2020-08-17T18:37:21.605023Z", "certification_date": "2020-08-17T18:37:21.605023Z"},
    ]


def test_no_data(client, setup_test_data):
    resp = client.get(url.format(agency="222", fy=2021, period=3))
    assert resp.status_code == status.HTTP_204_NO_CONTENT


def test_certification_nulls(client, setup_test_data):
    resp = client.get(url.format(agency="222", fy=2020, period=12) + "?sort=certification_date&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 4
    assert response["results"] == [
        {"certification_date": "2021-02-16T14:17:00.729315Z", "publication_date": "2021-02-16T14:16:00.729315Z"},
        {"certification_date": "2021-02-14T14:17:00.729315Z", "publication_date": "2021-02-14T14:16:00.729315Z"},
        {"certification_date": None, "publication_date": "2020-01-17T18:37:21.605023Z"},
        {"certification_date": None, "publication_date": "2020-01-14T14:17:00.729315Z"},
    ]

    resp = client.get(url.format(agency="222", fy=2020, period=12) + "?sort=certification_date&order=asc")
    assert resp.status_code == status.HTTP_200_OK
    response = resp.json()
    assert len(response["results"]) == 4
    assert response["results"] == [
        {"certification_date": None, "publication_date": "2020-01-14T14:17:00.729315Z"},
        {"certification_date": None, "publication_date": "2020-01-17T18:37:21.605023Z"},
        {"certification_date": "2021-02-14T14:17:00.729315Z", "publication_date": "2021-02-14T14:16:00.729315Z"},
        {"certification_date": "2021-02-16T14:17:00.729315Z", "publication_date": "2021-02-16T14:16:00.729315Z"},
    ]
