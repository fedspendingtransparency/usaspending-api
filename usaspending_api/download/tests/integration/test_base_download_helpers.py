import json
import pytest


from datetime import datetime, timezone
from model_bakery import baker
from unittest.mock import patch

from usaspending_api.broker.lookups import EXTERNAL_DATA_TYPE_DICT
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet


JSON_REQUEST = {"dummy_key": "dummy_value"}


@pytest.fixture
def common_test_data(db):
    for js in JOB_STATUS:
        baker.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    download_jobs = [
        {
            "download_job_id": 1,
            "file_name": "oldest_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 15, 12, 0, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 2,
            "file_name": "yesterday.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 16, 12, 0, 0, 0, timezone.utc),
        },
    ]
    for job in download_jobs:
        with patch("django.utils.timezone.now") as mock_now:
            mock_now.return_value = job["update_date"]
            baker.make("download.DownloadJob", **job)

    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=datetime(2021, 1, 1, 12, 0, 0, 0, timezone.utc),
    )


def test_elasticsearch_download_cached(common_test_data):
    external_load_dates = [
        # FABS and FPDS dates are much newer to show they aren't used for ES downloads
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fabs"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fpds"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_transactions"],
            "last_load_date": datetime(2021, 1, 17, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_awards"],
            "last_load_date": datetime(2021, 1, 17, 16, 0, 0, 0, timezone.utc),
        },
    ]
    for load_date in external_load_dates:
        baker.make("broker.ExternalDataLoadDate", **load_date)

    es_transaction_request = {
        **JSON_REQUEST,
        "download_types": ["elasticsearch_transactions", "elasticsearch_sub_awards"],
    }
    es_award_request = {**JSON_REQUEST, "download_types": ["elasticsearch_awards", "elasticsearch_sub_awards"]}

    download_jobs = [
        {
            "download_job_id": 10,
            "file_name": "es_transaction_job_wrong.zip",
            "job_status_id": 1,
            "json_request": json.dumps(es_transaction_request),
            "update_date": datetime(2021, 1, 17, 10, 0, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 11,
            "file_name": "es_transaction_job_right.zip",
            "job_status_id": 1,
            "json_request": json.dumps(es_transaction_request),
            "update_date": datetime(2021, 1, 17, 12, 30, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 20,
            "file_name": "es_award_job_wrong.zip",
            "job_status_id": 1,
            "json_request": json.dumps(es_award_request),
            "update_date": datetime(2021, 1, 17, 13, 0, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 21,
            "file_name": "es_award_job_right.zip",
            "job_status_id": 1,
            "json_request": json.dumps(es_award_request),
            "update_date": datetime(2021, 1, 17, 17, 0, 0, 0, timezone.utc),
        },
    ]
    for job in download_jobs:
        with patch("django.utils.timezone.now") as mock_now:
            mock_now.return_value = job["update_date"]
            baker.make("download.DownloadJob", **job)

    result = BaseDownloadViewSet._get_cached_download(
        json.dumps(es_transaction_request), es_transaction_request["download_types"]
    )
    assert result == {"download_job_id": 11, "file_name": "es_transaction_job_right.zip"}

    result = BaseDownloadViewSet._get_cached_download(json.dumps(es_award_request), es_award_request["download_types"])
    assert result == {"download_job_id": 21, "file_name": "es_award_job_right.zip"}


def test_elasticsearch_cached_download_not_found(common_test_data):
    external_load_dates = [
        # FABS and FPDS dates are much newer to show they aren't used for ES downloads
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fabs"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fpds"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_transactions"],
            "last_load_date": datetime(2021, 1, 17, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_awards"],
            "last_load_date": datetime(2021, 1, 17, 16, 0, 0, 0, timezone.utc),
        },
    ]
    for load_date in external_load_dates:
        baker.make("broker.ExternalDataLoadDate", **load_date)

    result = BaseDownloadViewSet._get_cached_download(
        json.dumps(JSON_REQUEST), ["elasticsearch_transactions", "elasticsearch_sub_awards"]
    )
    assert result is None

    result = BaseDownloadViewSet._get_cached_download(
        json.dumps(JSON_REQUEST), ["elasticsearch_awards", "elasticsearch_sub_awards"]
    )
    assert result is None


def test_non_elasticsearch_download_cached(common_test_data):
    external_load_dates = [
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fabs"],
            "last_load_date": datetime(2021, 1, 17, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fpds"],
            "last_load_date": datetime(2021, 1, 17, 13, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_transactions"],
            "last_load_date": datetime(2021, 1, 17, 14, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_awards"],
            "last_load_date": datetime(2021, 1, 17, 16, 0, 0, 0, timezone.utc),
        },
    ]
    for load_date in external_load_dates:
        baker.make("broker.ExternalDataLoadDate", **load_date)

    download_jobs = [
        {
            "download_job_id": 10,
            "file_name": "10_download_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 17, 10, 0, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 11,
            "file_name": "11_download_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 17, 12, 30, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 20,
            "file_name": "20_download_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 17, 13, 0, 0, 0, timezone.utc),
        },
        {
            "download_job_id": 21,
            "file_name": "21_download_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 17, 17, 0, 0, 0, timezone.utc),
        },
    ]
    for job in download_jobs:
        with patch("django.utils.timezone.now") as mock_now:
            mock_now.return_value = job["update_date"]
            baker.make("download.DownloadJob", **job)

    # Grab latest valid download
    result = BaseDownloadViewSet._get_cached_download(json.dumps(JSON_REQUEST))
    assert result == {"download_job_id": 21, "file_name": "21_download_job.zip"}

    # FABS date updated; download no longer cached
    baker.make(
        "broker.ExternalDataLoadDate",
        external_data_type__external_data_type_id=EXTERNAL_DATA_TYPE_DICT["fabs"],
        last_load_date=datetime(2021, 1, 18, 12, 0, 0, 0, timezone.utc),
    )
    result = BaseDownloadViewSet._get_cached_download(json.dumps(JSON_REQUEST))
    assert result is None

    # New download comes through and is cached
    with patch("django.utils.timezone.now") as mock_now:
        job = {
            "download_job_id": 30,
            "file_name": "30_download_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 18, 13, 0, 0, 0, timezone.utc),
        }
        mock_now.return_value = job["update_date"]
        baker.make("download.DownloadJob", **job)

    result = BaseDownloadViewSet._get_cached_download(json.dumps(JSON_REQUEST))
    assert result == {"download_job_id": 30, "file_name": "30_download_job.zip"}

    # New submission_reveal_date is set in DABSSubmissionWindowSchedule; clears the cache
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=datetime(2021, 1, 19, 6, 0, 0, 0, timezone.utc),
    )

    result = BaseDownloadViewSet._get_cached_download(json.dumps(JSON_REQUEST))
    assert result is None

    # Download after the new submission_reveal_date is cached
    with patch("django.utils.timezone.now") as mock_now:
        job = {
            "download_job_id": 31,
            "file_name": "31_download_job.zip",
            "job_status_id": 1,
            "json_request": json.dumps(JSON_REQUEST),
            "update_date": datetime(2021, 1, 19, 6, 15, 0, 0, timezone.utc),
        }
        mock_now.return_value = job["update_date"]
        baker.make("download.DownloadJob", **job)

    result = BaseDownloadViewSet._get_cached_download(json.dumps(JSON_REQUEST))
    assert result == {"download_job_id": 31, "file_name": "31_download_job.zip"}


def test_non_elasticsearch_cached_download_not_found(common_test_data):
    external_load_dates = [
        # FABS and FPDS dates are much newer to show they aren't used for ES downloads
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fabs"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["fpds"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_transactions"],
            "last_load_date": datetime(2021, 1, 17, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_awards"],
            "last_load_date": datetime(2021, 1, 17, 16, 0, 0, 0, timezone.utc),
        },
    ]
    for load_date in external_load_dates:
        baker.make("broker.ExternalDataLoadDate", **load_date)

    result = BaseDownloadViewSet._get_cached_download(json.dumps(JSON_REQUEST))
    assert result is None
