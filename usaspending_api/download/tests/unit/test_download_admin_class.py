# Stdlib imports
import json
import pytest

# Core Django imports

# Third-party app imports
from model_bakery import baker

# Imports from your apps
from usaspending_api.download.v2.download_admin import DownloadAdministrator


EXAMPLE_JSON_REQUEST = {
    "columns": [],
    "download_types": ["awards", "sub_awards"],
    "file_format": "csv",
    "filters": {
        "award_type_codes": ["02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "A", "B", "C", "D"],
        "recipient_search_text": ["806536462"],
        "time_period": [{"date_type": "action_date", "end_date": "2019-09-30", "start_date": "2007-10-01"}],
    },
    "limit": 500000,
    "request_type": "award",
}


@pytest.mark.django_db
def test_simple_download_admin_pass():
    job_status_row = {"job_status_id": 77, "name": "placeholder", "description": "Example Job Status"}
    baker.make("download.JobStatus", **job_status_row)
    download_job_row = {"download_job_id": 90, "file_name": "download_example_file.hdf5", "job_status_id": 77}
    baker.make("download.DownloadJob", **download_job_row)

    d = DownloadAdministrator()
    d.search_for_a_download(**{"download_job_id": 90})

    assert d.download_job.error_message is None
    assert d.download_job.number_of_rows is None

    d.update_download_job(error_message="Test Error", number_of_rows=-80)

    assert d.download_job.error_message == "Test Error"
    assert d.download_job.number_of_rows == -80


@pytest.mark.skip
@pytest.mark.django_db
def test_download_admin_restart_pass():
    job_status_rows = [
        {"job_status_id": 1, "name": "placeholder_1", "description": "Example Job Status 1"},
        {"job_status_id": 2, "name": "placeholder_2", "description": "Example Job Status 2"},
        {"job_status_id": 3, "name": "placeholder_3", "description": "Example Job Status 3"},
        {"job_status_id": 4, "name": "placeholder_4", "description": "Example Job Status 4"},
        {"job_status_id": 5, "name": "placeholder_5", "description": "Example Job Status 5"},
        {"job_status_id": 8, "name": "placeholder_8", "description": "Example Job Status 8"},
    ]
    for j in job_status_rows:
        baker.make("download.JobStatus", **j)
    download_job_row = {
        "download_job_id": 90,
        "file_name": "all_contracts_prime_awards_98237483.zip",
        "job_status_id": 1,
        "json_request": json.dumps(EXAMPLE_JSON_REQUEST),
    }
    baker.make("download.DownloadJob", **download_job_row)

    d = DownloadAdministrator()
    d.search_for_a_download(**{"file_name": "all_contracts_prime_awards_98237483.zip"})
    d.restart_download_operation()  # TODO: fix. It is looking for materialized views
