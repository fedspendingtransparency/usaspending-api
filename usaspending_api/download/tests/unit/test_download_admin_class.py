# Stdlib imports
import pytest

# Core Django imports

# Third-party app imports
from model_mommy import mommy

# Imports from your apps
from usaspending_api.download.v2.download_admin import DownloadAdministrator


@pytest.mark.django_db
def test_download_admin():
    job_status_row = {"job_status_id": 77, "name": "placeholder", "description": "Example Job Status"}
    mommy.make("download.JobStatus", **job_status_row)
    download_job_row = {"download_job_id": 90, "file_name": "download_example_file.hdf5", "job_status_id": 77}
    mommy.make("download.DownloadJob", **download_job_row)

    d = DownloadAdministrator()
    d.search_for_a_download(**{"download_job_id": 90})

    assert d.download_job.error_message is None
    assert d.download_job.number_of_rows is None

    d.update_download_job(error_message="Test Error", number_of_rows=-80)

    assert d.download_job.error_message == "Test Error"
    assert d.download_job.number_of_rows == -80
