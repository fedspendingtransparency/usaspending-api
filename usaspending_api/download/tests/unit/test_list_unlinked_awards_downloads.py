import datetime
from unittest.mock import MagicMock
import requests


from usaspending_api.download.helpers.download_file_helpers import get_last_modified_file

def test_url_correct():
    url = "https://api.usaspending.gov/api/v2/bulk_download/list_unlinked_awards_files/"
    request_body = {
    "toptier_code": "072"}
    response = requests.post(url, json=request_body)

    assert response.status_code == 201
    #not sure if this is the right status code for this API endpoint
    assert response.headers["Content-Type"] == "application/json; charset=UTF-8"

    data = response.json()
    assert "id" in data 
    assert data["file"["url"]] == "https://files.usaspending.gov/unlinked_awards_downloads/unlinked_awards_downloads/Agency_for_International_Development_UnlinkedAwards_2024-08-06_H09M35S39364372.zip"


def test_get_last_modified_file():
    expected_last_modified_file = "Department_of_Veterans_Affairs_unlinked_awards_2024-02-11_H21M34S39790417.zip"
    mock_s3_object_summary = []
    mock_s3_object_summary_a = MagicMock()
    mock_s3_object_summary_a.key = "Department_of_Veterans_Affairs_unlinked_awards_2024-02-09_H21M34S39790417.zip"
    mock_s3_object_summary_a.last_modified = datetime.date(2024, 2, 9)
    mock_s3_object_summary.append(mock_s3_object_summary_a)

    mock_s3_object_summary_b = MagicMock()
    mock_s3_object_summary_b.key = expected_last_modified_file
    mock_s3_object_summary_b.last_modified = datetime.date(2024, 2, 11)
    mock_s3_object_summary.append(mock_s3_object_summary_b)

    actual_last_modified_file = get_last_modified_file(mock_s3_object_summary)
    assert actual_last_modified_file == expected_last_modified_file


def test_get_last_modified_file_none_input():
    actual_last_modified_file = get_last_modified_file([])
    assert actual_last_modified_file is None
