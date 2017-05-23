import pytest

from usaspending_api.common.csv_helpers import format_path, create_filename_from_options


@pytest.mark.parametrize("path, formatted_path", [
    ("awards", "/api/v1/awards/"),
    ("/v1/awards/", "/api/v1/awards/"),
    ("api/v1/awards/", "/api/v1/awards/"),
    ("/api/v1/awards/", "/api/v1/awards/")
])
def test_format_path(path, formatted_path):
    assert format_path(path) == formatted_path


@pytest.mark.parametrize("path, checksum, expected_filename", [
    ("awards", "11111", "v1_awards_11111.csv"),
    ("/v1/awards/", "12345", "v1_awards_12345.csv"),
    ("transactions", "11111", "v1_transactions_11111.csv"),
])
def test_create_filename_from_options(path, checksum, expected_filename):
    assert create_filename_from_options(path, checksum) == expected_filename
