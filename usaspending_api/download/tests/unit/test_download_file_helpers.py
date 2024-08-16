from unittest.mock import MagicMock
from datetime import datetime

from usaspending_api.download.helpers.download_file_helpers import (
    get_last_modified_file,
    get_last_modified_int,
    remove_file_prefix_if_exists,
)


def test_get_last_modified_file():
    # Mocking files with last_modified as datetime objects
    file1 = MagicMock(key="file1.zip")
    file1.last_modified = datetime.strptime("2023-01-01", "%Y-%m-%d")

    file2 = MagicMock(key="file2.zip")
    file2.last_modified = datetime.strptime("2023-01-02", "%Y-%m-%d")

    files = [file1, file2]

    # The function should return 'file2.zip' as it is the most recent
    result = get_last_modified_file(files)
    assert result == "file2.zip"


def test_get_last_modified_int():
    mock_file = MagicMock()
    mock_file.last_modified.strftime.return_value = "1680382800"
    result = get_last_modified_int(mock_file)
    assert result == 1680382800


def test_remove_file_prefix_if_exists():
    result = remove_file_prefix_if_exists("prefix/file.zip", "prefix/")
    assert result == "file.zip"

    result = remove_file_prefix_if_exists("prefix/file.zip", "prefix")
    assert result == "/file.zip"

    # Example of what happens when the regex doesn't match
    result = remove_file_prefix_if_exists("prefix/file.zip", "prefix//")
    assert result == "prefix/file.zip"

    result = remove_file_prefix_if_exists("file.zip", "prefix")
    assert result == "file.zip"

    result = remove_file_prefix_if_exists(None, "prefix")
    assert result is None
