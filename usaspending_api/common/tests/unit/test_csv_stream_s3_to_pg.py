import logging
import pytest
from unittest.mock import Mock, mock_open, patch

from usaspending_api.common.csv_stream_s3_to_pg import _download_and_copy

# Dummy values
DUMMY_BUCKET = "mybucket"
DUMMY_KEY = "mykey"
DUMMY_TABLE = "mytable"
DUMMY_COLS = ["col1", "col2"]

logger = logging.getLogger("console")


@pytest.fixture
def mock_dependencies():
    with (
        patch("tempfile.NamedTemporaryFile") as mock_tempfile,
        patch("usaspending_api.common.helpers.s3_helpers.download_s3_object"),
        patch("gzip.open") as mock_gzip_open,
        patch("builtins.open", mock_open(read_data="data")),
    ):

        mock_tempfile.return_value.__enter__.return_value.name = "temp_file_name"
        mock_cursor = Mock()
        mock_cursor.copy_expert.return_value = None
        mock_cursor.rowcount = 5

        mock_gzip_open.return_value.__enter__.return_value = mock_open(read_data="data").return_value

        return mock_cursor


def test_download_and_copy_gzipped_success(mock_dependencies):
    # Mocks will be active here, thanks to the fixture.
    rows = next(
        _download_and_copy(
            logger,
            mock_dependencies,
            Mock(),
            DUMMY_BUCKET,
            DUMMY_KEY,
            DUMMY_TABLE,
            DUMMY_COLS,
            gzipped=True,
        )
    )

    assert rows == 5  # as we've mocked cursor.rowcount to 5


def test_download_and_copy_not_gzipped_success(mock_dependencies):
    # Mocks will be active here, thanks to the fixture.
    rows = next(
        _download_and_copy(
            logger,
            mock_dependencies,
            Mock(),
            DUMMY_BUCKET,
            DUMMY_KEY,
            DUMMY_TABLE,
            DUMMY_COLS,
            gzipped=False,
        )
    )

    assert rows == 5  # as we've mocked cursor.rowcount to 5


def test_download_and_copy_copy_failure(mock_dependencies):
    mock_dependencies.copy_expert.side_effect = Exception("DB Error")

    with pytest.raises(Exception, match="DB Error"):
        next(
            _download_and_copy(
                logger,
                mock_dependencies,
                Mock(),
                DUMMY_BUCKET,
                DUMMY_KEY,
                DUMMY_TABLE,
                DUMMY_COLS,
                gzipped=True,
            )
        )
