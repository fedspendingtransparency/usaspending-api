import boto3
import io
import logging
import pytest

from botocore.stub import Stubber
from collections import OrderedDict
from django.db import connection

from usaspending_api.common.csv_stream_s3_to_pg import _download_and_copy

# Test Values
TEST_BUCKET = "mybucket"
TEST_KEY = "mykey"
TEST_TABLE_NAME = "employee"
TEST_TABLE_COLUMNS = OrderedDict(
    {"id": "INT PRIMARY KEY", "name": "VARCHAR(255)", "age": "INT", "email": "VARCHAR(255) UNIQUE"}
)
TEST_TABLE_CSV_FILE = "usaspending_api/common/tests/data/test_csv_stream_s3_to_pg_data.csv"
NUMBER_OF_ROWS_IN_FILE = 4

logger = logging.getLogger("console")


@pytest.fixture
def mocked_s3_client():
    """Creates a mock s3 client that will return the contents of a file when the `download_file`
    method is used.
    """

    # Read file to use as mocked S3 object
    with open(TEST_TABLE_CSV_FILE, "rb") as f:
        body = f.read()

    # Set up the S3 client and the stubber
    s3 = boto3.client("s3")
    stubber = Stubber(s3)

    # Stubbing the HeadObject request first
    response_head_object = {"ContentLength": len(body)}
    stubber.add_response(
        "head_object",
        response_head_object,
        {
            "Bucket": TEST_BUCKET,
            "Key": TEST_KEY,
        },
    )

    # Stubbing the GetObject request
    response_get_object = {"Body": io.BytesIO(body)}
    stubber.add_response(
        "get_object",
        response_get_object,
        {
            "Bucket": TEST_BUCKET,
            "Key": TEST_KEY,
        },
    )

    stubber.activate()
    yield s3
    stubber.deactivate()


@pytest.fixture
def db_cursor_with_test_table():
    """Creates a DB cursor and a temporary file used for tests in this file"""

    cursor = connection.cursor()

    column_strings = [f"{column_name} {data_type}" for column_name, data_type in TEST_TABLE_COLUMNS.items()]
    columns_sql = ", ".join(column_strings)
    cursor.execute(f"CREATE TABLE {TEST_TABLE_NAME} ({columns_sql});")

    yield cursor

    cursor.execute(f"DROP TABLE {TEST_TABLE_NAME}")
    cursor.close()


@pytest.mark.django_db
def test_integration_download_and_copy_happy(mocked_s3_client, db_cursor_with_test_table):

    next(
        _download_and_copy(
            logger,
            cursor=db_cursor_with_test_table,
            s3_client=mocked_s3_client,
            s3_bucket_name=TEST_BUCKET,
            s3_obj_key=TEST_KEY,
            target_pg_table=TEST_TABLE_NAME,
            ordered_col_names=TEST_TABLE_COLUMNS.keys(),
            gzipped=False,
        )
    )

    # Verify the data was written to the DB
    db_cursor_with_test_table.execute(f"SELECT * FROM {TEST_TABLE_NAME}")
    results = db_cursor_with_test_table.fetchall()

    assert len(results) > 0, f"No rows were written to {TEST_TABLE_NAME} table"
    assert (
        len(results) == NUMBER_OF_ROWS_IN_FILE
    ), f"Expecting {NUMBER_OF_ROWS_IN_FILE} to be written to table, but {len(results)} found"
