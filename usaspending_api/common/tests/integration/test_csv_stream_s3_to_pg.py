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
TEST_TABLE_COLUMNS = OrderedDict({
    "id": "INT PRIMARY KEY",
    "name": "VARCHAR(255)",
    "age": "INT",
    "email": "VARCHAR(255) UNIQUE"
})
TEST_TABLE_CSV_FILE = "usaspending_api/common/tests/data/test_csv_stream_s3_to_pg_data.csv"


@pytest.fixture
def s3_client_and_stubber():
    # Read file to use as mocked S3 object
    with open(TEST_TABLE_CSV_FILE, "rb") as f:
        body = f.read()

    # Set up the S3 client and the stubber
    s3 = boto3.client('s3')
    stubber = Stubber(s3)

    # Stubbing the HeadObject request first
    response_head_object = {'ContentLength': len(body)}
    stubber.add_response('head_object', response_head_object, {
        'Bucket': TEST_BUCKET,
        'Key': TEST_KEY,
    })

    # Stubbing the GetObject request
    response_get_object = {'Body': io.BytesIO(body)}
    stubber.add_response('get_object', response_get_object, {
        'Bucket': TEST_BUCKET,
        'Key': TEST_KEY,
    })

    stubber.activate()
    yield s3, stubber
    stubber.deactivate()


@pytest.mark.django_db
def test_integration_download_and_copy(s3_client_and_stubber):

    s3, stubber = s3_client_and_stubber

    logger = logging.getLogger("console")

    with connection.cursor() as cursor:
        column_strings = [f"{column_name} {data_type}" for column_name, data_type in TEST_TABLE_COLUMNS.items()]
        columns_sql = ", ".join(column_strings)
        cursor.execute(f"CREATE TABLE {TEST_TABLE_NAME} ({columns_sql});")

        # Call the function
        next(_download_and_copy(
            logger,
            cursor=cursor,
            s3_client=s3,
            s3_bucket_name=TEST_BUCKET,
            s3_obj_key=TEST_KEY,
            target_pg_table=TEST_TABLE_NAME,
            ordered_col_names=TEST_TABLE_COLUMNS.keys(),
            gzipped=False
        ))

        # Verify the data was written to the DB
        cursor.execute(f"SELECT * FROM {TEST_TABLE_NAME}")
        results = cursor.fetchall()
        assert len(results) > 0

        cursor.execute(f"DROP TABLE {TEST_TABLE_NAME}")
