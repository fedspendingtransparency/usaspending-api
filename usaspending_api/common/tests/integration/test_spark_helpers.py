from unittest.mock import MagicMock

import pytest

from usaspending_api.common.etl.spark import rename_part_files
from usaspending_api.common.helpers.s3_helpers import _get_boto3, retrieve_s3_bucket_object_list


@pytest.fixture
def create_empty_files(s3_unittest_data_bucket):
    s3_client = _get_boto3("client", "s3")
    test_file_names = [
        "temp_download/new_file_name/another-csv-file.csv",
        "temp_download/new_file_name/another-txt-file.txt",
        "temp_download/new_file_name/part-sample-a.csv",
        "temp_download/new_file_name/part-sample-b.csv",
        "temp_download/new_file_name/part-sample-c.csv",
    ]
    for file_name in test_file_names:
        s3_client.put_object(Bucket=s3_unittest_data_bucket, Key=file_name)


def test_rename_part_files(create_empty_files, s3_unittest_data_bucket):
    object_keys = sorted(s3_object.key for s3_object in retrieve_s3_bucket_object_list(s3_unittest_data_bucket))
    assert object_keys == [
        "temp_download/new_file_name/another-csv-file.csv",
        "temp_download/new_file_name/another-txt-file.txt",
        "temp_download/new_file_name/part-sample-a.csv",
        "temp_download/new_file_name/part-sample-b.csv",
        "temp_download/new_file_name/part-sample-c.csv",
    ]

    logger = MagicMock()
    file_paths = rename_part_files(s3_unittest_data_bucket, "new_file_name", logger)

    expected_renamed_keys = [
        "temp_download/new_file_name_01.csv",
        "temp_download/new_file_name_02.csv",
        "temp_download/new_file_name_03.csv",
    ]

    expected_object_keys = [
        "temp_download/new_file_name/another-csv-file.csv",
        "temp_download/new_file_name/another-txt-file.txt",
        *expected_renamed_keys,
    ]
    assert sorted(file_paths) == [
        f"s3a://{s3_unittest_data_bucket}/{object_key}" for object_key in expected_renamed_keys
    ]

    object_keys = sorted(s3_object.key for s3_object in retrieve_s3_bucket_object_list(s3_unittest_data_bucket))
    assert object_keys == expected_object_keys
