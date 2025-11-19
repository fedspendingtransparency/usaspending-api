import pytest

from usaspending_api.common.helpers.s3_helpers import _get_boto3, rename_s3_object, retrieve_s3_bucket_object_list


@pytest.fixture
def create_empty_files(s3_unittest_data_bucket):
    s3_client = _get_boto3("client", "s3")
    test_file_names = ["SAMPLE_FILE_A.txt", "SAMPLE_FILE_B.txt", "SAMPLE_FILE_C.txt", "ANOTHER_FILE.txt"]
    for file_name in test_file_names:
        s3_client.put_object(Bucket=s3_unittest_data_bucket, Key=file_name)


def test_retrieve_s3_bucket_object_list(create_empty_files, s3_unittest_data_bucket):
    object_keys = sorted(s3_object.key for s3_object in retrieve_s3_bucket_object_list(s3_unittest_data_bucket))
    assert object_keys == ["ANOTHER_FILE.txt", "SAMPLE_FILE_A.txt", "SAMPLE_FILE_B.txt", "SAMPLE_FILE_C.txt"]

    object_keys = sorted(
        s3_object.key for s3_object in retrieve_s3_bucket_object_list(s3_unittest_data_bucket, key_prefix="SAMPLE_FILE")
    )
    assert object_keys == ["SAMPLE_FILE_A.txt", "SAMPLE_FILE_B.txt", "SAMPLE_FILE_C.txt"]


def test_rename_s3_object(create_empty_files, s3_unittest_data_bucket):
    object_keys = sorted(s3_object.key for s3_object in retrieve_s3_bucket_object_list(s3_unittest_data_bucket))
    assert object_keys == ["ANOTHER_FILE.txt", "SAMPLE_FILE_A.txt", "SAMPLE_FILE_B.txt", "SAMPLE_FILE_C.txt"]

    rename_s3_object(s3_unittest_data_bucket, "ANOTHER_FILE.txt", "CHANGED_NAME.txt")

    object_keys = sorted(s3_object.key for s3_object in retrieve_s3_bucket_object_list(s3_unittest_data_bucket))
    assert object_keys == ["CHANGED_NAME.txt", "SAMPLE_FILE_A.txt", "SAMPLE_FILE_B.txt", "SAMPLE_FILE_C.txt"]
