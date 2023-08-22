import pandas as pd
import logging
import inspect

from pytest import mark
from datetime import datetime

from usaspending_api.common.etl.spark import hadoop_copy_merge, load_csv_file
from usaspending_api.common.helpers.s3_helpers import download_s3_object
from usaspending_api.config import CONFIG
from usaspending_api.etl.tests.integration.test_load_to_from_delta import create_and_load_all_delta_tables

from botocore.client import BaseClient

from pathlib import Path
from typing import Union
from contextlib import closing
import boto3
import logging
from usaspending_api.config import CONFIG
from pyspark.sql import Row

logger = logging.getLogger("script")


def _get_boto3_s3_client() -> BaseClient:
    boto3_session = boto3.session.Session(
        region_name=CONFIG.AWS_REGION,
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )
    s3_client = boto3_session.client(
        service_name="s3",
        region_name=CONFIG.AWS_REGION,
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
    )
    return s3_client


def _get_s3_object_bytes(s3_bucket_name: str, s3_obj_key: str, configured_logger=None) -> bytes:
    if not configured_logger:
        configured_logger = logger
    s3_client = _get_boto3_s3_client()
    try:
        s3_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_obj_key)
        # Getting Body gives a botocore.response.StreamingBody object back to allow "streaming" its contents
        s3_obj_body = s3_obj["Body"]
        with closing(s3_obj_body):  # make sure to close the stream when done
            obj_bytes = s3_obj_body.read()
            configured_logger.info(
                f"Finished reading s3 object bytes for '{s3_obj_key}' from bucket '{s3_bucket_name}'"
            )
            return obj_bytes
    except Exception as exc:
        configured_logger.error(f"ERROR reading object '{s3_obj_key}' from bucket '{s3_bucket_name}'")
        configured_logger.exception(exc)
        raise exc


def _download_s3_object(
    s3_bucket_name: str, s3_obj_key: str, download_path: Union[str, Path], configured_logger=None
) -> None:
    try:
        with open(download_path, "wb") as csv_file:
            csv_file.write(
                _get_s3_object_bytes(
                    s3_bucket_name=s3_bucket_name,
                    s3_obj_key=s3_obj_key,
                    configured_logger=configured_logger,
                )
            )

        configured_logger.info(
            f"Finished writing s3 object '{s3_obj_key}' from bucket '{s3_bucket_name}' to path '{str(download_path)}'"
        )
    except Exception as exc:
        configured_logger.error(f"ERROR downloading object '{s3_obj_key}' from bucket '{s3_bucket_name}'")
        configured_logger.exception(exc)
        raise exc


@mark.django_db(transaction=True)  # Need this to save/commit it to Postgres, so we can dump it to delta
def test_load_csv_file(
    spark,
    s3_unittest_data_bucket,
    hive_unittest_metastore_db,
    tmp_path,
):
    data = [
        {"first_col": "row 1", "color": "blue", "numeric_val": 1},
        {"first_col": "row 2", "color": "green", "numeric_val": 2},
        {"first_col": "row 3", "color": "pink", "numeric_val": 3},
    ]
    df = spark.createDataFrame([Row(**data_row) for data_row in data])
    test_logger = logging.getLogger(inspect.currentframe().f_back.f_globals["__name__"])
    test_logger.info(f"test data has {df.count()} records for export")
    file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    bucket_name = s3_unittest_data_bucket
    obj_prefix = f"{CONFIG.SPARK_CSV_S3_PATH}/unit_test_csv_data/{file_timestamp}"
    bucket_path = f"s3a://{bucket_name}/{obj_prefix}"
    load_csv_file(spark, df, parts_dir=bucket_path, logger=test_logger)
    hadoop_copy_merge(
        spark=spark,
        parts_dir=bucket_path,
        header="first_col, color, numeric_val",
        logger=logger,
    )
    file_prefix = "csv"
    results_path = f"{obj_prefix}.{file_prefix}"
    download_path = tmp_path / f"{file_timestamp}.{file_prefix}"
    test_logger.info(f"Downloading test results from {results_path} to {download_path}")
    _download_s3_object(
        s3_bucket_name=bucket_name,
        s3_obj_key=results_path,
        download_path=download_path,
        configured_logger=logger,
    )
    pd_df = pd.read_csv(download_path)
    assert len(pd_df) == 3
    assert len(pd_df.columns) > 1
