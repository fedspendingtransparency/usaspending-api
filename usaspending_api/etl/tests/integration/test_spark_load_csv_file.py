import inspect
import logging
from datetime import datetime

import pandas as pd
from pytest import mark

from usaspending_api.common.etl.spark import rename_part_files, write_csv_file
from usaspending_api.common.helpers.s3_helpers import download_s3_object
from usaspending_api.config import CONFIG
from usaspending_api.tests.conftest_spark import create_and_load_all_delta_tables


@mark.django_db(transaction=True)  # Need this to save/commit it to Postgres, so we can dump it to delta
def test_load_csv_file(
    spark,
    s3_unittest_data_bucket,
    hive_unittest_metastore_db,
    populate_usas_data,
    tmp_path,
):
    tables_to_load = [
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    df = spark.table("int.transaction_fpds")
    test_logger = logging.getLogger(inspect.currentframe().f_back.f_globals["__name__"])
    test_logger.info(f"'int.transction_fpds' has {df.count()} records for export")
    file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    bucket_name = s3_unittest_data_bucket
    obj_prefix = f"{CONFIG.SPARK_CSV_S3_PATH}/unit_test_csv_data/{file_timestamp}"
    bucket_path = f"s3a://{bucket_name}/{obj_prefix}"
    write_csv_file(spark, df, parts_dir=bucket_path, logger=test_logger)  # write Delta to CSV in S3 using Spark
    file_ext = "csv"
    rename_part_files(
        bucket_name=bucket_name,
        destination_file_name=f"csv/unit_test_csv_data/{file_timestamp}",
        temp_download_dir_name="data",
        file_format=file_ext,
        logger=test_logger,
    )
    download_path = tmp_path / f"{file_timestamp}.{file_ext}"
    download_s3_object(
        bucket_name=bucket_name,
        key=f"{obj_prefix}_01.{file_ext}",
        file_path=str(download_path),
    )
    pd_df = pd.read_csv(download_path)
    assert len(pd_df) == 3  # number of transaction records created in data setup fixture
    assert len(pd_df.columns) > 1
