import inspect
import logging
from datetime import datetime

import pandas as pd
from pyspark.sql import Row
from pytest import mark

from usaspending_api.common.etl.spark import rename_part_files, write_csv_file
from usaspending_api.common.helpers.s3_helpers import download_s3_object
from usaspending_api.config import CONFIG

logger = logging.getLogger("script")


@mark.django_db(transaction=True)  # Need this to save/commit it to Postgres, so we can dump it to delta
def test_write_csv_file(
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
    write_csv_file(spark, df, parts_dir=bucket_path, logger=test_logger)
    file_prefix = "csv"
    full_csv_file_paths = rename_part_files(
        bucket_name=bucket_name,
        destination_file_name=f"csv/unit_test_csv_data/{file_timestamp}",
        temp_download_dir_name="data",
        logger=test_logger,
    )
    test_logger.info(f"CSV file paths: {full_csv_file_paths}")
    results_path = f"{obj_prefix}_01.{file_prefix}"
    download_path = tmp_path / f"{file_timestamp}.{file_prefix}"
    test_logger.info(f"Downloading test results from {results_path} to {download_path}")
    download_s3_object(
        bucket_name,
        f"{results_path}",
        f"{download_path}",
    )
    pd_df = pd.read_csv(download_path)
    print(pd_df)
    assert len(pd_df) == 3
    assert len(pd_df.columns) > 1
