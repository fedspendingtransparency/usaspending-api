import pandas as pd
import logging
import inspect

from pytest import mark
from datetime import datetime

from usaspending_api.common.etl.spark import hadoop_copy_merge, write_csv_file
from usaspending_api.common.helpers.s3_helpers import download_s3_object
from usaspending_api.config import CONFIG
from pyspark.sql import Row

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
    write_csv_file(spark, df, parts_dir=bucket_path, num_partitions=1, logger=test_logger)
    hadoop_copy_merge(
        spark=spark,
        parts_dir=bucket_path,
        part_merge_group_size=1,
        header="first_col, color, numeric_val",
        logger=logger,
    )
    file_prefix = "csv"
    results_path = f"{obj_prefix}.{file_prefix}"
    download_path = tmp_path / f"{file_timestamp}.{file_prefix}"
    test_logger.info(f"Downloading test results from {results_path} to {download_path}")
    download_s3_object(
        bucket_name,
        f"{results_path}",
        f"{download_path}",
    )
    pd_df = pd.read_csv(download_path)
    assert len(pd_df) == 3
    assert len(pd_df.columns) > 1
