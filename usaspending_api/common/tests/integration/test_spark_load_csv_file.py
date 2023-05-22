import pandas as pd
import logging
import inspect

from pytest import mark
from datetime import datetime

from usaspending_api.common.helpers.s3_helpers import download_s3_object
from usaspending_api.common.etl.spark import load_csv_file_and_zip
from usaspending_api.common.helpers.sql_helpers import execute_sql_simple
from usaspending_api.tests.conftest_spark import create_and_load_all_delta_tables
from usaspending_api.config import CONFIG


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
    load_csv_file_and_zip(spark, df, parts_dir=bucket_path, compress=False, logger=test_logger)
    download_path = tmp_path / f"{file_timestamp}.csv"
    download_s3_object(
        s3_bucket_name=bucket_name,
        s3_obj_key=f"{obj_prefix}.csv",
        download_path=download_path,
        configured_logger=test_logger,
    )
    pd_df = pd.read_csv(download_path)
    assert len(pd_df) == 3  # number of transaction records created in data setup fixture
    assert len(pd_df.columns) > 1


def test_fixtures(db, broker_server_dblink_setup):
    restock_duns_sql = open("usaspending_api/broker/management/sql/restock_duns.sql", "r").read()
    execute_sql_simple(restock_duns_sql.replace("VACUUM ANALYZE int.duns;", ""))
