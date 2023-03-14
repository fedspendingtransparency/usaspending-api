from pytest import mark

from usaspending_api.common.etl.spark import load_csv_file
from usaspending_api.common.helpers.sql_helpers import execute_sql_simple
from usaspending_api.tests.conftest_spark import create_and_load_all_delta_tables
from datetime import datetime
from usaspending_api.config import CONFIG


@mark.django_db(transaction=True)  # Need this to save/commit it to Postgres, so we can dump it to delta
def test_load_csv_file(
    spark,
    s3_unittest_data_bucket,
    hive_unittest_metastore_db,
    populate_usas_data,
):
    tables_to_load = [
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    df = spark.table("rtp.transaction_search")
    file_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    bucket_path = f"s3a://{CONFIG.SPARK_S3_BUCKET}/{CONFIG.SPARK_CSV_S3_PATH}/unit_test_csv_data/{file_timestamp}"
    load_csv_file(spark, df, parts_dir=bucket_path, compress=False)


def test_fixtures(db, broker_server_dblink_setup):
    restock_duns_sql = open("usaspending_api/broker/management/sql/restock_duns.sql", "r").read()
    execute_sql_simple(restock_duns_sql.replace("VACUUM ANALYZE int.duns;", ""))
