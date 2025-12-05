from usaspending_api.common.spark.jobs import LocalStrategy, SparkJobs
from usaspending_api.etl.management.commands.load_query_to_delta import TABLE_SPEC


def test_local_spark_jobs_strategy(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    expected_table_name = "award_search"
    delta_table_spec = TABLE_SPEC[expected_table_name]
    expected_db_name = delta_table_spec["destination_database"]

    spark_jobs = SparkJobs(LocalStrategy())
    spark_jobs.start(
        job_name="create_delta_table-award_search",
        command_name="create_delta_table",
        command_options=[f"--destination-table={expected_table_name}", f"--spark-s3-bucket={s3_unittest_data_bucket}"],
    )

    schemas = spark.sql("show schemas").collect()
    assert expected_db_name in [s["namespace"] for s in schemas]

    tables = spark.sql("show tables").collect()
    assert expected_table_name in [t["tableName"] for t in tables]
