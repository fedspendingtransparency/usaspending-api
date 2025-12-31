from pytest import mark

from django.core.management import call_command


@mark.django_db(transaction=True)  # Need this to save/commit it to Postgres, so we can dump it to delta
def test_load_csv_file(
    spark,
    s3_unittest_data_bucket,
    hive_unittest_metastore_db,
):
    call_command("execute_spark_sql", f"--sql", "SHOW DATABASES;")
