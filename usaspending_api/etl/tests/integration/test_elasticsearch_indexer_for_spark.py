import tempfile
from decimal import Decimal

from django.core.management import call_command

from usaspending_api.common.etl.spark import create_ref_temp_views


test_data = [
    [
        "city",
        "city_ascii",
        "city_alt",
        "city_local",
        "city_local_lang",
        "lat",
        "lng",
        "country",
        "iso2",
        "IND",
        "admin_name",
        "admin_name_ascii",
        "admin_code",
        "admin_type",
        "capital",
        "density",
        "population",
        "population_proper",
        "ranking",
        "timezone",
        "same_name",
        "id",
    ],
    [
        "test_city",
        "test_city_ascii",
        "test_city_alt|another test city|hello world",
        "test_city_local",
        "test_city_local_lang",
        Decimal("90.0000"),
        Decimal("180.0000"),
        "test_country",
        "test_iso2",
        "DEU",
        "test_admin_name",
        "test_admin_name_ascii",
        "test_admin_code",
        "test_admin_type",
        "test_capital",
        10.0,
        100_000,
        100_000,
        "test_ranking",
        "test_timezone",
        False,
        1234,
    ],
]


def test_location_elasticsearch_indexer(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    create_ref_temp_views(spark)
    spark.sql("CREATE DATABASE IF NOT EXISTS rpt")

    with tempfile.NamedTemporaryFile(mode="w") as f:
        for row in test_data:
            f.write(",".join(str(v) for v in row) + "\n")
        f.flush()
        f.seek(0)
        call_command(
            "load_csv_to_delta",
            "--destination-table=world_cities",
            f"--source-path={f.name}",
            f"--spark-s3-bucket={s3_unittest_data_bucket}",
        )
    transaction_search_df = spark.DataFrame(
        [
            {
                "pop_state_code": "MO",
                "pop_congressional_code_current": "01",
                "recipient_location_state_code": "MO",
                "recipient_location_congressional_code_current": "01",
                "pop_congressional_code": "01",
                "recipient_locatoin_congressional_code": "01",
            },
            {
                "pop_state_code": "KS",
                "pop_congressional_code_current": "01",
                "recipient_location_state_code": "KS",
                "recipient_location_congressional_code_current": "01",
                "pop_congressional_code": "01",
                "recipient_locatoin_congressional_code": "01",
            },
        ]
    )
    transaction_search_df.write.saveAsTable("rpt.transaction_search")
    call_command(
        "elasticsearch_indexer_for_spark", create_new_index=True, load_type="location", index_name="2025-locations"
    )
