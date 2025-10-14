import tempfile
from decimal import Decimal

from django.core.management import call_command


def test_load_world_cities_csv_to_delta(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
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
            "iso3",
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
            "test_city_alt",
            "test_city_local",
            "test_city_local_lang",
            Decimal("90.0000"),
            Decimal("180.0000"),
            "test_country",
            "test_iso2",
            "test_iso3",
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
    with tempfile.NamedTemporaryFile(mode="w") as f:
        for row in test_data:
            f.write(",".join(str(v) for v in row) + "\n")
        f.flush()
        f.seek(0)
        call_command(
            "load_csv_to_delta",
            "--destination-table=world_cities",
            f"--alt-path={f.name}",
            f"--spark-s3-bucket={s3_unittest_data_bucket}",
        )
    df = spark.sql("SELECT * FROM raw.world_cities")
    assert df.count() == 1
    assert df.columns == test_data[0]
    assert [v for v in df.collect()[0].asDict().values()] == test_data[1]

    # Test overwrite

    test_data[1] = [
        "test_city_2",
        "test_city_ascii_2",
        "test_city_alt_2",
        "test_city_local_2",
        "test_city_local_lang_2",
        Decimal("89.0000"),
        Decimal("179.0000"),
        "test_country_2",
        "test_iso2_2",
        "test_iso3_2",
        "test_admin_name_2",
        "test_admin_name_ascii_2",
        "test_admin_code_2",
        "test_admin_type_2",
        "test_capital_2",
        11.0,
        100_001,
        100_001,
        "test_ranking_2",
        "test_timezone_2",
        True,
        1235,
    ]

    with tempfile.NamedTemporaryFile(mode="w") as f:
        for row in test_data:
            f.write(",".join(str(v) for v in row) + "\n")
        f.flush()
        f.seek(0)
        call_command(
            "load_csv_to_delta",
            "--destination-table=world_cities",
            f"--alt-path={f.name}",
            f"--spark-s3-bucket={s3_unittest_data_bucket}",
        )
    df = spark.sql("SELECT * FROM raw.world_cities")

    assert df.count() == 1
    assert df.columns == test_data[0]
    assert [v for v in df.collect()[0].asDict().values()] == test_data[1]
