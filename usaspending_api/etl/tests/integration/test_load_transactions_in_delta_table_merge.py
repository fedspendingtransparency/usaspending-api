from datetime import datetime, timedelta

import pytest
from django.core.management import call_command
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from usaspending_api.awards.delta_models.award_id_lookup import AWARD_ID_LOOKUP_SCHEMA
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.etl.management.commands.load_transactions_in_delta import Command as LoadTransactionsInDeltaCommand
from usaspending_api.transactions.delta_models.transaction_id_lookup import TRANSACTION_ID_LOOKUP_SCHEMA


@pytest.fixture
def prepare_delta_tables(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    action_date = "2025-01-01"
    updated_at = datetime.now()

    # Prepare raw tables
    call_command(
        "create_delta_table",
        "--destination-table=detached_award_procurement",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    detached_award_procurement_df = spark.createDataFrame(
        [
            (1, "unique_trx_id_10", "unique_award_id_1010", updated_at, action_date, "A", "award", None, None, None),
            (2, "unique_trx_id_20", "unique_award_id_1020", updated_at, action_date, None, "IDV", "B", "A", None),
            (
                3,
                "unique_trx_id_30",
                "unique_award_id_1030",
                updated_at,
                action_date,
                None,
                "IDV",
                "B",
                None,
                "INDEFINITE DELIVERY / REQUIREMENTS",
            ),
            (
                4,
                "unique_trx_id_40",
                "unique_award_id_1040",
                updated_at,
                action_date,
                None,
                "IDV",
                "B",
                None,
                "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
            ),
            (
                5,
                "unique_trx_id_50",
                "unique_award_id_1050",
                updated_at,
                action_date,
                None,
                "IDV",
                "B",
                None,
                "INDEFINITE DELIVERY / DEFINITE QUANTITY",
            ),
            (6, "unique_trx_id_60", "unique_award_id_1060", updated_at, action_date, None, "IDV", "E", None, None),
        ],
        schema=StructType(
            [
                StructField("detached_award_procurement_id", IntegerType()),
                StructField("detached_award_proc_unique", StringType()),
                StructField("unique_award_key", StringType()),
                StructField("updated_at", TimestampType()),
                StructField("action_date", StringType()),
                StructField("contract_award_type", StringType()),
                StructField("pulled_from", StringType()),
                StructField("idv_type", StringType()),
                StructField("type_of_idc", StringType()),
                StructField("type_of_idc_description", StringType()),
            ]
        ),
    )
    detached_award_procurement_df.write.format("delta").mode("append").saveAsTable("raw.detached_award_procurement")

    call_command(
        "create_delta_table",
        "--destination-table=published_fabs",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    published_fabs_df = spark.createDataFrame(
        [
            (7, "unique_trx_id_70", "unique_award_id_1070", updated_at, action_date, "02"),
            (8, "unique_trx_id_80", "unique_award_id_1080", updated_at, action_date, "F006"),
            (9, "unique_trx_id_90", "unique_award_id_1090", updated_at, action_date, "F005"),
            (10, "unique_trx_id_100", "unique_award_id_1100", updated_at, action_date, "F010"),
            (11, "unique_trx_id_110", "unique_award_id_1110", updated_at, action_date, "-1"),
        ],
        schema=StructType(
            [
                StructField("published_fabs_id", IntegerType()),
                StructField("afa_generated_unique", StringType()),
                StructField("unique_award_key", StringType()),
                StructField("updated_at", TimestampType()),
                StructField("action_date", StringType()),
                StructField("assistance_type", StringType()),
            ]
        ),
    )
    published_fabs_df.write.format("delta").mode("append").saveAsTable("raw.published_fabs")

    # Prepare lookup tables
    call_command(
        "create_delta_table",
        "--destination-table=award_id_lookup",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    award_id_lookup_df = spark.createDataFrame(
        [
            (1010, True, "UNIQUE_TRX_ID_10", "UNIQUE_AWARD_ID_1010"),
            (1020, True, "UNIQUE_TRX_ID_20", "UNIQUE_AWARD_ID_1020"),
            (1030, True, "UNIQUE_TRX_ID_30", "UNIQUE_AWARD_ID_1030"),
            (1040, True, "UNIQUE_TRX_ID_40", "UNIQUE_AWARD_ID_1040"),
            (1050, True, "UNIQUE_TRX_ID_50", "UNIQUE_AWARD_ID_1050"),
            (1060, True, "UNIQUE_TRX_ID_60", "UNIQUE_AWARD_ID_1060"),
            (1070, False, "UNIQUE_TRX_ID_70", "UNIQUE_AWARD_ID_1070"),
            (1080, False, "UNIQUE_TRX_ID_80", "UNIQUE_AWARD_ID_1080"),
            (1090, False, "UNIQUE_TRX_ID_90", "UNIQUE_AWARD_ID_1090"),
            (1100, False, "UNIQUE_TRX_ID_100", "UNIQUE_AWARD_ID_1100"),
            (1110, False, "UNIQUE_TRX_ID_110", "UNIQUE_AWARD_ID_1110"),
        ],
        schema=AWARD_ID_LOOKUP_SCHEMA,
    )
    award_id_lookup_df.write.format("delta").mode("append").saveAsTable("int.award_id_lookup")

    call_command(
        "create_delta_table",
        "--destination-table=transaction_id_lookup",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    transaction_id_lookup_df = spark.createDataFrame(
        [
            (10, True, "UNIQUE_TRX_ID_10"),
            (20, True, "UNIQUE_TRX_ID_20"),
            (30, True, "UNIQUE_TRX_ID_30"),
            (40, True, "UNIQUE_TRX_ID_40"),
            (50, True, "UNIQUE_TRX_ID_50"),
            (60, True, "UNIQUE_TRX_ID_60"),
            (70, False, "UNIQUE_TRX_ID_70"),
            (80, False, "UNIQUE_TRX_ID_80"),
            (90, False, "UNIQUE_TRX_ID_90"),
            (100, False, "UNIQUE_TRX_ID_100"),
            (110, False, "UNIQUE_TRX_ID_110"),
        ],
        schema=TRANSACTION_ID_LOOKUP_SCHEMA,
    )
    transaction_id_lookup_df.write.format("delta").mode("append").saveAsTable("int.transaction_id_lookup")

    # Create silver tables for populating later
    call_command(
        "create_delta_table",
        "--destination-table=transaction_fabs",
        "--alt-db=int",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    call_command(
        "create_delta_table",
        "--destination-table=transaction_fpds",
        "--alt-db=int",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    call_command(
        "create_delta_table",
        "--destination-table=transaction_normalized",
        "--alt-db=int",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )
    call_command(
        "create_delta_table",
        "--destination-table=awards",
        "--alt-db=int",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
    )

    # Tables needed by the transaction loader that are created without the normal management command workflow
    spark.sql(
        f"""
            CREATE OR REPLACE TABLE int.award_ids_delete_modified (award_id LONG NOT NULL)
            USING DELTA
            LOCATION 's3a://{s3_unittest_data_bucket}/data/delta/int/award_ids_delete_modified';
        """
    )

    # Prepare the command's class
    load_transaction_command = LoadTransactionsInDeltaCommand()
    load_transaction_command.last_etl_date = str(datetime.now() - timedelta(days=7))

    with load_transaction_command.prepare_spark():
        create_ref_temp_views(spark)
        yield spark, load_transaction_command


@pytest.mark.django_db(transaction=True)
def test_transaction_load_award_type_codes(prepare_delta_tables):
    spark, load_transaction_command = prepare_delta_tables

    # Load and Validate int.transaction_fabs
    load_transaction_command.etl_level = "transaction_fabs"
    spark.sql(load_transaction_command.transaction_fabs_fpds_merge_into_sql())
    df = (
        spark.table("int.transaction_fabs")
        .select(["transaction_id", "published_fabs_id", "afa_generated_unique", "assistance_type"])
        .orderBy("transaction_id")
    )
    expected_result = [
        {
            "transaction_id": 70,
            "published_fabs_id": 7,
            "afa_generated_unique": "UNIQUE_TRX_ID_70",
            "assistance_type": "02",
        },
        {
            "transaction_id": 80,
            "published_fabs_id": 8,
            "afa_generated_unique": "UNIQUE_TRX_ID_80",
            "assistance_type": "F006",
        },
        {
            "transaction_id": 90,
            "published_fabs_id": 9,
            "afa_generated_unique": "UNIQUE_TRX_ID_90",
            "assistance_type": "F005",
        },
        {
            "transaction_id": 100,
            "published_fabs_id": 10,
            "afa_generated_unique": "UNIQUE_TRX_ID_100",
            "assistance_type": "F010",
        },
        {
            "transaction_id": 110,
            "published_fabs_id": 11,
            "afa_generated_unique": "UNIQUE_TRX_ID_110",
            "assistance_type": "-1",
        },
    ]
    assert df.toPandas().to_dict(orient="records") == expected_result

    # Load and validate int.transaction_fpds
    load_transaction_command.etl_level = "transaction_fpds"
    spark.sql(load_transaction_command.transaction_fabs_fpds_merge_into_sql())
    df = (
        spark.table("int.transaction_fpds")
        .select(
            [
                "transaction_id",
                "detached_award_procurement_id",
                "detached_award_proc_unique",
                "contract_award_type",
                "pulled_from",
                "idv_type",
                "type_of_idc",
                "type_of_idc_description",
            ]
        )
        .orderBy("transaction_id")
    )
    expected_result = [
        {
            "transaction_id": 10,
            "detached_award_procurement_id": 1,
            "detached_award_proc_unique": "UNIQUE_TRX_ID_10",
            "contract_award_type": "A",
            "pulled_from": "AWARD",
            "idv_type": None,
            "type_of_idc": None,
            "type_of_idc_description": None,
        },
        {
            "transaction_id": 20,
            "detached_award_procurement_id": 2,
            "detached_award_proc_unique": "UNIQUE_TRX_ID_20",
            "contract_award_type": None,
            "pulled_from": "IDV",
            "idv_type": "B",
            "type_of_idc": "A",
            "type_of_idc_description": None,
        },
        {
            "transaction_id": 30,
            "detached_award_procurement_id": 3,
            "detached_award_proc_unique": "UNIQUE_TRX_ID_30",
            "contract_award_type": None,
            "pulled_from": "IDV",
            "idv_type": "B",
            "type_of_idc": None,
            "type_of_idc_description": "INDEFINITE DELIVERY / REQUIREMENTS",
        },
        {
            "transaction_id": 40,
            "detached_award_procurement_id": 4,
            "detached_award_proc_unique": "UNIQUE_TRX_ID_40",
            "contract_award_type": None,
            "pulled_from": "IDV",
            "idv_type": "B",
            "type_of_idc": None,
            "type_of_idc_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
        },
        {
            "transaction_id": 50,
            "detached_award_procurement_id": 5,
            "detached_award_proc_unique": "UNIQUE_TRX_ID_50",
            "contract_award_type": None,
            "pulled_from": "IDV",
            "idv_type": "B",
            "type_of_idc": None,
            "type_of_idc_description": "INDEFINITE DELIVERY / DEFINITE QUANTITY",
        },
        {
            "transaction_id": 60,
            "detached_award_procurement_id": 6,
            "detached_award_proc_unique": "UNIQUE_TRX_ID_60",
            "contract_award_type": None,
            "pulled_from": "IDV",
            "idv_type": "E",
            "type_of_idc": None,
            "type_of_idc_description": None,
        },
    ]
    assert df.toPandas().to_dict(orient="records") == expected_result

    # Load and validate int.transaction_normalized
    load_transaction_command.etl_level = "transaction_normalized"
    spark.sql(load_transaction_command.transaction_normalized_merge_into_sql("fabs"))
    spark.sql(load_transaction_command.transaction_normalized_merge_into_sql("fpds"))
    df = (
        spark.table("int.transaction_normalized")
        .select(["id", "unique_award_key", "transaction_unique_id", "type"])
        .orderBy("id")
    )
    expected_result = [
        {
            "id": 10,
            "unique_award_key": "UNIQUE_AWARD_ID_1010",
            "transaction_unique_id": "UNIQUE_TRX_ID_10",
            "type": "A",
        },
        {
            "id": 20,
            "unique_award_key": "UNIQUE_AWARD_ID_1020",
            "transaction_unique_id": "UNIQUE_TRX_ID_20",
            "type": "IDV_B_A",
        },
        {
            "id": 30,
            "unique_award_key": "UNIQUE_AWARD_ID_1030",
            "transaction_unique_id": "UNIQUE_TRX_ID_30",
            "type": "IDV_B_A",
        },
        {
            "id": 40,
            "unique_award_key": "UNIQUE_AWARD_ID_1040",
            "transaction_unique_id": "UNIQUE_TRX_ID_40",
            "type": "IDV_B_B",
        },
        {
            "id": 50,
            "unique_award_key": "UNIQUE_AWARD_ID_1050",
            "transaction_unique_id": "UNIQUE_TRX_ID_50",
            "type": "IDV_B_C",
        },
        {
            "id": 60,
            "unique_award_key": "UNIQUE_AWARD_ID_1060",
            "transaction_unique_id": "UNIQUE_TRX_ID_60",
            "type": "IDV_E",
        },
        {
            "id": 70,
            "unique_award_key": "UNIQUE_AWARD_ID_1070",
            "transaction_unique_id": "UNIQUE_TRX_ID_70",
            "type": "02",
        },
        {
            "id": 80,
            "unique_award_key": "UNIQUE_AWARD_ID_1080",
            "transaction_unique_id": "UNIQUE_TRX_ID_80",
            "type": "F006",
        },
        {
            "id": 90,
            "unique_award_key": "UNIQUE_AWARD_ID_1090",
            "transaction_unique_id": "UNIQUE_TRX_ID_90",
            "type": "F005",
        },
        {
            "id": 100,
            "unique_award_key": "UNIQUE_AWARD_ID_1100",
            "transaction_unique_id": "UNIQUE_TRX_ID_100",
            "type": "F010",
        },
        {
            "id": 110,
            "unique_award_key": "UNIQUE_AWARD_ID_1110",
            "transaction_unique_id": "UNIQUE_TRX_ID_110",
            "type": "-1",
        },
    ]
    assert df.toPandas().to_dict(orient="records") == expected_result

    # Load and validate int.awards
    load_transaction_command.etl_level = "awards"
    load_transaction_command.update_awards()
    df = spark.table("int.awards").select(["id", "generated_unique_award_id", "type", "category"]).orderBy("id")
    expected_result = [
        {"id": 1010, "generated_unique_award_id": "UNIQUE_AWARD_ID_1010", "type": "A", "category": "contract"},
        {"id": 1020, "generated_unique_award_id": "UNIQUE_AWARD_ID_1020", "type": "IDV_B_A", "category": "idv"},
        {
            "id": 1030,
            "generated_unique_award_id": "UNIQUE_AWARD_ID_1030",
            "type": "IDV_B_A",
            "category": "idv",
        },
        {
            "id": 1040,
            "generated_unique_award_id": "UNIQUE_AWARD_ID_1040",
            "type": "IDV_B_B",
            "category": "idv",
        },
        {
            "id": 1050,
            "generated_unique_award_id": "UNIQUE_AWARD_ID_1050",
            "type": "IDV_B_C",
            "category": "idv",
        },
        {
            "id": 1060,
            "generated_unique_award_id": "UNIQUE_AWARD_ID_1060",
            "type": "IDV_E",
            "category": "idv",
        },
        {"id": 1070, "generated_unique_award_id": "UNIQUE_AWARD_ID_1070", "type": "02", "category": "grant"},
        {"id": 1080, "generated_unique_award_id": "UNIQUE_AWARD_ID_1080", "type": "F006", "category": "direct payment"},
        {"id": 1090, "generated_unique_award_id": "UNIQUE_AWARD_ID_1090", "type": "F005", "category": "insurance"},
        {"id": 1100, "generated_unique_award_id": "UNIQUE_AWARD_ID_1100", "type": "F010", "category": "other"},
        {"id": 1110, "generated_unique_award_id": "UNIQUE_AWARD_ID_1110", "type": "-1", "category": None},
    ]
    assert df.toPandas().to_dict(orient="records") == expected_result
