from datetime import datetime, timedelta, timezone

import pytest
from django.core.management import call_command
from usaspending_api.common.helpers.spark_helpers import load_dict_to_delta_table

_BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
_INITIAL_DATETIME = datetime(2022, 10, 31, tzinfo=timezone.utc)
_INITIAL_SOURCE_TABLE_LOAD_DATETIME = _INITIAL_DATETIME + timedelta(hours=12)
_INITIAL_ASSISTS = [
    {
        "published_fabs_id": 1,
        "afa_generated_unique": "award_assist_0001_trans_0001",
        "action_date": _INITIAL_DATETIME.isoformat(),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "is_active": True,
        "unique_award_key": "award_assist_0001",
        "hash": 1020304,
    },
    {
        "published_fabs_id": 2,
        "afa_generated_unique": "award_assist_0002_trans_0001",
        "action_date": _INITIAL_DATETIME.isoformat(),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "is_active": True,
        "unique_award_key": "award_assist_0002",
        "hash": 5060708,
    },
    {
        "published_fabs_id": 3,
        "afa_generated_unique": "award_assist_0002_trans_0002",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": _INITIAL_DATETIME.strftime("%Y%m%d"),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "is_active": True,
        "unique_award_key": "award_assist_0002",
        "hash": 9101112,
    },
    {
        "published_fabs_id": 4,
        "afa_generated_unique": "award_assist_0003_trans_0001",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": _INITIAL_DATETIME.strftime("%Y%m%d"),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "is_active": True,
        "unique_award_key": "award_assist_0003",
        "hash": 13141516,
    },
    {
        "published_fabs_id": 5,
        "afa_generated_unique": "award_assist_0004_trans_0001",
        "action_date": _INITIAL_DATETIME.isoformat(),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "is_active": True,
        "unique_award_key": "award_assist_0004",
        "hash": 17181920,
    },
]
_INITIAL_PROCURES = [
    {
        "detached_award_procurement_id": 1,
        "detached_award_proc_unique": "award_procure_0001_trans_0001",
        "action_date": _INITIAL_DATETIME.isoformat(),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "unique_award_key": "award_procure_0001",
        "hash": 1020304,
    },
    {
        "detached_award_procurement_id": 2,
        "detached_award_proc_unique": "award_procure_0002_trans_0001",
        "action_date": _INITIAL_DATETIME.isoformat(),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "unique_award_key": "award_procure_0002",
        "hash": 5060708,
    },
    {
        "detached_award_procurement_id": 3,
        "detached_award_proc_unique": "award_procure_0002_trans_0002",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": _INITIAL_DATETIME.strftime("%Y%m%d"),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "unique_award_key": "award_procure_0002",
        "hash": 9101112,
    },
    {
        "detached_award_procurement_id": 4,
        "detached_award_proc_unique": "award_procure_0003_trans_0001",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": _INITIAL_DATETIME.strftime("%Y%m%d"),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "unique_award_key": "award_procure_0003",
        "hash": 13141516,
    },
    {
        "detached_award_procurement_id": 5,
        "detached_award_proc_unique": "award_procure_0003_trans_0002",
        "action_date": _INITIAL_DATETIME.isoformat(),
        "created_at": _INITIAL_DATETIME,
        "updated_at": _INITIAL_DATETIME,
        "unique_award_key": "award_procure_0003",
        "hash": 17181920,
    },
]


@pytest.mark.django_db
def test_load_transactions(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):

    # Load initial Data
    load_dict_to_delta_table(
        spark, s3_unittest_data_bucket, "raw", "detached_award_procurement", _INITIAL_PROCURES, True
    )
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "raw", "published_fabs", _INITIAL_ASSISTS, True)

    # Create Delta Tables
    call_command("create_delta_table", "--destination-table=transaction_fpds", "--alt-db=int")
    call_command("create_delta_table", "--destination-table=transaction_fabs", "--alt-db=int")
    call_command("create_delta_table", "--destination-table=transaction_normalized", "--alt-db=int")
    call_command("create_delta_table", "--destination-table=awards", "--alt-db=int")

    # Load Transactions
    call_command("load_transaction_fpds_in_delta")
    call_command("load_transaction_fabs_in_delta")
    call_command("load_transaction_normalized")
    call_command("load_awards_in_delta")

    # Check Transaction FPDS
    fpds_df = spark.sql("select * from int.transaction_fpds")
    fpds_transactions = set(
        row["detached_award_proc_unique"] for row in fpds_df.select("detached_award_proc_unique").collect()
    )
    initial_fpds_transactions = set(
        transaction["detached_award_proc_unique"].upper() for transaction in _INITIAL_PROCURES
    )
    assert initial_fpds_transactions == fpds_transactions

    # Check Transaction FABS
    fabs_df = spark.sql("select * from int.transaction_fabs")
    fabs_transactions = set(row["afa_generated_unique"] for row in fabs_df.select("afa_generated_unique").collect())
    initial_fabs_transactions = set(transaction["afa_generated_unique"].upper() for transaction in _INITIAL_ASSISTS)
    assert initial_fabs_transactions == fabs_transactions

    # Check Transaction Normalized
    norm_df = spark.sql("select * from int.transaction_normalized")
    norm_transactions = set(row["transaction_unique_id"] for row in norm_df.select("transaction_unique_id").collect())
    assert norm_transactions == initial_fpds_transactions.union(initial_fabs_transactions)

    # Check Awards
    award_df = spark.sql("select * from int.awards")
    awards = set(row["generated_unique_award_id"] for row in award_df.select("generated_unique_award_id").collect())
    initial_awards = set(transaction["unique_award_key"].upper() for transaction in _INITIAL_PROCURES).union(
        set(transaction["unique_award_key"].upper() for transaction in _INITIAL_ASSISTS)
    )
    assert initial_awards == awards

    # Delete some contracts and assistance
    spark.sql("DELETE FROM raw.detached_award_procurement WHERE unique_award_key = 'award_procure_0001'")
    spark.sql("DELETE FROM raw.published_fabs WHERE unique_award_key = 'award_assist_0001'")

    # Update a transaction
    spark.sql(
        "UPDATE raw.published_fabs set award_description = 'test award', hash =  25262728 where  published_fabs_id = 2"
    )

    # Load some new contracts and assistance
    new_assist = [
        {
            "published_fabs_id": 6,
            "afa_generated_unique": "award_assist_0005_trans_0001",
            "action_date": _INITIAL_DATETIME.isoformat(),
            "created_at": _INITIAL_DATETIME,
            "updated_at": _INITIAL_DATETIME,
            "is_active": True,
            "unique_award_key": "award_assist_0005",
            "hash": 21222324,
        }
    ]
    new_proc = [
        {
            "detached_award_procurement_id": 6,
            "detached_award_proc_unique": "award_procure_0004_trans_0001",
            "action_date": _INITIAL_DATETIME.isoformat(),
            "created_at": _INITIAL_DATETIME,
            "updated_at": _INITIAL_DATETIME,
            "unique_award_key": "award_procure_0004",
            "hash": 21222324,
        }
    ]
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "raw", "detached_award_procurement", new_proc, False)
    load_dict_to_delta_table(spark, s3_unittest_data_bucket, "raw", "published_fabs", new_assist, False)

    # Reload Transactions
    call_command("load_transaction_fpds_in_delta")
    call_command("load_transaction_fabs_in_delta")
    call_command("load_transaction_normalized")
    call_command("load_awards_in_delta")

    # Check Transaction FPDS
    fpds_df = spark.sql("select * from int.transaction_fpds")
    fpds_transactions = set(
        row["detached_award_proc_unique"] for row in fpds_df.select("detached_award_proc_unique").collect()
    )
    expected_fpds_transactions = set(
        transaction["detached_award_proc_unique"].upper()
        for transaction in _INITIAL_PROCURES + new_proc
        if transaction["unique_award_key"] != "award_procure_0001"
    )
    assert expected_fpds_transactions == fpds_transactions

    # Check Transaction FABS
    fabs_df = spark.sql("select * from int.transaction_fabs")
    fabs_transactions = set(row["afa_generated_unique"] for row in fabs_df.select("afa_generated_unique").collect())
    expected_fabs_transactions = set(
        transaction["afa_generated_unique"].upper()
        for transaction in _INITIAL_ASSISTS + new_assist
        if transaction["unique_award_key"] != "award_assist_0001"
    )
    assert expected_fabs_transactions == fabs_transactions

    # Check Transaction Normalized
    norm_df = spark.sql("select * from int.transaction_normalized")
    norm_transactions = set(row["transaction_unique_id"] for row in norm_df.select("transaction_unique_id").collect())
    assert norm_transactions == expected_fpds_transactions.union(expected_fabs_transactions)

    # Check Awards
    award_df = spark.sql("select * from int.awards")
    awards = set(row["generated_unique_award_id"] for row in award_df.select("generated_unique_award_id").collect())
    expected_awards = set(
        transaction["unique_award_key"].upper()
        for transaction in _INITIAL_PROCURES + new_proc
        if transaction["unique_award_key"] != "award_procure_0001"
    ).union(
        set(
            transaction["unique_award_key"].upper()
            for transaction in _INITIAL_ASSISTS + new_assist
            if transaction["unique_award_key"] != "award_assist_0001"
        )
    )
    assert expected_awards == awards
    assert (
        spark.sql("select description  from int.awards where generated_unique_award_id = 'AWARD_ASSIST_0002'")
        .collect()[0]
        .description
        == "TEST AWARD"
    )
