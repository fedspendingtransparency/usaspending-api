from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from model_bakery import baker

from usaspending_api.tests.conftest_spark import create_and_load_all_delta_tables, create_all_delta_tables


@pytest.fixture
def initial_award_and_subaward_data():
    initial_update = datetime.now() - timedelta(days=10)
    static_award_no_subawards = baker.make(
        "search.AwardSearch",
        award_id=1,
        update_date=initial_update,
        transaction_unique_id="1",
        generated_unique_award_id="1",
        subaward_count=0,
        total_subaward_amount=None,
    )
    static_award_with_subawards = baker.make(
        "search.AwardSearch",
        award_id=2,
        update_date=initial_update,
        transaction_unique_id="2",
        generated_unique_award_id="2",
        subaward_count=1,
        total_subaward_amount=25.0,
    )
    manipulated_award_with_subawards = baker.make(
        "search.AwardSearch",
        award_id=3,
        update_date=initial_update,
        transaction_unique_id="3",
        generated_unique_award_id="3",
        subaward_count=2,
        total_subaward_amount=150.0,
    )
    broker_subawards = [
        {
            "unique_award_key": static_award_with_subawards.generated_unique_award_id,
            "subaward_amount": 25.0,
            "id": 123,
            "subaward_number": "123",
            "sub_action_date": "2024-01-01",
        },
        {
            "unique_award_key": manipulated_award_with_subawards.generated_unique_award_id,
            "subaward_amount": 100.0,
            "id": 456,
            "subaward_number": "456",
            "sub_action_date": "2024-01-01",
        },
        {
            "unique_award_key": manipulated_award_with_subawards.generated_unique_award_id,
            "subaward_amount": 40.0,
            "id": 789,
            "subaward_number": "789",
            "sub_action_date": "2024-01-01",
        },
    ]
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        for subaward in broker_subawards:
            cursor.execute(
                """
                    INSERT INTO subaward (unique_award_key, subaward_amount, id, subaward_number, sub_action_date)
                    VALUES (%(unique_award_key)s, %(subaward_amount)s, %(id)s, %(subaward_number)s, %(sub_action_date)s)
                """,
                subaward,
            )

    yield {
        "static_award_no_subawards": static_award_no_subawards,
        "static_award_with_subawards": static_award_with_subawards,
        "manipulated_award_with_subawards": manipulated_award_with_subawards,
        "subaward_to_delete": broker_subawards[2],
    }

    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(f"TRUNCATE subaward")
        cursor.execute(f"SELECT COUNT(*) FROM subaward")
        assert cursor.fetchall()[0][0] == 0


@pytest.fixture
def create_new_subaward(initial_award_and_subaward_data):
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        subaward = {
            "unique_award_key": initial_award_and_subaward_data[
                "manipulated_award_with_subawards"
            ].generated_unique_award_id,
            "subaward_amount": 300.0,
            "id": 111,
            "subaward_number": "111",
            "sub_action_date": "2024-01-01",
        }
        cursor.execute(
            """
                INSERT INTO subaward (unique_award_key, subaward_amount, id, subaward_number, sub_action_date)
                VALUES (%(unique_award_key)s, %(subaward_amount)s, %(id)s, %(subaward_number)s, %(sub_action_date)s)
            """,
            subaward,
        )
    return initial_award_and_subaward_data


@pytest.fixture
def delete_one_subaward(initial_award_and_subaward_data):
    subaward_to_delete = initial_award_and_subaward_data["subaward_to_delete"]
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
                DELETE FROM subaward WHERE subaward_number = %(subaward_number)s
            """,
            subaward_to_delete,
        )
    return initial_award_and_subaward_data


@pytest.fixture
def delete_all_subawards(initial_award_and_subaward_data):
    subaward_to_delete = initial_award_and_subaward_data["subaward_to_delete"]
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
                DELETE FROM subaward WHERE unique_award_key = %(unique_award_key)s
            """,
            subaward_to_delete,
        )
    return initial_award_and_subaward_data


@pytest.mark.django_db(transaction=True, databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
@pytest.mark.parametrize(
    "fixture_name,expected_manipulated_value",
    [
        ("initial_award_and_subaward_data", {"id": 3, "subaward_count": 2, "total_subaward_amount": Decimal(140)}),
        ("create_new_subaward", {"id": 3, "subaward_count": 3, "total_subaward_amount": Decimal(440)}),
        ("delete_one_subaward", {"id": 3, "subaward_count": 1, "total_subaward_amount": Decimal(100)}),
        ("delete_all_subawards", {"id": 3, "subaward_count": 0, "total_subaward_amount": None}),
    ],
)
def test_update_award_with_subaward(
    fixture_name,
    expected_manipulated_value,
    spark,
    s3_unittest_data_bucket,
    hive_unittest_metastore_db,
    broker_db_setup,
    request,
):
    fixture_value = request.getfixturevalue(fixture_name)

    tables_to_create = [
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "transaction_normalized",
        "transaction_fpds",
        "transaction_fabs",
        "transaction_current_cd_lookup",
        "zips",
    ]
    create_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_create)
    tables_to_load = [
        "awards",
        "subaward",
        "subaward_search",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)

    base_award_dataframe = spark.sql("SELECT * FROM int.awards")

    call_command("update_delta_award_with_subaward_counts")

    expected_static_values = [
        {
            "id": award.award_id,
            "subaward_count": award.subaward_count,
            "total_subaward_amount": Decimal(award.total_subaward_amount) if award.total_subaward_amount else None,
        }
        for award in [fixture_value["static_award_no_subawards"], fixture_value["static_award_with_subawards"]]
    ]
    actual_static_values = [
        award_row.asDict()
        for award_row in base_award_dataframe.select(list(expected_static_values[0].keys()))
        .filter(base_award_dataframe.id.isin([award["id"] for award in expected_static_values]))
        .orderBy(base_award_dataframe.id)
        .collect()
    ]

    assert actual_static_values == expected_static_values

    actual_manipulated_value = (
        base_award_dataframe.select(list(expected_manipulated_value.keys()))
        .filter(base_award_dataframe.id == expected_manipulated_value["id"])
        .collect()
    )
    assert actual_manipulated_value[0].asDict() == expected_manipulated_value
