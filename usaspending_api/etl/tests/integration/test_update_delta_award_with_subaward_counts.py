from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from django.conf import settings
from django.db import connections

from model_bakery import baker

from usaspending_api.search.models import SubawardSearch
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
            "unique_award_key": static_award_no_subawards.award_id,
            "subaward_amount": 25.0,
            "id": 123,
            "subaward_number": "123",
            "sub_action_date": "2024-01-01",
        },
        {
            "unique_award_key": manipulated_award_with_subawards.award_id,
            "subaward_amount": 100.0,
            "id": 456,
            "subaward_number": "456",
            "sub_action_date": "2024-01-01",
        },
        {
            "unique_award_key": manipulated_award_with_subawards.award_id,
            "subaward_amount": 50.0,
            "id": 789,
            "subaward_number": "789",
            "sub_action_date": "2024-01-01",
        },
    ]
    with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
        for subaward in broker_subawards:
            cursor.execute(
                """
                    INSERT INTO subaward (unique_award_key, subaward_amount)
                    VALUES (%(unique_award_key)s, %(subaward_amount)s)
                """,
                subaward,
            )

    return {
        "static_award_no_subawards": static_award_no_subawards,
        "static_award_with_subawards": static_award_with_subawards,
        "manipulated_award_with_subawards": manipulated_award_with_subawards,
        "subaward_to_delete": broker_subawards[2],
    }


@pytest.fixture
def create_new_subaward(initial_award_and_subaward_data):
    award_id = initial_award_and_subaward_data["manipulated_award_with_subawards"].award_id
    baker.make("search.SubawardSearch", award_id=award_id, subaward_amount=300.0)
    return initial_award_and_subaward_data


@pytest.fixture
def delete_one_subaward(initial_award_and_subaward_data):
    broker_subaward_id = initial_award_and_subaward_data["subaward_to_delete"]["unique_award_key"]
    SubawardSearch.objects.filter(broker_subaward_id=broker_subaward_id).delete()
    return initial_award_and_subaward_data


@pytest.fixture
def delete_all_subawards(initial_award_and_subaward_data):
    award_id = initial_award_and_subaward_data["manipulated_award_with_subawards"].award_id
    SubawardSearch.objects.filter(award_id=award_id).delete()
    return initial_award_and_subaward_data


@pytest.mark.django_db(transaction=True, databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS])
@pytest.mark.parametrize(
    "fixture_name,expected_subaward_count,expected_total_subaward_amount",
    [
        ("initial_award_and_subaward_data", 0, 0),
        ("create_new_subaward", 0, 0),
        ("delete_one_subaward", 0, 0),
        ("delete_all_subawards", 0, 0),
    ],
)
def test_update_award_with_subaward(
    fixture_name,
    expected_subaward_count,
    expected_total_subaward_amount,
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
        for award_row in spark.sql("SELECT * FROM int.awards ORDER BY id DESC")
        .select(list(expected_static_values[0].keys()))
        .filter()
        .collect()
    ]

    assert actual_static_values == expected_static_values
