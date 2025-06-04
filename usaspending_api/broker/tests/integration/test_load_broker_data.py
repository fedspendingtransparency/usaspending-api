from datetime import datetime, timezone

import pytest
from django.core.management import call_command
from django.db import connections

from usaspending_api.settings import DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS


@pytest.fixture(scope="module")
def now():
    return datetime.now().replace(tzinfo=timezone.utc)


@pytest.fixture
def broker_zips_grouped(now):
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE zips_grouped_test (
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                zips_grouped_id INT NOT NULL,
                zip5 text,
                state_abbreviation text,
                county_number text,
                congressional_district_no text
            );
        """
        )
        cursor.execute(
            f"""
            INSERT INTO zips_grouped_test (created_at, updated_at, zips_grouped_id, zip5, state_abbreviation, county_number, congressional_district_no)
            VALUES
                ('{now}', '{now}', 1, '00001', 'KS', '01', '01' ),
                ('{now}', '{now}', 2, '00002', 'KS', '02', '02' )
        """
        )
    yield
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute("DROP TABLE zips_grouped_test;")


@pytest.mark.django_db(databases=[DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_load_broker_table(broker_zips_grouped, now):
    call_command(
        "load_broker_table",
        "--table-name=zips_grouped_test",
        "--schema-name=public",
        "--usaspending-table-name=zips_grouped",
    )
    with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM public.zips_grouped
            ORDER BY zips_grouped_id
        """
        )
        rows = cursor.fetchall()
        assert rows == [(now, now, 1, "00001", "KS", "01", "01"), (now, now, 2, "00002", "KS", "02", "02")]


@pytest.mark.django_db(databases=[DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_load_broker_table_exists_failure(broker_zips_grouped):
    with pytest.raises(ValueError):
        call_command(
            "load_broker_table",
            "--table-name=zips_grouped_test",
            "--schema-name=public",
            "--usaspending-table-name=zips_grouped_test",
        )
