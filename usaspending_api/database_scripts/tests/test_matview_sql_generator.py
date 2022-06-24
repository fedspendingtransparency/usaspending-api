import pytest

from django.db import connection
from usaspending_api.common.helpers.generic_helper import generate_matviews
from usaspending_api.database_scripts.matview_generator.chunked_matview_sql_generator import (
    make_read_constraints,
    make_read_indexes,
    make_copy_constraints,
    make_copy_indexes,
)
from usaspending_api.common.matview_manager import MATERIALIZED_VIEWS, CHUNKED_MATERIALIZED_VIEWS
from usaspending_api.etl.broker_etl_helpers import dictfetchall

ALL_MATVIEWS = {**MATERIALIZED_VIEWS, **CHUNKED_MATERIALIZED_VIEWS}


@pytest.fixture
def convert_traditional_views_to_materialized_views(transactional_db):
    """
    In conftest.py, we convert materialized views to traditional views for testing performance reasons.
    For the following test to work, we will need to undo all the traditional view stuff and generate
    actual materialized views.  Once done, we want to convert everything back to traditional views.
    """

    # Get rid of the traditional views that replace our materialized views for tests.
    with connection.cursor() as cursor:
        cursor.execute("; ".join(f"drop view if exists {v} cascade" for v in ALL_MATVIEWS))

    # Create materialized views.
    generate_matviews(materialized_views_as_traditional_views=False)

    yield

    # Great.  Test is over.  Drop all of our materialized views.
    with connection.cursor() as cursor:
        cursor.execute("; ".join(f"drop materialized view if exists {v} cascade" for v in ALL_MATVIEWS))

    # Recreate our traditional views.
    generate_matviews(materialized_views_as_traditional_views=True)


def test_matview_sql_generator(convert_traditional_views_to_materialized_views):
    """
    The goal of this test is to ensure that we can successfully create materialized views using our homegrown
    materialized view generator.  We will not test the validity of the data contained therein, just that we
    can create the materialized views without issue.
    """

    # Run through all of the materialized views and perform a simple count query.
    for matview in MATERIALIZED_VIEWS.values():
        # This will fail if the materialized view doesn't exist for whatever reason which is what we want.
        matview["model"].objects.count()


def test_make_copy_table_metadata(db):
    """
    Simply put, make a table, make a blank copy of said table, and ensure the two match in terms of metadata
    (asides from the names)
    """
    with connection.cursor() as cursor:
        # make the base tables - table y will only be used for a foreign key relationship
        table_y = "y"
        cursor.execute(
            f"""
            CREATE TABLE {table_y} (
                id INT PRIMARY KEY
            );
        """
        )
        table_x = "x"
        cursor.execute(
            f"""
            CREATE TABLE {table_x} (
                id INT PRIMARY KEY,
                column_1 INT NOT NULL,
                column_2 TEXT UNIQUE,
                foreign_id INT NOT NULL,
                FOREIGN KEY (foreign_id) REFERENCES {table_y} (id)
            );
        """
        )

        def _gather_table_metadata(table, cursor, dup_table_name=None, drop_foreign=False):
            column_sql = f"""
                SELECT
                   table_name,
                   column_name,
                   data_type
                FROM
                   information_schema.columns
                WHERE
                   table_name = '{table}';
            """
            cursor.execute(column_sql)
            columns = dictfetchall(cursor)

            cursor.execute(";".join(make_read_indexes(table)))
            indexes = dictfetchall(cursor)

            cursor.execute(";".join(make_read_constraints(table)))
            constraints = dictfetchall(cursor)

            if drop_foreign:
                constraints = [
                    constraint for constraint in constraints if "FOREIGN" not in constraint["pg_get_constraintdef"]
                ]
            # when comparing the two, we need to ignore the new table name and updated names
            # so we're updating the base table to match the expected results
            if dup_table_name:
                for column in columns:
                    column["table_name"] = dup_table_name
                for ix_dict in indexes:
                    old_index = ix_dict["indexname"]
                    new_index = f"{old_index}_temp"
                    ix_dict["indexname"] = new_index
                    ix_dict["indexdef"] = ix_dict["indexdef"].replace(old_index, new_index)
                    ix_dict["indexdef"] = ix_dict["indexdef"].replace(f"public.{table}", f"public.{dup_table_name}")
                for cst_dict in constraints:
                    cst_dict["conname"] = "{}_temp".format(cst_dict["conname"])

            return {
                "columns": sorted(columns, key=lambda column: column["column_name"]),
                "constraints": sorted(constraints, key=lambda constraint: constraint["conname"]),
                "indexes": sorted(indexes, key=lambda index: index["indexname"]),
            }

        # make a copy with foreign keys
        dup_table_x_foreign = "x_temp_foreign_keys"
        cursor.execute(f"CREATE TABLE {dup_table_x_foreign} (LIKE {table_x}" f" INCLUDING DEFAULTS INCLUDING IDENTITY)")
        cursor.execute("; ".join(make_copy_constraints(cursor, table_x, dup_table_x_foreign, drop_foreign_keys=False)))
        cursor.execute("; ".join(make_copy_indexes(cursor, table_x, dup_table_x_foreign)))

        # gather the metadata results and clean that temp table
        dup_foreign_metadata = _gather_table_metadata(dup_table_x_foreign, cursor)
        cursor.execute(f"DROP TABLE {dup_table_x_foreign}")
        assert (
            _gather_table_metadata(table_x, cursor, dup_table_name=dup_table_x_foreign, drop_foreign=False)
            == dup_foreign_metadata
        )

        # make a copy without foreign keys
        dup_table_x_not_foreign = "x_temp_drop_foreign_keys"
        cursor.execute(
            f"CREATE TABLE {dup_table_x_not_foreign} (LIKE {table_x}" f" INCLUDING DEFAULTS INCLUDING IDENTITY)"
        )
        cursor.execute(
            "; ".join(make_copy_constraints(cursor, table_x, dup_table_x_not_foreign, drop_foreign_keys=True))
        )
        cursor.execute("; ".join(make_copy_indexes(cursor, table_x, dup_table_x_not_foreign)))

        # gather the metadata results and clean that temp table
        dup_not_foreign_metadata = _gather_table_metadata(dup_table_x_not_foreign, cursor)
        cursor.execute(f"DROP TABLE {dup_table_x_not_foreign}")
        assert (
            _gather_table_metadata(table_x, cursor, dup_table_name=dup_table_x_not_foreign, drop_foreign=True)
            == dup_not_foreign_metadata
        )

        # Cleanup
        cursor.execute(f"DROP TABLE {table_x}")
        cursor.execute(f"DROP TABLE {table_y}")
