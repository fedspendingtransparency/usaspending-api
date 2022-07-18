"""Automated Integration Tests for copying of table metadata

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from django.db import connection
from usaspending_api.etl.broker_etl_helpers import dictfetchall

from usaspending_api.etl.management.commands.copy_table_metadata import (
    make_read_constraints,
    make_copy_constraints,
    make_read_indexes,
    make_copy_indexes,
)


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
