
from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings

from usaspending_api.etl.broker_etl_helpers import setup_broker_fdw


def schema_and_table(raw_table_name):
    if raw_table_name:
        raw_table_name = raw_table_name.lower()
        if '.' in raw_table_name:
            return raw_table_name.split('.', 1)
        else:
            return 'public', raw_table_name
    else:
        return (None, None)


class Command(BaseCommand):
    """
    Developer utility to help write raw SQL insert statements.  Generates a
    specific format useful for writing ETL inserts like those in
    submission_etl.sql.

    Arguments:

        - Destination table name (Postgres-side, not the model name)
        - Source table name (Postgres-side, not the model name)

    """

    help = "Write skeleton for raw insert statement into given table."

    def add_arguments(self, parser):
        parser.add_argument('table_name', help='(schema name.)table name to generate insert for')
        parser.add_argument('source_name', help='(schema name.)table name of primary table to get data from')

    COLUMN_QRY = """SELECT column_name, data_type
        FROM   information_schema.columns
        WHERE  table_name = %s
        AND    table_schema = %s
        ORDER BY ordinal_position"""

    INSERT = """

CREATE UNIQUE INDEX ON {dest_schema}.{dest_table} (
    {unique_list}
);


INSERT INTO {dest_schema}.{dest_table} (
    {column_list}
    )
SELECT
{select_list}
FROM {source_schema}.{source_table}
GROUP BY {group_by_list}
ON CONFLICT (
    {unique_list}
)
DO UPDATE
SET _procurement_ids = EXCLUDED._procurement_ids;
        """

    NULL_SENTINELS = {'numeric': '-1', 'timestamp with time zone': "'1900-01-01'::DATE"}

    def handle(self, *args, **options):

        setup_broker_fdw()

        (dest_schema, dest_table) = schema_and_table(options['table_name'])
        (source_schema, source_table) = schema_and_table(options['source_name'])

        with connection.cursor() as cursor:
            cursor.execute(self.COLUMN_QRY, [dest_table, dest_schema, ])
            dest_columns = cursor.fetchall()
            dest_column_names = [c[0] for c in dest_columns]
            # column_names = [row[0] for row in cursor.fetchall()]
            if source_table:
                cursor.execute(self.COLUMN_QRY, [source_table, source_schema, ])
                source_columns = {c[0]: c[1] for c in cursor.fetchall()}
            else:
                source_columns = []
                source_columns_names = []

        column_list = ',\n    '.join(dest_column_names)
        select_list = []
        unique_list = []
        for (dest_column_name, dest_column_type) in dest_columns:
            null_sentinel = self.NULL_SENTINELS.get(dest_column_type, "''")
            unique_list.append("COALESCE({}, {})".format(dest_column_name, null_sentinel))
            src = 'NULL'.format(dest_column_type)
            type_cast = '::{}'.format(dest_column_type)
            if dest_column_name in source_columns:
                src = dest_column_name
                if dest_column_type == source_columns[dest_column_name]:
                    type_cast = ''
            elif dest_column_name in settings.LONG_TO_TERSE_LABELS:
                potential_src = settings.LONG_TO_TERSE_LABELS[dest_column_name]
                if potential_src in source_columns:
                    print('dest col {} is {} in LONG_TO_TERSE_LABELS'.format(dest_column_name, src))
                    src = potential_src
                    if dest_column_type == source_columns[src]:
                        type_cast = ''
            select_list.append('    {}{}, -- ==> {}'.format(src, type_cast, dest_column_name))
        group_by_list = ", ".join(str(i) for i in range(1, len(select_list)))
        select_list = '\n'.join(select_list)
        unique_list = ',\n    '.join(unique_list)

        print(self.INSERT.format(**locals()))
