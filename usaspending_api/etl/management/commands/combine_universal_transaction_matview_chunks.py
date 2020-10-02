import logging
import asyncio
import sqlparse

from django.db import connection
from django.core.management.base import BaseCommand
from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

logger = logging.getLogger("script")

RECREATE_TABLE_SQL = """
DROP TABLE IF EXISTS universal_transaction_table;
CREATE TABLE universal_transaction_table AS TABLE universal_transaction_matview_0 WITH NO DATA;
"""

INSERT_INTO_TABLE_SQL = """
INSERT INTO universal_transaction_table SELECT * FROM universal_transaction_matview_{current_chunk}
"""

TABLE_INDEX_SQL = """
CREATE UNIQUE INDEX idx_ut_transaction_id_table ON universal_transaction_table USING BTREE(transaction_id ASC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_ut_action_date_table ON universal_transaction_table USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_last_modified_date_table ON universal_transaction_table USING BTREE(last_modified_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_ut_fiscal_year_table ON universal_transaction_table USING BTREE(fiscal_year DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_type_table ON universal_transaction_table USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_award_id_table ON universal_transaction_table USING BTREE(award_id) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_pop_zip5_table ON universal_transaction_table USING BTREE(pop_zip5) WITH (fillfactor = 97) WHERE pop_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_recipient_unique_id_table ON universal_transaction_table USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_parent_recipient_unique_id_table ON universal_transaction_table USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97) WHERE parent_recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_simple_pop_geolocation_table ON universal_transaction_table USING BTREE(pop_state_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_recipient_hash_table ON universal_transaction_table USING BTREE(recipient_hash) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_action_date_pre2008_table ON universal_transaction_table USING BTREE(action_date) WITH (fillfactor = 97) WHERE action_date < '2007-10-01';
"""


class Command(BaseCommand):

    help = """
    This script combines the chunked Universal Transaction Matviews and
    combines them into a single table.
    """

    def add_arguments(self, parser):
        parser.add_argument("--chunk-count", default=10, help="Broker submission_id to load", type=int)

    def handle(self, *args, **options):
        chunk_count = options["chunk_count"]

        logger.info("Chunk Count: {}".format(chunk_count))

        with Timer("Recreating table"):
            self.recreate_table()

        with Timer("Inserting data into table"):
            self.insert_table_data(chunk_count)

        with Timer("Creating table indexes"):
            self.create_indexes()

    def recreate_table(self):
        with connection.cursor() as cursor:
            cursor.execute(RECREATE_TABLE_SQL)

    def insert_table_data(self, chunk_count):
        loop = asyncio.new_event_loop()
        tasks = []
        for current_chunk in range(0, chunk_count):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        INSERT_INTO_TABLE_SQL.format(current_chunk=current_chunk),
                        wrapper=Timer("Insert into table {}".format(current_chunk)),
                    ),
                    loop=loop,
                )
            )

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    def create_indexes(self):
        loop = asyncio.new_event_loop()
        tasks = []
        i = 0
        for sql in sqlparse.split(TABLE_INDEX_SQL):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        sql,
                        wrapper=Timer("Creating Index {}".format(i)),
                    ),
                    loop=loop,
                )
            )
            i += 1

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
