import logging
import asyncio
import sqlparse

from django.db import connection
from django.core.management.base import BaseCommand
from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

logger = logging.getLogger("script")

RECREATE_TABLE_SQL = """
DROP {old_object_type} IF EXISTS universal_transaction_matview_temp;
CREATE TABLE universal_transaction_matview_temp AS SELECT * from universal_transaction_matview_0 WITH NO DATA;
"""

INSERT_INTO_TABLE_SQL = """
INSERT INTO universal_transaction_matview_temp SELECT * FROM universal_transaction_matview_{current_chunk}
"""

TABLE_INDEX_SQL = """
CREATE UNIQUE INDEX idx_ut_transaction_id_temp ON universal_transaction_matview_temp USING BTREE(transaction_id ASC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_ut_action_date_temp ON universal_transaction_matview_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_last_modified_date_temp ON universal_transaction_matview_temp USING BTREE(last_modified_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_ut_fiscal_year_temp ON universal_transaction_matview_temp USING BTREE(fiscal_year DESC NULLS LAST) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_type_temp ON universal_transaction_matview_temp USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_award_id_temp ON universal_transaction_matview_temp USING BTREE(award_id) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_pop_zip5_temp ON universal_transaction_matview_temp USING BTREE(pop_zip5) WITH (fillfactor = 97) WHERE pop_zip5 IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97) WHERE parent_recipient_unique_id IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_simple_pop_geolocation_temp ON universal_transaction_matview_temp USING BTREE(pop_state_code, action_date) WITH (fillfactor = 97) WHERE pop_country_code = 'USA' AND pop_state_code IS NOT NULL AND action_date >= '2007-10-01';
CREATE INDEX idx_ut_recipient_hash_temp ON universal_transaction_matview_temp USING BTREE(recipient_hash) WITH (fillfactor = 97) WHERE action_date >= '2007-10-01';
CREATE INDEX idx_ut_action_date_pre2008_temp ON universal_transaction_matview_temp USING BTREE(action_date) WITH (fillfactor = 97) WHERE action_date < '2007-10-01';
"""

SWAP_TABLES_SQL = """
DROP {old_object_type} IF EXISTS universal_transaction_matview_old;

ALTER TABLE IF EXISTS universal_transaction_matview RENAME TO universal_transaction_matview_old;
ALTER MATERIALIZED VIEW IF EXISTS universal_transaction_matview RENAME TO universal_transaction_matview_old;

ALTER INDEX IF EXISTS idx_ut_transaction_id RENAME TO idx_ut_transaction_id_old;
ALTER INDEX IF EXISTS idx_ut_action_date RENAME TO idx_ut_action_date_old;
ALTER INDEX IF EXISTS idx_ut_last_modified_date RENAME TO idx_ut_last_modified_date_old;
ALTER INDEX IF EXISTS idx_ut_fiscal_year RENAME TO idx_ut_fiscal_year_old;
ALTER INDEX IF EXISTS idx_ut_type RENAME TO idx_ut_type_old;
ALTER INDEX IF EXISTS idx_ut_award_id RENAME TO idx_ut_award_id_old;
ALTER INDEX IF EXISTS idx_ut_pop_zip5 RENAME TO idx_ut_pop_zip5_old;
ALTER INDEX IF EXISTS idx_ut_recipient_unique_id RENAME TO idx_ut_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_ut_parent_recipient_unique_id RENAME TO idx_ut_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_ut_simple_pop_geolocation RENAME TO idx_ut_simple_pop_geolocation_old;
ALTER INDEX IF EXISTS idx_ut_recipient_hash RENAME TO idx_ut_recipient_hash_old;
ALTER INDEX IF EXISTS idx_ut_action_date_pre2008 RENAME TO idx_ut_action_date_pre2008_old;

ALTER TABLE universal_transaction_matview_temp RENAME TO universal_transaction_matview;

ALTER INDEX idx_ut_transaction_id_temp RENAME TO idx_ut_transaction_id;
ALTER INDEX idx_ut_action_date_temp RENAME TO idx_ut_action_date;
ALTER INDEX idx_ut_last_modified_date_temp RENAME TO idx_ut_last_modified_date;
ALTER INDEX idx_ut_fiscal_year_temp RENAME TO idx_ut_fiscal_year;
ALTER INDEX idx_ut_type_temp RENAME TO idx_ut_type;
ALTER INDEX idx_ut_award_id_temp RENAME TO idx_ut_award_id;
ALTER INDEX idx_ut_pop_zip5_temp RENAME TO idx_ut_pop_zip5;
ALTER INDEX idx_ut_recipient_unique_id_temp RENAME TO idx_ut_recipient_unique_id;
ALTER INDEX idx_ut_parent_recipient_unique_id_temp RENAME TO idx_ut_parent_recipient_unique_id;
ALTER INDEX idx_ut_simple_pop_geolocation_temp RENAME TO idx_ut_simple_pop_geolocation;
ALTER INDEX idx_ut_recipient_hash_temp RENAME TO idx_ut_recipient_hash;
ALTER INDEX idx_ut_action_date_pre2008_temp RENAME TO idx_ut_action_date_pre2008;
"""

ANALYZE_TABLE_SQL = """
ANALYZE VERBOSE universal_transaction_matview;
"""

TABLE_PERMISSION_SQL = """
GRANT SELECT ON universal_transaction_matview TO readonly;
"""


class Command(BaseCommand):

    help = """
    This script combines the chunked Universal Transaction Matviews and
    combines them into a single table.
    """

    def add_arguments(self, parser):
        parser.add_argument("--chunk-count", default=10, help="Number of chunked matviews to read from", type=int)
        parser.add_argument(
            "--analyze", action="store_true", help="Indicates whether table should be analyzed"
        )
        parser.add_argument(
            "--old-object-type", default="TABLE", help="Indicates whether the old version of the table is a Matview"
        )

    def handle(self, *args, **options):
        chunk_count = options["chunk_count"]

        logger.info(f"Chunk Count: {chunk_count}")

        old_object_type = options["old_object_type"]

        with Timer("Recreating table"):
            self.recreate_matview(old_object_type)

        with Timer("Inserting data into table"):
            self.insert_matview_data(chunk_count)

        with Timer("Creating table indexes"):
            self.create_indexes()

        with Timer("Swapping Tables/Indexes"):
            self.swap_matviews(old_object_type)

        if options["analyze"]:
            with Timer("Analyzing Table"):
                self.analyze_matview()

        with Timer("Granting Table Permissions"):
            self.grant_matview_permissions()

    def recreate_matview(self, old_object_type):
        with connection.cursor() as cursor:
            cursor.execute(RECREATE_TABLE_SQL.format(old_object_type=old_object_type))

    def insert_matview_data(self, chunk_count):
        loop = asyncio.new_event_loop()
        tasks = []
        for current_chunk in range(0, chunk_count):
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        INSERT_INTO_TABLE_SQL.format(current_chunk=current_chunk),
                        wrapper=Timer(f"Insert into table {current_chunk}"),
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
                asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(f"Creating Index {i}"),), loop=loop,)
            )
            i += 1

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    def swap_matviews(self, old_object_type):
        with connection.cursor() as cursor:
            cursor.execute(SWAP_TABLES_SQL.format(old_object_type=old_object_type))

    def analyze_matview(self):
        with connection.cursor() as cursor:
            cursor.execute(ANALYZE_TABLE_SQL)

    def grant_matview_permissions(self):
        with connection.cursor() as cursor:
            cursor.execute(TABLE_PERMISSION_SQL)
