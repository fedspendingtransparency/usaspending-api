"""
For awards with no agency, assigns according to subtier in broker tables.

Idempotent, and only assigns in unambiguous cases (subtier code uniquely matches)
"""

import logging
import time

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("script")

BATCH_SIZE = 10000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("--batch", type=int, default=BATCH_SIZE, help="ID range to update per query")

    def _run_updaters(self, curs, table, updaters, options, start_time):
        for descrip, base_qry in updaters:
            batches = self.find_batches(curs=curs, table=table, options=options)
            for floor, ceiling in batches:
                qry = base_qry.format(floor=floor, ceiling=ceiling)
                curs.execute(qry)
                elapsed = time.time() - start_time
                logger.info("{}: ID {} to {}, {} s".format(descrip, floor, ceiling, elapsed))

    def handle(self, *args, **options):

        start = time.time()
        with connection.cursor() as curs:
            # create matview
            logger.info("Creating matview")
            curs.execute(self.MATVIEW_CREATE)
            logger.info("Time to create matview: {}".format(time.time() - start))

            # run the queries
            self._run_updaters(
                curs=curs, table="awards", updaters=self.AWARD_UPDATERS, options=options, start_time=start
            )
            self._run_updaters(
                curs=curs,
                table="transaction_normalized",
                updaters=self.TRANSACTION_NORMALIZED_UPDATERS,
                options=options,
                start_time=start,
            )

            # drop the matview
            logger.info("Dropping matview")
            curs.execute(self.MATVIEW_DELETE)

    def find_batches(self, curs, table, options):
        batch = options["batch"]
        curs.execute(self.BOUNDARY_FINDER.format(table))
        (lowest, highest) = curs.fetchone()
        floor = (lowest // batch) * batch
        while floor <= highest:
            yield (floor, floor + batch)
            floor += batch

    MATVIEW_CREATE = """
        DROP MATERIALIZED VIEW IF EXISTS full_agency_data;

        CREATE MATERIALIZED VIEW full_agency_data AS
            SELECT a.id as id,
                st.subtier_code as subtier_code
            FROM agency as a
            JOIN subtier_agency as st
                ON a.subtier_agency_id = st.subtier_agency_id;

        CREATE INDEX subtier_code_idx ON full_agency_data (subtier_code);
        CREATE INDEX id_idx ON full_agency_data (id);
        CREATE INDEX id_desc_idx ON full_agency_data (id DESC);"""

    MATVIEW_DELETE = """
        DROP MATERIALIZED VIEW full_agency_data;"""

    BOUNDARY_FINDER = """
        SELECT MIN(id) AS floor, MAX(id) AS ceiling
        FROM   {}
        WHERE  awarding_agency_id IS NULL
        OR     funding_agency_id IS NULL"""

    # Tuples of ( description, query)
    AWARD_UPDATERS = (
        (
            "Awarding agency for FPDS",
            """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    vw_transaction_fpds t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    vw_awards aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.awarding_sub_tier_agency_c)
        WHERE   aw.awarding_agency_id IS NULL
        AND     aw.award_id >= {floor}
        AND     aw.award_id < {ceiling}
        GROUP BY aw.award_id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE award_search
    SET    awarding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = award_search.award_id
    """,
        ),
        (
            "Funding agency for FPDS",
            """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    vw_transaction_fpds t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    VW_awards aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.funding_sub_tier_agency_co)
        WHERE   aw.funding_agency_id IS NULL
        AND     aw.id >= {floor}
        AND     aw.id < {ceiling}
        GROUP BY aw.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE award_search
    SET    funding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = award_search.award_id
    """,
        ),
        (
            "Awarding agency for FABS",
            """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    transaction_fabs t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    award_search aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.awarding_sub_tier_agency_c)
        WHERE   aw.awarding_agency_id IS NULL
        AND     aw.id >= {floor}
        AND     aw.id < {ceiling}
        GROUP BY aw.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE award_search
    SET    awarding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = award_search.award_id
    """,
        ),
        (
            "Funding agency for FABS",
            """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    transaction_fabs t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    award_search aw ON (tn.award_id = aw.award_id AND aw.latest_transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.funding_sub_tier_agency_co)
        WHERE   aw.funding_agency_id IS NULL
        AND     aw.award_id >= {floor}
        AND     aw.award_id < {ceiling}
        GROUP BY aw.award_id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE award_search
    SET    funding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = award_search.award_id
    """,
        ),
    )

    TRANSACTION_SEARCH_UPDATERS = (
        (
            "Awarding agency in transaction_normalized for FPDS",
            """
    WITH    match_by_subtier AS (
        SELECT  tn.id AS transaction_id,
                MAX(a.id) AS agency_id
        FROM    vw_transaction_fpds t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.awarding_sub_tier_agency_c)
        WHERE   tn.awarding_agency_id IS NULL
        AND     tn.id >= {floor}
        AND     tn.id < {ceiling}
        GROUP BY tn.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE transaction_search
    SET    awarding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.transaction_id = transaction_search.transaction_id
    """,
        ),
        (
            "Funding agency in transaction_normalized for FPDS",
            """
    WITH    match_by_subtier AS (
        SELECT  tn.id AS transaction_id,
                MAX(a.id) AS agency_id
        FROM    vw_transaction_fpds t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    award_search aw ON (tn.award_id = aw.award_id AND aw.latest_transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.funding_sub_tier_agency_co)
        WHERE   tn.funding_agency_id IS NULL
        AND     tn.id >= {floor}
        AND     tn.id < {ceiling}
        GROUP BY tn.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE transaction_search
    SET    funding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.transaction_id = transaction_search.transaction_id
    """,
        ),
        (
            "Awarding agency in transaction_normalized for FABS",
            """
    WITH    match_by_subtier AS (
        SELECT  tn.id AS transaction_id,
                MAX(a.id) AS agency_id
        FROM    vw_transaction_fabs t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.awarding_sub_tier_agency_c)
        WHERE   tn.awarding_agency_id IS NULL
        AND     tn.id >= {floor}
        AND     tn.id < {ceiling}
        GROUP BY tn.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE transaction_search
    SET    awarding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.transaction_id = transaction_search.transaction_id
    """,
        ),
        (
            "Funding agency in transaction_normalized for FABS",
            """
    WITH    match_by_subtier AS (
        SELECT  tn.id AS transaction_id,
                MAX(a.id) AS agency_id
        FROM    vw_transaction_fabs t
        JOIN    vw_transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    full_agency_data a ON (a.subtier_code = t.funding_sub_tier_agency_co)
        WHERE   tn.funding_agency_id IS NULL
        AND     tn.id >= {floor}
        AND     tn.id < {ceiling}
        GROUP BY tn.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE transaction_search
    SET    funding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.transaction_id = transaction_search.transaction_id
    """,
        ),
    )
