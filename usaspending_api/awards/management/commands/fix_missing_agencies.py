"""
For awards with no agency, assigns according to subtier in broker tables.

Idempotent, and only assigns in unambiguous cases (subtier code uniquely matches)
"""

import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

BATCH_SIZE = 10000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument('--batch', type=int, default=BATCH_SIZE, help="ID range to update per query")

    def handle(self, *args, **options):

        start = time.time()
        with connection.cursor() as curs:
            for (descrip, base_qry) in self.UPDATERS:
                batches = self.find_batches(curs=curs, table='awards', options=options)
                for (floor, ceiling) in batches:
                    qry = base_qry.format(floor=floor, ceiling=ceiling)
                    curs.execute(qry)
                    elapsed = time.time() - start
                    logger.info('{}: ID {} to {}, {} s'.format(descrip, floor, ceiling, elapsed))

    def find_batches(self, curs, table, options):
        batch = options['batch']
        curs.execute(self.BOUNDARY_FINDER.format(table))
        (lowest, highest) = curs.fetchone()
        floor = (lowest // batch) * batch
        while floor <= highest:
            yield (floor, floor + batch)
            floor += batch

    BOUNDARY_FINDER = """
        SELECT MIN(id) AS floor, MAX(id) AS ceiling
        FROM   {}"""

    # Tuples of ( description, query)
    UPDATERS = (
        ('Awarding agency for FPDS', """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    transaction_fpds t
        JOIN    transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    awards aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    subtier_agency st ON (st.subtier_code = t.awarding_sub_tier_agency_c)
        JOIN    agency a ON (a.subtier_agency_id = st.subtier_agency_id)
        WHERE   aw.awarding_agency_id IS NULL
        AND     aw.id >= {floor}
        AND     aw.id < {ceiling}
        GROUP BY aw.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE awards
    SET    awarding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = awards.id
    """),
        ('Funding agency for FPDS', """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    transaction_fpds t
        JOIN    transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    awards aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    subtier_agency st ON (st.subtier_code = t.funding_sub_tier_agency_co)
        JOIN    agency a ON (a.subtier_agency_id = st.subtier_agency_id)
        WHERE   aw.funding_agency_id IS NULL
        GROUP BY aw.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE awards
    SET    funding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = awards.id
    AND    awards.id >= {floor}
    AND    awards.id < {ceiling};
    """),
        ('Awarding agency for FABS', """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    transaction_fabs t
        JOIN    transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    awards aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    subtier_agency st ON (st.subtier_code = t.awarding_sub_tier_agency_c)
        JOIN    agency a ON (a.subtier_agency_id = st.subtier_agency_id)
        WHERE   aw.awarding_agency_id IS NULL
        GROUP BY aw.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE awards
    SET    awarding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = awards.id
    AND    awards.id >= {floor}
    AND    awards.id < {ceiling};
    """),
        ('Funding agency for FABS', """
    WITH    match_by_subtier AS (
        SELECT  aw.id AS award_id,
                MAX(a.id) AS agency_id
        FROM    transaction_fabs t
        JOIN    transaction_normalized tn ON (t.transaction_id = tn.id)
        JOIN    awards aw ON (tn.award_id = aw.id AND aw.latest_transaction_id = tn.id)
        JOIN    subtier_agency st ON (st.subtier_code = t.funding_sub_tier_agency_co)
        JOIN    agency a ON (a.subtier_agency_id = st.subtier_agency_id)
        WHERE   aw.funding_agency_id IS NULL
        GROUP BY aw.id
        HAVING  COUNT(DISTINCT a.id) = 1
    )
    UPDATE awards
    SET    funding_agency_id = match_by_subtier.agency_id
    FROM   match_by_subtier
    WHERE  match_by_subtier.award_id = awards.id
    AND    awards.id >= {floor}
    AND    awards.id < {ceiling};
    """),
    )
