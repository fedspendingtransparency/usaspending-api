import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import GTASTotalObligation

logger = logging.getLogger("console")

TOTAL_OBLIGATION_SQL = """
SELECT
    fiscal_year,
    CASE
        WHEN sf_133.period = 3 THEN 1
        WHEN sf_133.period = 6 THEN 2
        WHEN sf_133.period = 9 THEN 3
        WHEN sf_133.period = 12 THEN 4
    END AS fiscal_quarter,
    SUM(amount) AS total_obligation
FROM sf_133
WHERE
    line = 2190
    AND
    period in (3, 6, 9, 12)
GROUP BY fiscal_year, fiscal_quarter
ORDER BY fiscal_year, fiscal_quarter;
"""


class Command(BaseCommand):
    help = "Update GTAS aggregations used as domain data"

    @transaction.atomic()
    def handle(self, *args, **options):
        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

        logger.info("Running TOTAL_OBLIGATION_SQL")
        broker_cursor.execute(TOTAL_OBLIGATION_SQL)

        logger.info("Getting total obligation values from cursor")
        total_obligation_values = dictfetchall(broker_cursor)

        logger.info("Deleting all existing GTAS total obligation records in website")
        GTASTotalObligation.objects.all().delete()

        logger.info("Inserting GTAS total obligations records into website")
        total_obligation_objs = [GTASTotalObligation(**values) for values in total_obligation_values]
        GTASTotalObligation.objects.bulk_create(total_obligation_objs)

        logger.info("GTAS loader finished successfully!")
