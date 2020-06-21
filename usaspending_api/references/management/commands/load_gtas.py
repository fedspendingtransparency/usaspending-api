import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import GTASTotalObligation

logger = logging.getLogger("console")

DERIVED_COLUMNS = {"obligations_incurred_total_cpe": [2190]}


class Command(BaseCommand):
    help = "Update GTAS aggregations used as domain data"

    @transaction.atomic()
    def handle(self, *args, **options):
        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

        print(self.generate_sql())
        logger.info("Running TOTAL_OBLIGATION_SQL")
        broker_cursor.execute(self.generate_sql())

        logger.info("Getting total obligation values from cursor")
        total_obligation_values = dictfetchall(broker_cursor)

        logger.info("Deleting all existing GTAS total obligation records in website")
        GTASTotalObligation.objects.all().delete()

        logger.info("Inserting GTAS total obligations records into website")
        total_obligation_objs = [GTASTotalObligation(**values) for values in total_obligation_values]
        GTASTotalObligation.objects.bulk_create(total_obligation_objs)

        logger.info("GTAS loader finished successfully!")

    def generate_sql(self):
        inner_statements = "\n".join(
            [
                """(
      SELECT
      SUM(sf.amount) 
      FROM sf_133 sf
      WHERE sf.fiscal_year = outer_table.fiscal_year
      AND sf.period = outer_table.period
      AND (sf.disaster_emergency_fund_code is null AND outer_table.disaster_emergency_fund_code is null OR sf.disaster_emergency_fund_code = outer_table.disaster_emergency_fund_code)
      AND line in (2190)
    ) AS obligations_incurred_total_cpe,"""
                for elem in DERIVED_COLUMNS
            ]
        )

        return f"""
SELECT
    fiscal_year,
    period as fiscal_period,
    {inner_statements}
    outer_table.disaster_emergency_fund_code
FROM sf_133 outer_table
GROUP BY fiscal_year, fiscal_period, disaster_emergency_fund_code
ORDER BY fiscal_year, fiscal_period;
"""
