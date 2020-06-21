import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import GTASTotalObligation

logger = logging.getLogger("console")

DERIVED_COLUMNS = {
    "obligations_incurred_total_cpe": [2190],
    "budget_authority_appropriation_amount_cpe": [1160, 1180, 1260, 1280],
    "other_budgetary_resources_amount_cpe": [1340, 1440, 1540, 1640, 1750, 1850],
    "gross_outlay_amount_by_tas_cpe": [3020],
    "unobligated_balance_cpe": [2490],
}


class Command(BaseCommand):
    help = "Update GTAS aggregations used as domain data"

    @transaction.atomic()
    def handle(self, *args, **options):
        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

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
                f"""(
      SELECT
      COALESCE(SUM(sf.amount), 0.0)
      FROM sf_133 sf
      WHERE sf.fiscal_year = outer_table.fiscal_year
      AND sf.period = outer_table.period
      AND (sf.disaster_emergency_fund_code is null AND outer_table.disaster_emergency_fund_code is null OR sf.disaster_emergency_fund_code = outer_table.disaster_emergency_fund_code)
      AND line in ({','.join([str(elem) for elem in val])})
    ) AS {key},"""
                for key, val in DERIVED_COLUMNS.items()
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
