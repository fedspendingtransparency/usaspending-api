import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.common.etl import mixins
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import GTASSF133Balances

logger = logging.getLogger("console")

DERIVED_COLUMNS = {
    "obligations_incurred_total_cpe": [2190],
    "budget_authority_appropriation_amount_cpe": [1160, 1180, 1260, 1280],
    "other_budgetary_resources_amount_cpe": [1340, 1440, 1540, 1640, 1750, 1850],
    "unobligated_balance_cpe": [2490],
}
INVERTED_DERIVED_COLUMNS = {
    "gross_outlay_amount_by_tas_cpe": [3020],
}


class Command(mixins.ETLMixin, BaseCommand):
    help = "Update GTAS aggregations used as domain data"

    @transaction.atomic()
    def handle(self, *args, **options):
        logger.info("Creating broker cursor")
        broker_cursor = connections["data_broker"].cursor()

        logger.info("Running TOTAL_OBLIGATION_SQL")
        broker_cursor.execute(self.broker_fetch_sql())

        logger.info("Getting total obligation values from cursor")
        total_obligation_values = dictfetchall(broker_cursor)

        logger.info("Deleting all existing GTAS total obligation records in website")
        GTASSF133Balances.objects.all().delete()

        logger.info("Inserting GTAS total obligations records into website")
        total_obligation_objs = [GTASSF133Balances(**values) for values in total_obligation_values]
        GTASSF133Balances.objects.bulk_create(total_obligation_objs)

        self._execute_dml_sql(self.tas_fk_sql(), "Populating TAS foreign keys")

        logger.info("GTAS loader finished successfully!")

    def broker_fetch_sql(self):
        return f"""
            SELECT
                fiscal_year,
                period as fiscal_period,
                {self.column_statements()}
                disaster_emergency_fund_code,
                CONCAT(
                    CASE WHEN sf.allocation_transfer_agency is not null THEN CONCAT(sf.allocation_transfer_agency, '-') ELSE null END,
                    sf.agency_identifier, '-',
                    CASE WHEN sf.beginning_period_of_availa is not null THEN CONCAT(sf.beginning_period_of_availa, '/', sf.ending_period_of_availabil) ELSE sf.availability_type_code END,
                    '-', sf.main_account_code, '-', sf.sub_account_code)
                as tas_rendering_label
            FROM
                sf_133 sf
            GROUP BY
                fiscal_year,
                fiscal_period,
                disaster_emergency_fund_code,
                tas_rendering_label
            ORDER BY
                fiscal_year,
                fiscal_period;
        """

    def column_statements(self):
        return "\n".join(
            [
                f"""COALESCE(SUM(CASE WHEN line IN ({','.join([str(elem) for elem in val])}) THEN sf.amount ELSE 0 END), 0.0) AS {key},"""
                for key, val in DERIVED_COLUMNS.items()
            ]
        ) + "\n".join(
            [
                f"""COALESCE(SUM(CASE WHEN line IN ({','.join([str(elem) for elem in val])}) THEN sf.amount * -1 ELSE 0 END), 0.0) AS {key},"""
                for key, val in INVERTED_DERIVED_COLUMNS.items()
            ]
        )

    def tas_fk_sql(self):
        return """UPDATE gtas_sf133_balances
                    SET treasury_account_identifier = tas.treasury_account_identifier
                    FROM treasury_appropriation_account tas
                    WHERE tas.tas_rendering_label = gtas_sf133_balances.tas_rendering_label;"""
