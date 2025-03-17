import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.common.etl.postgres import mixins
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import GTASSF133Balances

logger = logging.getLogger("script")

DERIVED_COLUMNS = {
    "anticipated_prior_year_obligation_recoveries": [1033],
    "adjustments_to_unobligated_balance_brought_forward_fyb": [1020],
    "borrowing_authority_amount": [1340, 1440],
    "budget_authority_appropriation_amount_cpe": [1160, 1180, 1260, 1280],
    "budget_authority_unobligated_balance_brought_forward_cpe": [1000],
    "contract_authority_amount": [1540, 1640],
    "deobligations_or_recoveries_or_refunds_from_prior_year_cpe": [1021, 1033],
    "obligations_incurred": [2190],
    "obligations_incurred_total_cpe": [2190],
    "other_budgetary_resources_amount_cpe": [1340, 1440, 1540, 1640, 1750, 1850],
    "prior_year_paid_obligation_recoveries": [1061],
    "spending_authority_from_offsetting_collections_amount": [1750, 1850],
    "total_budgetary_resources_cpe": [1910],
    "unobligated_balance_cpe": [2490],
    "status_of_budgetary_resources_total_cpe": [2500],
}

INVERTED_DERIVED_COLUMNS = {
    "gross_outlay_amount_by_tas_cpe": [3020],
}

# The before_year list of items is applied to records before the change_year fiscal year.
# The year_and_after list is applied to the change_year and subsequent fiscal years.
DERIVED_COLUMNS_DYNAMIC = {
    "adjustments_to_unobligated_balance_brought_forward_cpe": {
        "before_year": list(range(1010, 1043)),
        "year_and_after": list(range(1010, 1066)),
        "change_year": 2021,
    }
}
GTAS_TABLE = GTASSF133Balances.objects.model._meta.db_table


class Command(mixins.ETLMixin, BaseCommand):
    help = "Drop and recreate all GTAS reference data"

    def handle(self, *args, **options):
        logger.info("Starting ETL script")
        self.process_data()
        logger.info("GTAS ETL finished successfully!")

    @transaction.atomic()
    def process_data(self):
        broker_cursor = connections[settings.DATA_BROKER_DB_ALIAS].cursor()

        logger.info("Extracting data from Broker")
        broker_cursor.execute(self.broker_fetch_sql)
        total_obligation_values = dictfetchall(broker_cursor)

        logger.info("Deleting all existing GTAS total obligation records in website")
        deletes = GTASSF133Balances.objects.all().delete()
        logger.info(f"Deleted {deletes[0]:,} records")

        logger.info("Transforming new GTAS records")
        total_obligation_objs = [GTASSF133Balances(**values) for values in total_obligation_values]

        logger.info("Loading new GTAS records into database")
        new_rec_count = len(GTASSF133Balances.objects.bulk_create(total_obligation_objs))
        logger.info(f"Loaded: {new_rec_count:,} records")

        load_rec = self._execute_dml_sql(self.tas_fk_sql, "Populating TAS foreign keys")
        logger.info(f"Set {load_rec:,} TAS FKs in GTAS table, {new_rec_count - load_rec:,} NULLs")
        delete_rec = self._execute_dml_sql(self.financing_account_sql, "Drop Financing Account TAS")
        logger.info(f"Deleted {delete_rec:,} records in GTAS table due to invalid TAS")
        logger.info("Committing transaction to database")

    @property
    def broker_fetch_sql(self):
        return f"""
            SELECT
                fiscal_year,
                period AS fiscal_period,
                {self.column_statements}
                disaster_emergency_fund_code AS disaster_emergency_fund_id,
                CONCAT(
                    CASE WHEN sf.allocation_transfer_agency is not null THEN CONCAT(sf.allocation_transfer_agency, '-') ELSE null END,
                    sf.agency_identifier, '-',
                    CASE WHEN sf.beginning_period_of_availa is not null THEN CONCAT(sf.beginning_period_of_availa, '/', sf.ending_period_of_availabil) ELSE sf.availability_type_code END,
                    '-', sf.main_account_code, '-', sf.sub_account_code)
                AS tas_rendering_label
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

    @property
    def column_statements(self):
        simple_fields = [
            f"COALESCE(SUM(CASE WHEN line IN ({','.join([str(elem) for elem in val])}) THEN sf.amount ELSE 0 END), 0.0) AS {key},"
            for key, val in DERIVED_COLUMNS.items()
        ]
        inverted_fields = [
            f"COALESCE(SUM(CASE WHEN line IN ({','.join([str(elem) for elem in val])}) THEN sf.amount * -1 ELSE 0 END), 0.0) AS {key},"
            for key, val in INVERTED_DERIVED_COLUMNS.items()
        ]
        year_specific_fields = [
            f"""COALESCE(SUM(CASE
                        WHEN line IN ({','.join([str(elem) for elem in val["before_year"]])}) AND fiscal_year < {val["change_year"]} THEN sf.amount * -1
                        WHEN line IN ({','.join([str(elem) for elem in val["year_and_after"]])}) AND fiscal_year >= {val["change_year"]} THEN sf.amount * -1
                        ELSE 0
                    END), 0.0) AS {key},"""
            for key, val in DERIVED_COLUMNS_DYNAMIC.items()
        ]
        return "\n".join(simple_fields + inverted_fields + year_specific_fields)

    @property
    def tas_fk_sql(self):
        return f"""
            UPDATE {GTAS_TABLE}
            SET treasury_account_identifier = tas.treasury_account_identifier
            FROM treasury_appropriation_account tas
            WHERE
                tas.tas_rendering_label = {GTAS_TABLE}.tas_rendering_label
                AND {GTAS_TABLE}.treasury_account_identifier IS DISTINCT FROM tas.treasury_account_identifier"""

    @property
    def financing_account_sql(self):
        return f"""DELETE FROM {GTAS_TABLE} WHERE treasury_account_identifier IS NULL"""
