import logging
from datetime import datetime, timezone

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS, IntegrityError, connections, transaction

from usaspending_api.common.operations_reporter import OpsReporter
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models.office import Office

logger = logging.getLogger("script")
Reporter = OpsReporter(iso_start_datetime=datetime.now(timezone.utc).isoformat(), job_name="load_offices.py")


class Command(BaseCommand):
    help = "Drop and recreate all office reference data"

    def handle(self, *args, **options):
        logger.info("Starting ETL script")
        try:
            self.process_data()
            logger.info("Office ETL finished successfully!")
        except IntegrityError:
            logger.warning("Unique constraint violated. Continuing with pipeline execution.")
            raise SystemExit(3)  # Raise exit code 3 for unique constraint violation

    @transaction.atomic()
    def process_data(self):
        broker_cursor = connections[settings.DATA_BROKER_DB_ALIAS].cursor()

        logger.info("Extracting data from Broker")
        broker_cursor.execute(self.broker_fetch_sql)
        office_values = dictfetchall(broker_cursor)

        logger.info("Deleting all existing office reference data")
        deletes = Office.objects.all().delete()
        logger.info(f"Deleted {deletes[0]:,} records")

        logger.info("Transforming new office records")
        total_objs = [Office(**values) for values in office_values]

        logger.info("Loading new office records into database")
        new_rec_count = len(Office.objects.bulk_create(total_objs))
        logger.info(f"Loaded: {new_rec_count:,} records")
        logger.info("Committing transaction to database")

        logger.info(f"Deleting all existing offices that are not linked to a transaction in {DEFAULT_DB_ALIAS}")
        # Identify offices that do not correspond to any transactions, using only USAS DB
        with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            cursor.execute(self.usas_unlinked_offices_sql)

    @property
    def broker_fetch_sql(self):
        return """
            SELECT
                office_code,
                office_name,
                sub_tier_code,
                agency_code,
                contract_awards_office,
                contract_funding_office,
                financial_assistance_awards_office,
                financial_assistance_funding_office
            FROM office
        """

    @property
    def usas_unlinked_offices_sql(self):
        return """
        DROP TABLE IF EXISTS temp_unique_office_codes_from_source;
        CREATE TEMPORARY TABLE temp_unique_office_codes_from_source (
            awarding_office_code TEXT,
            funding_office_code TEXT,
            UNIQUE (awarding_office_code, funding_office_code)
        );
        INSERT INTO temp_unique_office_codes_from_source
        SELECT * FROM (
                    SELECT DISTINCT awarding_office_code, funding_office_code FROM source_assistance_transaction
                    UNION
                    SELECT DISTINCT awarding_office_code, funding_office_code FROM source_procurement_transaction
                ) s;
        CREATE INDEX awarding_office_code_idx_temp ON temp_unique_office_codes_from_source (awarding_office_code);
        CREATE INDEX funding_office_code_idx_temp ON temp_unique_office_codes_from_source (funding_office_code);
        DELETE FROM office
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_unique_office_codes_from_source AS s
            WHERE s.awarding_office_code = office.office_code OR s.funding_office_code = office.office_code
        );
        DROP TABLE IF EXISTS temp_unique_office_codes_from_source;
        """
