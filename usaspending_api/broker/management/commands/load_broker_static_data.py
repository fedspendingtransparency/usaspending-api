import logging

from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.broker import lookups
from usaspending_api.broker.models import DeltaTableLoadVersion, ExternalDataType

logger = logging.getLogger("script")


@transaction.atomic
class Command(BaseCommand):
    help = "Loads static data necessary for broker related tasks"

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info("Starting data load: Broker Static Data")

        # External Data Load Date
        logger.info("Loading External Data Types...")
        new_rows = 0

        for external_data_type in lookups.EXTERNAL_DATA_TYPE:
            _, created = ExternalDataType.objects.update_or_create(
                external_data_type_id=external_data_type.id,
                name=external_data_type.name,
                defaults={"description": external_data_type.desc},
            )
            if created:
                new_rows += 1

        logger.info("Stored External Data Types, {} records added".format(new_rows))

        # Delta Table Load Version
        logger.info("Loading Delta Table Load Versions...")
        new_rows = 0

        for load_version_type in lookups.DELTA_TABLE_LOAD_VERSION_TYPE:
            _, created = DeltaTableLoadVersion.objects.update_or_create(
                delta_table_load_version_id=load_version_type.id,
                name=load_version_type.name,
                defaults={"description": load_version_type.desc},
            )
            if created:
                new_rows += 1
        logger.info("Stored Delta Table Load Versions, {} records added".format(new_rows))

        logger.info("Finished data load: Broker Static Data")
