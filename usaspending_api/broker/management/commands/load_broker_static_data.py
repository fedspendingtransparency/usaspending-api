import logging

from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.broker import lookups
from usaspending_api.broker.models import ExternalDataType

logger = logging.getLogger("script")


@transaction.atomic
class Command(BaseCommand):
    help = "Loads static data necessary for broker related tasks"

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info("Starting data load: Broker Static Data")
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
        logger.info("Finished data load: Broker Static Data")
