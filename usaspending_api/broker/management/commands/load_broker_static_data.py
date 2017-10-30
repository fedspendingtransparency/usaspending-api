from django.core.management.base import BaseCommand
from usaspending_api.broker.models import ExternalDataType
from usaspending_api.broker import lookups

from django.db import transaction
import logging

logger = logging.getLogger('console')


@transaction.atomic
class Command(BaseCommand):
    help = "Loads static data necessary for broker related tasks"

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting data load: Broker Static Data')

        logger.info('Loading External Data Types...')
        for external_data_type in lookups.EXTERNAL_DATA_TYPE:
            external_data_type_obj = ExternalDataType(external_data_type_id=external_data_type.id, name=external_data_type.name, description=external_data_type.desc)
            external_data_type_obj.save()
        logger.info('Finished loading External Data Types')

        logger.info('Finished data load: Broker Static Data')
