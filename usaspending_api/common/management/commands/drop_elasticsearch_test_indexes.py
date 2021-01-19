import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch


logger = logging.getLogger("console")


TEST_INDEX_NAME_PATTERN = "test-*"


class Command(BaseCommand):
    def handle(self, *args, **options):
        client = Elasticsearch([settings.ES_HOSTNAME], timeout=settings.ES_TIMEOUT)
        response = client.indices.delete(TEST_INDEX_NAME_PATTERN)
        if response.get("acknowledged") is True:
            logger.info(
                "All Elasticsearch indexes matching '{}' have been dropped from {}... probably.".format(
                    TEST_INDEX_NAME_PATTERN, settings.ES_HOSTNAME
                )
            )
        else:
            logger.warning(
                "Attempted to drop All Elasticsearch indexes matching '{}' from {} but did "
                "not receive a positive acknowledgment.  Is that a problem?  ¯\\_(ツ)_/¯".format(
                    TEST_INDEX_NAME_PATTERN, settings.ES_HOSTNAME
                )
            )
