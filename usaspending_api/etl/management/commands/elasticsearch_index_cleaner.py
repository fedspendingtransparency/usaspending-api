import logging

from django.core.management.base import BaseCommand
from opensearchpy import OpenSearch

from usaspending_api.settings import ES_HOSTNAME, ES_TIMEOUT

logger = logging.getLogger("console")


class Command(BaseCommand):
    help = 'List all aliases in the OpenSearch cluster'

    def add_arguments(self, parser):
        parser.add_argument(
            "--ignore-indexes",
            type=str,
            nargs='*',
            default=[],
            help="List of indexes to ignore when checking for stale indexes to delete."
        )

    def handle(self, *args, **options):
        indexes_to_ignore = options.get('ignore_indexes')
        logger.info(f"Ignoring indexes: {indexes_to_ignore}")

        try:
            client = OpenSearch([ES_HOSTNAME], timeout=ES_TIMEOUT)

            if not client.ping():
                logger.error('Failed to connect to OpenSearch cluster')
                return

            # Get all aliases in OpenSearch cluster
            aliases = { alias['alias']: alias['index'] for alias in client.cat.aliases(format='json') }

            if not aliases:
                logger.warning('No aliases found in the cluster')
                return

            logger.info("Aliases found:")
            for alias_name, index_name in aliases.items():
                logger.info(f"{alias_name:<40} => {index_name}")

            # Get all indexes in OpenSearch cluster and ignore the system indexes (like .opensearch-observability)
            all_indexes = [idx for idx in client.indices.get_alias(index='*').keys() if not idx.startswith('.')]

            deleted_index_count = 0
            for idx in all_indexes:
                if idx not in indexes_to_ignore and idx not in aliases.values():
                    logger.info(f"Removing index: {idx}")
                    client.indices.delete(index=idx)
                    deleted_index_count += 1

            logger.info(f"Deleted {deleted_index_count} indexes")

        except Exception as e:
            logger.error(f'Error: {str(e)}')
            raise
