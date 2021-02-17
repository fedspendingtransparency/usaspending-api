import logging
from django.core.management.base import BaseCommand
from django.core.cache import caches


class Command(BaseCommand):
    """
    This command will clear the usaspending-cache (useful after a load or a deletion
    to ensure end users don't see stale data)
    """

    help = "Clears the usaspending-cache"
    logger = logging.getLogger("script")

    def handle(self, *args, **options):
        self.logger.info("Clearing usaspending-cache...")
        cache = caches["usaspending-cache"]
        cache.clear()
        self.logger.info("Done.")
