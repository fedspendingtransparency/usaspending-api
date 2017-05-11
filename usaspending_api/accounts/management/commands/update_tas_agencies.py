from datetime import datetime
import logging
import os
from django.core.management.base import BaseCommand
from usaspending_api.accounts.models import TreasuryAppropriationAccount


class Command(BaseCommand):
    """
    This command calls "update_agency_linkages" on all TAS objects
    """
    help = "Updates all TAS agencies to have proper AID ATA Agency linkages"
    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        self.logger.info("Updating TAS agency linkages...")
        for tas in TreasuryAppropriationAccount.objects.all():
            tas.update_agency_linkages()
        self.logger.info("Dones")
