"""
Cleans up the model description fields.
"""

from django.core.management.base import BaseCommand
from usaspending_api.etl.helpers import update_model_description_fields
from usaspending_api.etl.award_helpers import (
    update_awards, update_contract_awards,
    update_award_categories, )
import logging


class Command(BaseCommand):

    help = "Cleaning up model description fields takes a long time, so use this to update them if you are using" \
           "--noclean to skip this during load_submission."

    def handle(self, *args, **options):
        logger = logging.getLogger('console')

        # 1. Update the descriptions
        logger.info('Manually cleaning up model description fields...')
        update_model_description_fields()
        # 2. Update awards to reflect their latest associated txn info
        logger.info('Manually cleaning up awards...')
        update_awards()
        # 3. Update contract-specific award fields to reflect latest txn info
        logger.info('Manually cleaning up contract-specific awards...')
        update_contract_awards()
        # 4. Update the category variable
        logger.info('Manually cleaning up award categories...')
        update_award_categories()
