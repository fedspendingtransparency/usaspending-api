"""
Cleans up the model description fields.
"""

from django.core.management.base import BaseCommand
from usaspending_api.etl.helpers import update_model_description_fields
import logging


class Command(BaseCommand):

    help = "Cleaning up model description fields takes a long time, so use this to update them if you are using" \
           "--noclean to skip this during load_submission."

    def handle(self, *args, **options):
        logger = logging.getLogger('console')
        logger.info('Manually cleaning up model description fields...')
        update_model_description_fields()
