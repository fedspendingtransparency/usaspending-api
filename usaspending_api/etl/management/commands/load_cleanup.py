"""
Cleans up the model description fields.
"""

from django.core.management.base import BaseCommand
from usaspending_api.etl.helpers import update_model_description_fields
import logging


class Command(BaseCommand):

    help = (
        "Cleaning up model description fields takes a long time, so use this to update them if you are using"
        "--noclean to skip this during load_submission."
    )

    def handle(self, *args, **options):
        logger = logging.getLogger("console")

        logger.info("Updating model description fields...")
        update_model_description_fields()

        logger.info(
            "SKIPPING - Done in load_base - Updating awards to reflect their latest associated transaction info..."
        )
        # update_awards()

        logger.info(
            "SKIPPING - Done in load_base - Updating contract-specific awards to reflect their latest "
            "transaction info..."
        )
        # update_contract_awards()

        logger.info("SKIPPING - Done in load_base - Updating award category variables...")
        # update_award_categories()
