from django.core.management.base import BaseCommand
from usaspending_api.etl.helpers import update_model_description_fields


class Command(BaseCommand):
    """
    This command will generate SQL using sqlsequencereset for each app, so that
    one can repair the primary key sequences of the listed models
    """

    help = "Update model description fields based on code"

    def handle(self, *args, **options):
        update_model_description_fields()
