import logging
from django.core.management.base import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    """
    This command will generate SQL using sqlsequencereset for each app, so that one can repair the primary key
    sequences of the listed models
    """

    help = "Generate SQL to repair primary key sequences"
    logger = logging.getLogger("script")

    def handle(self, *args, **options):
        fixable_apps = ["accounts", "awards", "common", "financial_activities", "references", "submissions"]

        for app in fixable_apps:
            call_command("sqlsequencereset", app)
