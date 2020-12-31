import logging

from django.db import connection
from django.core.management.base import BaseCommand
from django.core.management import call_command

logger = logging.getLogger("console")
# if we're ever planning to run this again, update this to receive the app
# list as a command argument instead of hard-coding here
app_list = ["accounts", "awards", "common", "financial_activities", "references", "submissions"]

remove_ghost_migrations = "DELETE FROM django_migrations WHERE app in %s "


class Command(BaseCommand):
    """
    Resets Django migrations for the specified applications.
    Note: This command should be run in conjunction with a commit that:
    1. removes the existing migration .py files
    2. adds a new set of 0001_initial migration files that reflect the
       current state of the database
    """

    help = "Resets Django migrations"

    def handle(self, *args, **options):
        # the actual migration .py files are gone by the time this command
        # executes, so remove the "ghost" records of them in the migrations table
        # (normally, you'd do this by running a 'zero' migration with fake=true,
        # but that needs the migration.py files for reference, and we've already
        # deleted them)
        with connection.cursor() as cursor:
            cursor.execute(remove_ghost_migrations, [tuple(app_list)])
            rows = cursor.rowcount
        logger.info("Removed {} django_migrations records for apps {}".format(rows, app_list))

        # fake the initial migrations
        for app in app_list:
            call_command("migrate", app_label=app, fake_initial=True, interactive=False)
