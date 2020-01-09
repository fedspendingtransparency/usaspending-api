"""
Jira Ticket Number(s): DEV-3495

    Tweak to Federal Account Name derivation mechanism.

Expected CLI:

    $ ./manage.py repair_federal_accounts

Purpose:

    Some general federal and treasury account cleanup:

        - removes obsolete federal accounts
        - updates stale federal accounts
        - adds new federal accounts
        - re-links mislinked treasury accounts to federal accounts

Life expectancy:

    Once Sprint 98 has been rolled out to production this script is safe to delete... although I
    would recommend keeping it around for a few additional sprints for reference.

"""
import logging

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.references.account_helpers import (
    insert_federal_accounts,
    link_treasury_accounts_to_federal_accounts,
    remove_empty_federal_accounts,
    update_federal_accounts,
)


logger = logging.getLogger("console")


class Command(BaseCommand):

    help = (
        "One off script to clean up federal accounts and re-link treasury accounts to their "
        "appropriate federal accounts."
    )

    def handle(self, *args, **options):

        with Timer("Repairing treasury and federal accounts"):

            with transaction.atomic():

                deletes = remove_empty_federal_accounts()
                logger.info(f"   Removed {deletes:,} Federal Account Rows")

                updates = update_federal_accounts()
                logger.info(f"   Updated {updates:,} Federal Account Rows")

                inserts = insert_federal_accounts()
                logger.info(f"   Created {inserts:,} Federal Account Rows")

                links = link_treasury_accounts_to_federal_accounts()
                logger.info(f"   Linked {links:,} Treasury Accounts to Federal Accounts")

            with connection.cursor() as cursor:

                with Timer("Vacuuming treasury_appropriation_account table"):
                    cursor.execute("vacuum (full, analyze) treasury_appropriation_account")

                with Timer("Vacuuming federal_account table"):
                    cursor.execute("vacuum (full, analyze) federal_account")
