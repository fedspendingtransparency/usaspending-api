import logging
import sys

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import ToptierAgency
from usaspending_api.references.reference_helpers import (
    insert_federal_accounts,
    update_federal_accounts,
    remove_empty_federal_accounts,
)

logger = logging.getLogger("console")

TAS_SQL_PATH = "usaspending_api/references/management/sql/restock_tas.sql"


class Command(BaseCommand):
    help = "Update TAS records from the DATA Broker"

    @transaction.atomic()
    def handle(self, *args, **options):
        try:
            call_command("run_sql", "-f", TAS_SQL_PATH)

            # Match funding toptiers by FREC if they didn't match by AID
            unmapped_funding_agencies = TreasuryAppropriationAccount.objects.filter(funding_toptier_agency=None)
            match_count = 0
            msg_str = "\n=== Found {} unmatched funding agencies across all TAS objects. ==="
            logger.info(msg_str.format(unmapped_funding_agencies.count()))
            for next_tas in unmapped_funding_agencies:
                # CGAC code is a combination of FRECs and CGACs. It will never be empty and it will always
                # be unique in ToptierAgencies; this should be safe to do.
                frec_match = ToptierAgency.objects.filter(cgac_code=next_tas.fr_entity_code).first()
                if frec_match:
                    match_count += 1
                    logger.info(
                        "   Matched unknown funding agency for TAS {} with FREC {}".format(
                            next_tas.tas_rendering_label, next_tas.fr_entity_code
                        )
                    )
                    next_tas.funding_toptier_agency = frec_match
                    next_tas.save()

            logger.info("\n=== Updated {} funding toptiers with a FREC agency. ===".format(match_count))

            # update TAS fk relationships to federal accounts
            logger.info("\n=== Updating TAS FK relationships to Federal Accounts ===")
            updates = update_federal_accounts()
            logger.info("   Updated {} Federal Account Rows".format(updates))
            inserts = insert_federal_accounts()
            logger.info("   Created {} Federal Account Rows".format(inserts))
            deletes = remove_empty_federal_accounts()
            logger.info("   Removed {} Federal Account Rows".format(deletes))

            logger.info("\n=== TAS loader finished successfully! ===")
        except Exception as e:
            logger.error(e)
            logger.error("\n=== TAS loader failed ===")
            sys.exit(1)
