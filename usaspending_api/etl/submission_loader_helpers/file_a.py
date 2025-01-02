import logging
import re

from collections import defaultdict

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.etl.submission_loader_helpers.bulk_create_manager import BulkCreateManager
from usaspending_api.etl.submission_loader_helpers.treasury_appropriation_account import (
    bulk_treasury_appropriation_account_tas_lookup,
    get_treasury_appropriation_account_tas_lookup,
)


logger = logging.getLogger("script")


def get_file_a(submission_attributes, db_cursor):
    db_cursor.execute(
        "SELECT * FROM published_appropriation WHERE submission_id = %s", [submission_attributes.submission_id]
    )
    return dictfetchall(db_cursor)


def load_file_a(submission_attributes, appropriation_data, db_cursor):
    """Process and load file A broker data (aka TAS balances, aka appropriation account balances)."""
    reverse = re.compile("gross_outlay_amount_by_tas_cpe")
    skipped_tas = defaultdict(int)  # tracks count of rows skipped due to "missing" TAS
    bulk_treasury_appropriation_account_tas_lookup(appropriation_data, db_cursor)

    # Create account objects
    save_manager = BulkCreateManager(AppropriationAccountBalances)
    for row in appropriation_data:

        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(row.get("account_num"))
        if treasury_account is None:
            skipped_tas[tas_rendering_label] += 1
            continue

        # Now that we have the account, we can load the appropriation balances
        # TODO: Figure out how we want to determine what row is overridden by what row
        # If we want to correlate, the following attributes are available in data broker data that might be useful:
        # appropriation_id, row_number appropriation_balances = something something get appropriation balances...
        appropriation_balances = AppropriationAccountBalances()

        value_map = {
            "treasury_account_identifier": treasury_account,
            "submission": submission_attributes,
            "reporting_period_start": submission_attributes.reporting_period_start,
            "reporting_period_end": submission_attributes.reporting_period_end,
        }

        field_map = {}

        save_manager.append(
            load_data_into_model(
                appropriation_balances, row, field_map=field_map, value_map=value_map, save=False, reverse=reverse
            )
        )

    save_manager.save_stragglers()

    for tas, count in skipped_tas.items():
        logger.info(f"Skipped {count:,} rows due to {tas}")

    total_tas_skipped = sum([count for count in skipped_tas.values()])

    if total_tas_skipped > 0:
        logger.info(f"SKIPPED {total_tas_skipped:,} ROWS of File A (missing TAS)")
    else:
        logger.info("All File A records in Broker loaded into USAspending")
