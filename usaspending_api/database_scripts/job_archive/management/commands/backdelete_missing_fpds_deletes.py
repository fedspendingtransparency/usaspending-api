"""
Jira Ticket Number(s): DEV-4941

    Delete FPDS transactions from USAspending that are no longer in Broker.

Expected CLI:

    $ ./manage.py backdelete_missing_fpds_deletes

Purpose:

    This script will simply delete all source_procurement_transaction and transaction_fpds
    records that are no longer in Broker.  Unfortunately, delete files have proven themselves
    to be unreliable so we're going to do this the old-fashioned way... diffing primary keys.
    The diffing has been pre-processed since diffing on the fly is not a quick process.  The
    pertinent detached_award_procurement_ids can be found in backdelete_missing_fpds_deletes.txt.

Life expectancy:

    Once Sprint 106 has been rolled out to production this script is safe to delete... although I
    would recommend keeping it around for a few additional sprints for reference.

    BE SURE TO ALSO DELETE backdelete_missing_fpds_deletes.txt WHEN YOU DELETE THIS SCRIPT!

"""

import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction, DEFAULT_DB_ALIAS
from pathlib import Path
from usaspending_api.broker.management.commands.load_fpds_transactions import Command as FPDSCommand
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer
from usaspending_api.etl.transaction_loaders.fpds_loader import delete_stale_fpds
from usaspending_api.transactions.management.commands.delete_procurement_records import Command as SourceCommand


logger = logging.getLogger("script")


def get_matching_count(connection_name, table_name, ids):
    with connections[connection_name].cursor() as cursor:
        cursor.execute(
            f"""
                select  count(*)
                from    "{table_name}"
                where   detached_award_procurement_id in %s
            """,
            [ids],
        )
        return cursor.fetchall()[0][0]


class Command(BaseCommand):
    def handle(self, *args, **options):
        with Timer("Backdelete undeleted FPDS transactions"):

            filepath = str(Path(__file__).resolve().parent / "backdelete_missing_fpds_deletes.txt")
            with open(filepath) as f:
                offending_ids = tuple(int(id_) for id_ in f if id_.strip().isnumeric())
            logger.info(f"{len(offending_ids):,} detached_award_procurement_ids found in {filepath}")

            match_count = get_matching_count("data_broker", "detached_award_procurement", offending_ids)
            if match_count > 0:
                raise RuntimeError(
                    f"Found {match_count:,} detached_award_procurement_ids in Broker's detached_award_procurement"
                )
            logger.info("Confirmed that none of the detached_award_procurement_ids exist in Broker")

            # Structure necessary for deletes.
            delete_ids = {"2020-04-27": offending_ids}

            with transaction.atomic():

                match_count = get_matching_count(DEFAULT_DB_ALIAS, "source_procurement_transaction", offending_ids)
                logger.info(f"Found {match_count:,} detached_award_procurement_ids in source_procurement_transaction")

                source_command = SourceCommand()
                source_command.delete_rows(delete_ids)

                match_count = get_matching_count(DEFAULT_DB_ALIAS, "transaction_fpds", offending_ids)
                logger.info(f"Found {match_count:,} detached_award_procurement_ids in transaction_fpds")

                fpds_command = FPDSCommand()
                stale_awards = delete_stale_fpds(delete_ids)
                fpds_command.update_award_records(awards=stale_awards, skip_cd_linkage=False)
