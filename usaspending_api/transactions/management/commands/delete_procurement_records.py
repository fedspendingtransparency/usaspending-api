import logging

from collections import defaultdict
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand

from usaspending_api.transactions.agnostic_transaction_deletes import AgnosticDeletes
from usaspending_api.transactions.models.source_procurement_transaction import SourceProcurementTransaction
from usaspending_api.transactions.transaction_delete_journal_helpers import retrieve_deleted_fpds_transactions

logger = logging.getLogger("script")


class Command(AgnosticDeletes, BaseCommand):
    help = "Delete procurement transactions in an USAspending database"
    destination_table_name = SourceProcurementTransaction().table_name
    shared_pk = "detached_award_procurement_id"

    def fetch_deleted_transactions(self, date_time: datetime):
        ids_to_delete = defaultdict(list)

        if settings.IS_LOCAL:
            logger.info("Local mode does not handle deleted records")
            return None

        full_key_list = retrieve_deleted_fpds_transactions(start_datetime=date_time)

        for date, transaction_ids in full_key_list.items():
            numeric_keys = list([int(row) for row in transaction_ids if row.isnumeric()])
            odd_ids = set(transaction_ids).symmetric_difference([str(id) for id in numeric_keys])

            if odd_ids:
                logger.info(f"Unexpected non-numeric IDs in file: {list(odd_ids)}")

            if numeric_keys:
                logger.info(f"Obtained {len(numeric_keys)} IDs in file")
                ids_to_delete[date].extend(numeric_keys)
            else:
                logger.warn("No IDs in file!")

        total_ids = sum([len(v) for v in ids_to_delete.values()])
        logger.info(f"Total number of delete records to process: {total_ids}")
        return full_key_list

    def store_delete_records(self, id_list):
        logger.info("Nothing to store for procurement deletes")
