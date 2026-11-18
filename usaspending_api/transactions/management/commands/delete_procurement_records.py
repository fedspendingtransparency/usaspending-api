import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

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

    def fetch_deleted_transactions(self) -> Optional[dict]:
        if self.ids is not None:
            return {date.today().strftime("%Y-%m-%d"): self.ids}

        if settings.IS_LOCAL:
            logger.info("Local mode does not handle datetime based deleted records")
            return None

        date_time = self.datetime

        full_key_list = retrieve_deleted_fpds_transactions(start_datetime=date_time)
        ids_to_delete = self.validate_transaction_ids(full_key_list)
        total_ids = sum([len(v) for v in ids_to_delete.values()])
        logger.info(f"Total number of delete records to process: {total_ids}")
        return ids_to_delete

    def store_delete_records(self, deleted_dict: dict) -> None:
        logger.info("Nothing to store for procurement deletes")

    @staticmethod
    def validate_transaction_ids(id_lookup_by_date: dict[str, list[int | str]]) -> dict[str, list[int | str]]:
        result = defaultdict(list)

        for date_key, transaction_ids in id_lookup_by_date.items():
            numeric_ids = []
            string_ids = []

            for val in transaction_ids:
                # Similar logic to using .isnumeric(), but it takes into account decimal values
                try:
                    decimal_val = Decimal(val)
                except (InvalidOperation, TypeError, ValueError):
                    string_ids.append(val)
                    continue

                # NaN and Infinity parse as Decimals but can't be converted to an int; negative IDs are invalid
                if decimal_val.is_finite() and decimal_val >= 0:
                    numeric_ids.append(int(decimal_val))
                else:
                    string_ids.append(val)

            if string_ids:
                logger.info(f"Unexpected {len(string_ids)} non-numeric IDs for {date_key}: {string_ids}")

            if numeric_ids:
                logger.info(f"Obtained {len(numeric_ids)} IDs for {date_key}")
                result[date_key].extend(numeric_ids)
            else:
                # TODO: Validate cases where an error should be raised instead of logging a warning
                logger.warning(f"No {'valid ' if bool(string_ids) else ''}IDs for {date_key}!")

        return result
