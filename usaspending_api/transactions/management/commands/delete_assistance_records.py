import logging

from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections
from typing import Optional

from usaspending_api.broker.helpers.store_deleted_fabs import store_deleted_fabs
from usaspending_api.transactions.agnostic_transaction_deletes import AgnosticDeletes
from usaspending_api.transactions.models.source_assistance_transaction import SourceAssistanceTransaction

logger = logging.getLogger("script")


class Command(AgnosticDeletes, BaseCommand):
    help = "Delete assistance transactions in an USAspending database"
    destination_table_name = SourceAssistanceTransaction().table_name
    shared_pk = "published_award_financial_assistance_id"

    def fetch_deleted_transactions(self, date_time: datetime) -> Optional[dict]:
        if settings.IS_LOCAL:
            logger.info("Local mode does not handle deleted records")
            return None

        sql = """
        select  published_award_financial_assistance_id
        from    published_award_financial_assistance p
        where   UPPER(correction_delete_indicatr) = 'D' and
                not exists (
                    select  *
                    from    published_award_financial_assistance
                    where   afa_generated_unique = p.afa_generated_unique and is_active is true
                )
                and updated_at >= %s
        """
        with connections["data_broker"].cursor() as cursor:
            cursor.execute(sql, [date_time])
            results = cursor.fetchall()

        if results is None:
            logger.info("No new inactive records found")
            return None
        else:
            logger.info(f"Found {len(results)} inactive transactions to remove")
            return {date_time: [row[0] for row in results]}

    def store_delete_records(self, deleted_dict: dict) -> None:
        """FABS needs to store IDs for downline ETL, run that here"""
        sql = """
            SELECT afa_generated_unique
            FROM   published_award_financial_assistance
            WHERE  published_award_financial_assistance_id IN {ids}
        """
        if not self.dry_run:
            id_list = [item for row in deleted_dict.values() for item in row]
            if len(id_list) == 1:
                id_list += id_list
            with connections["data_broker"].cursor() as cursor:
                cursor.execute(sql.format(ids=tuple(id_list)))
                afa_id_list = cursor.fetchall()

            store_deleted_fabs([afa_id[0] for afa_id in afa_id_list])
