import logging

from datetime import date
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
    shared_pk = "published_fabs_id"

    def fetch_deleted_transactions(self) -> Optional[dict]:
        if self.ids is not None:
            return {date.today().strftime("%Y-%m-%d"): self.ids}

        if settings.IS_LOCAL:
            logger.info("Local mode does not handle datetime based deleted records")
            return None

        date_time = self.datetime

        sql = """
        select  published_fabs_id
        from    published_fabs
        where   UPPER(afa_generated_unique) in (
                    select  UPPER(afa_generated_unique)
                    from    published_fabs
                    where   updated_at >= %s and
                            upper(correction_delete_indicatr) = 'D'
                ) and
                is_active is not true
        """
        with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
            cursor.execute(sql, [date_time])
            results = cursor.fetchall()

        if results is None:
            logger.info("No new inactive records found")
            r = None
        else:
            logger.info(f"Found {len(results)} inactive transactions to remove")
            r = {date_time: [row[0] for row in results]}

        return r

    def store_delete_records(self, deleted_dict: dict) -> None:
        """FABS needs to store IDs for downline ETL, run that here"""
        sql = """
            SELECT afa_generated_unique
            FROM   published_fabs
            WHERE  published_fabs_id IN {ids}
        """
        records = []

        if not self.dry_run:
            id_list = [item for row in deleted_dict.values() for item in row]
            if len(id_list) == 1:
                id_list += id_list

            if len(id_list) > 0:
                with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
                    cursor.execute(sql.format(ids=tuple(id_list)))
                    afa_id_list = cursor.fetchall()
                    records = [afa_id[0] for afa_id in afa_id_list]

            store_deleted_fabs(records)
