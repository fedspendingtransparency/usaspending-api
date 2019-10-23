import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from psycopg2.sql import Composable, Composed, Identifier, SQL, Literal

from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string

from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.transactions.models import SourceProcurmentTransaction


def get_model_fields(model):
    return tuple([f.name for f in model._meta.get_fields(include_parents=False)])


class Command(BaseCommand):

    help = "Upsert procurement transactions from a broker system into USAspending"
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive_group.add_argument(
            "--ids",
            nargs="+",
            type=int,
            help="Load/Reload transactions using this detached_award_procurement_id list (space-separated)",
        )
        mutually_exclusive_group.add_argument(
            "--date",
            dest="date",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Load/Reload all FPDS records from the provided datetime to the script execution start time.",
        )
        mutually_exclusive_group.add_argument(
            "--since-last-load",
            action="store_true",
            help="Equivalent to loading from date, but date is drawn from last update date recorded in DB",
        )
        mutually_exclusive_group.add_argument(
            "--file",
            metavar="FILEPATH",
            type=str,
            help="Load/Reload transactions using the detached_award_procurement_id list stored at this file path (one ID per line)"
            "to reload, one ID per line. Nonexistent IDs will be ignored.",
        )
        mutually_exclusive_group.add_argument(
            "--reload-all",
            action="store_true",
            help="Script will load or reload all FPDS records in broker database, from all time. This does NOT clear the USASpending database first",
        )

    def handle(self, *args, **options):
        fields = get_model_fields(SourceProcurmentTransaction)
        # print(fields)
        # print("---------------------------------------")
        excluded = ", ".join(["{col} = EXCLUDED.{col}".format(col=field) for field in fields])
        # print(excluded)
        # print("=======================================")
        from random import random

        values = SQL(", ".join([str(random() * 100) for _ in fields]))
        # print(values)
        # print("+++++++++++++++++++++++++++++++++++++++")

        sql = SQL(UPSERT_SQL).format(
            destination=Identifier(SourceProcurmentTransaction().table_name),
            fields=Composed([Identifier(f) for f in fields]),
            values=values,
            excluded=Composable(excluded),
        )

        print(sql)
        print("=======================================")
        print(convert_composable_query_to_string(sql=sql, model=SourceProcurmentTransaction))

        # with connection.cursor() as cursor:
        #     print(sql.as_string(cursor.connection))
        #     self.logger.info("Restocking parent_award")
        #     cursor.execute("")


UPSERT_SQL = """
INSERT INTO {destination} {fields}
VALUES ({values})
ON CONFLICT detached_award_procurement_id DO UPDATE SET
{excluded}
"""
