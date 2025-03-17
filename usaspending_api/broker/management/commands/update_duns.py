import logging

from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.models import Max

from usaspending_api.recipient.models import DUNS
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import load_data_into_model

logger = logging.getLogger("script")


class Command(BaseCommand):
    def gather_new_duns(self, db_cursor, update_date, latest_broker_duns_id):
        new_duns_query = (
            "SELECT * FROM sam_recipient "
            "WHERE updated_at > '" + str(update_date) + "' AND "
            "sam_recipient_id > " + str(latest_broker_duns_id)
        )
        logger.info("Gathering duns created since last update")
        db_cursor.execute(new_duns_query)
        new_duns = dictfetchall(db_cursor)

        update_duns_query = (
            "SELECT * FROM sam_recipient "
            "WHERE updated_at > '" + str(update_date) + "' AND "
            "sam_recipient_id <= " + str(latest_broker_duns_id)
        )
        logger.info("Gathering duns updated since last update")
        db_cursor.execute(update_duns_query)
        update_duns = dictfetchall(db_cursor)

        return new_duns, update_duns

    def add_duns(self, new_duns, update_date):
        logger.info("Adding {} duns records".format(len(new_duns)))
        new_records = []
        for row in new_duns:
            new_record = load_data_into_model(
                DUNS(),
                row,
                field_map={
                    "awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
                    "legal_business_name": "legal_business_name",
                    "ultimate_parent_unique_ide": "ultimate_parent_unique_ide",
                    "ultimate_parent_legal_enti": "ultimate_parent_legal_enti",
                    "broker_duns_id": "sam_recipient_id",
                },
                value_map={"update_date": update_date},
                as_dict=False,
                save=False,
            )
            new_records.append(new_record)
        DUNS.objects.bulk_create(new_records)

    def update_duns(self, update_duns, update_date):
        logger.info("Updating {} duns records".format(len(update_duns)))
        for row in update_duns:
            equivalent_duns = DUNS.objects.filter(broker_duns_id=row["sam_recipient_id"])[0]
            load_data_into_model(
                equivalent_duns,
                row,
                field_map={
                    "awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
                    "legal_business_name": "legal_business_name",
                    "ultimate_parent_unique_ide": "ultimate_parent_unique_ide",
                    "ultimate_parent_legal_enti": "ultimate_parent_legal_enti",
                    "broker_duns_id": "sam_recipient_id",
                },
                value_map={"update_date": update_date},
                as_dict=False,
                save=True,
            )

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info("Running duns updater to pull any added/updated DUNS records from the broker.")
        total_start = datetime.now()
        new_update_date = total_start.strftime("%Y-%m-%d")

        db_cursor = connections[settings.DATA_BROKER_DB_ALIAS].cursor()

        update_date_query = DUNS.objects.all().aggregate(Max("update_date"))
        update_date = update_date_query["update_date__max"]

        latest_broker_duns_query = DUNS.objects.all().aggregate(Max("broker_duns_id"))
        latest_broker_duns_id = latest_broker_duns_query["broker_duns_id__max"]

        new_duns, update_duns = self.gather_new_duns(db_cursor, update_date, latest_broker_duns_id)
        self.add_duns(new_duns, new_update_date)
        self.update_duns(update_duns, new_update_date)

        logger.info("Finished updating DUNS in %s seconds." % str(datetime.now() - total_start))
