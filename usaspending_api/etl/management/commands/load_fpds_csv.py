import logging
import os
import pandas as pd
import signal

from collections import OrderedDict
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import transaction
from usaspending_api.awards.models import TransactionFPDS


logger = logging.getLogger("script")


class Command(BaseCommand):
    """
    This command will load a single submission from Data Broker. If we've already loaded the specified broker
    submisison, this command will remove the existing records before loading them again.
    """

    help = "Updates the TransactionFPDS with the correct data from the fpds csv"

    def add_arguments(self, parser):
        parser.add_argument("--file", dest="fpds_csv", nargs="+", type=str, help="data broker submission id to load")

        parser.add_argument(
            "--fiscal_year", dest="fiscal_year", nargs="+", type=int, help="Year for which to run the historical load"
        )

        # super(Command, self).add_arguments(parser)

    @transaction.atomic
    def handle(self, *args, **options):
        fiscal_year = options.get("fiscal_year")

        if fiscal_year:
            fiscal_year = fiscal_year[0]
            logger.info("Processing data for Fiscal Year " + str(fiscal_year))
        else:
            fiscal_year = 2017

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        fpds_csv = options["fpds_csv"][0]
        if not os.path.exists(fpds_csv):
            raise Exception("FPDS CSV doesn't exist")

        column_header_mapping = {
            "detached_award_proc_unique": 0,
            "base_exercised_options_val": 1,
            "base_and_all_options_value": 2,
        }
        column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

        logger.info("Getting FPDS data from CSV...")

        fpds_data = pd.read_csv(
            fpds_csv,
            usecols=column_header_mapping_ordered.values(),
            names=column_header_mapping_ordered.keys(),
            header=None,
        )

        logger.info("Processing data for Fiscal Year " + str(fiscal_year))
        models = {
            transaction_fpds.detached_award_proc_unique: transaction_fpds
            for transaction_fpds in TransactionFPDS.objects.filter(action_date__fy=fiscal_year)
        }

        new_transactions_found = []
        total_rows = len(fpds_data)
        logger.info("Iterating through FPDS data from CSV...")
        start_time = datetime.now()
        for index, row in fpds_data.iterrows():
            if not (index % 100):
                logger.info(
                    "Processing FPDS CSV Data: "
                    "Processing row {} of {} ({}))".format(str(index), str(total_rows), datetime.now() - start_time)
                )

            detached_award_proc_unique = row["detached_award_proc_unique"]
            if detached_award_proc_unique not in models:
                new_transactions_found.append(detached_award_proc_unique)
                continue
            for field, value in row.items():
                setattr(models[detached_award_proc_unique], field, value)
            models[detached_award_proc_unique].save()
        # if new_transactions_found:
        #     print("Found new transactions: {}. Please inform the broker team.".format(new_transactions_found))
