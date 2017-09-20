from datetime import datetime
import logging
import re
import signal
import sys
import os
from collections import OrderedDict

from django.db import connections, transaction
from django.core.cache import caches
import pandas as pd

from usaspending_api.awards.models import TransactionFPDS
from usaspending_api.etl.management import load_base

# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't
# keep hitting the databroker DB for account data
TAS_ID_TO_ACCOUNT = {}

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []

awards_cache = caches['awards']
logger = logging.getLogger('console')


class Command(load_base.Command):
    """
    This command will load a single submission from the DATA Act broker. If
    we've already loaded the specified broker submisison, this command
    will remove the existing records before loading them again.
    """
    help = "Updates the TransactionFPDS with the correct data from the fpds csv"

    def add_arguments(self, parser):
        parser.add_argument('fpds_csv', nargs=1, help='the data broker submission id to load', type=str)
        super(Command, self).add_arguments(parser)

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):

        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception('Received interrupt signal. Aborting...')

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        fpds_csv = options["fpds_csv"][0]
        if not os.path.exists(fpds_csv):
            raise Exception('FPDS CSV doesn\'t exist')

        column_header_mapping = {
            "detached_award_proc_unique": 0,
            "base_exercised_options_val": 1,
            "base_and_all_options_value": 2
        }
        column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

        fpds_data = pd.read_csv(fpds_csv, usecols=column_header_mapping_ordered.values(), names=column_header_mapping_ordered.keys(), header=None)

        models = {transaction_fpds.detached_award_proc_unique: transaction_fpds for transaction_fpds in TransactionFPDS.objects.all()}

        new_transactions_found = []
        for _, row in fpds_data.iterrows():
            detached_award_proc_unique = row['detached_award_proc_unique']
            if detached_award_proc_unique not in models:
                new_transactions_found.append(detached_award_proc_unique)
                continue
            for field, value in row.items():
                setattr(models[detached_award_proc_unique], field, value)
            models[detached_award_proc_unique].save()
        if new_transactions_found:
            print("Found new transactions: {}. Please inform the broker team.".format(new_transactions_found))