from datetime import datetime
from decimal import Decimal
import logging
import os
import re

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.core.serializers.json import json
from django.db import connections
from django.db.models import F, Q
from django.core.cache import caches
import pandas as pd
import numpy as np

from usaspending_api.accounts.models import (
    AppropriationAccountBalances, AppropriationAccountBalancesQuarterly,
    TreasuryAppropriationAccount)
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards,
    TransactionAssistance, TransactionContract, Transaction)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass, TasProgramActivityObjectClassQuarterly)
from usaspending_api.references.models import (
    Agency, LegalEntity, Location, ObjectClass, RefCountryCode, RefProgramActivity)
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.award_helpers import (
    get_award_financial_transaction, update_awards, update_contract_awards,
    update_award_categories, get_awarding_agency)
from usaspending_api.etl.helpers import get_fiscal_quarter, get_previous_submission
from usaspending_api.etl.broker_etl_helpers import dictfetchall, PhonyCursor
from usaspending_api.etl.subaward_etl import load_subawards
from usaspending_api.references.helpers import canonicalize_location_dict

from usaspending_api.etl.helpers import update_model_description_fields
from usaspending_api.etl.management import load_base

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []
AWARD_CONTRACT_UPDATE_ID_LIST = []

awards_cache = caches['awards']
logger = logging.getLogger('console')


def yyyymmdd(date_str):
    """Convert a YYYY-MM-DD date to YYYYMMDD

    Action_date in the broker is stored as a YYYYMMDD string,
    but we ask user for a more legible YYYY-MM-DD"""

    return str(datetime.strptime(date_str, '%Y-%m-%d').date()).replace('-', '')


class Command(load_base.Command):
    """
    This command will load detached submissions from the DATA Act broker.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable must set so we can pull submission data from their db."

    def add_arguments(self, parser):

        super(Command, self).add_arguments(parser)
        parser.add_argument('--contracts', action='store_true', help='Load contracts (not FA')
        parser.add_argument('--financial_assistance', action='store_true', help='Load financial assistance (not contracts)')
        parser.add_argument('--action_date_begin', type=yyyymmdd, default=None, help='First action_date to get - YYYY-MM-DD')
        parser.add_argument('--action_date_end', type=yyyymmdd, default=None, help='Last action_date to get - YYYY-MM-DD')
        parser.add_argument('--awarding_agency_code', default=None)

    def handle_loading(self, db_cursor, *args, **options):

        submission_attributes = SubmissionAttributes()
        submission_attributes.save()

        if options['contracts'] and options['financial_assistance']:
            raise CommandError('Default is to load both contracts and financial_assistance')

        if not options['financial_assistance']:
            procurement_data = self.broker_data(db_cursor, 'detached_award_procurement', options)
            load_base.load_file_d1(submission_attributes, procurement_data, db_cursor, date_pattern='%Y-%m-%d %H:%M:%S')

        if not options['contracts']:
            assistance_data = self.broker_data(db_cursor, 'published_award_financial_assistance', options)
            load_base.load_file_d2(submission_attributes, assistance_data, db_cursor)

    def broker_data(self, db_cursor, table_name, options):
        filter_sql = []
        filter_values = []
        for (column, filter) in (
                ('action_date_begin', ' AND action_date >= %s'),
                ('action_date_end', ' AND action_date <= %s'),
                ('awarding_agency_code', ' AND awarding_agency_code = %s'), ):
            if options[column]:
                filter_sql.append(filter)
                filter_values.append(options[column])
        filter_sql = "\n".join(filter_sql)

        sql = 'SELECT * FROM {} WHERE true {}'.format(table_name, filter_sql)
        db_cursor.execute(sql, filter_values)
        results = dictfetchall(db_cursor)
        logger.info('Acquired {}, there are {} rows.'.format(table_name, len(results)))
        return results
