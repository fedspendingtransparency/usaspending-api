from datetime import datetime
from decimal import Decimal
import logging
import os
import re

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.core.management import call_command
from django.core.management.base import BaseCommand
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
    Agency, LegalEntity, Location, ObjectClass, RefCountryCode, Cfda, RefProgramActivity)
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


def to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d').date()


class Command(load_base.Command):
    """
    This command will load detached submissions from the DATA Act broker.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable must set so we can pull submission data from their db."

    def add_arguments(self, parser):

        super(Command, self).add_arguments(parser)
        parser.add_argument('--action_date_begin', type=to_date, default=None, help='First action_date to get - YYYY-MM-DD')
        parser.add_argument('--action_date_end', type=to_date, default=None, help='Last action_date to get - YYYY-MM-DD')
        parser.add_argument('--awarding_agency_code', default=None)
        parser.add_argument('--awarding_agency_name', type=str.lower, default=None)

    def handle_loading(self, db_cursor, *args, **options):

        submission_attributes = SubmissionAttributes()
        submission_attributes.save()

        # File D1
        sql = 'SELECT * FROM detached_award_procurement WHERE true'
        filter_values = []
        for (column, filter) in (
                ('action_date_begin', ' AND CAST(action_date AS DATE) >= %s'),  # what a performance-killer!
                ('action_date_end', ' AND CAST(action_date AS DATE) <= %s'),
                ('awarding_agency_code', ' AND awarding_agency_code = %s'),
                ('awarding_agency_name', ' AND LOWER(awarding_agency_code) = %s'), ):
            if options[column]:
                sql += filter
                filter_values.append(options[column])
        db_cursor.execute(sql, filter_values)
        procurement_data = dictfetchall(db_cursor)
        logger.info('Acquired award procurement data for detached, there are ' + str(len(procurement_data)) + ' rows.')
        load_base.load_file_d1(submission_attributes, procurement_data, db_cursor, date_pattern='%Y-%m-%d %H:%M:%S')
