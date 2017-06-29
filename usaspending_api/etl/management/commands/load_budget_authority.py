import logging
import csv
import glob
from datetime import date
import os.path
from collections import defaultdict
from argparse import ArgumentTypeError

from xlrd import open_workbook

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connections

from usaspending_api.accounts.models import FederalAccount, BudgetAuthority
from usaspending_api.references.models import OverallTotals
from usaspending_api.common.helpers import fy
logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


def valid_quarter(raw):
    result = int(raw)
    if not 1 <= result <= 4:
        raise ArgumentTypeError('Quarter should be 1-4')
    return result


class Command(BaseCommand):
    """
    Loads historical budget authority data from a CSV
    """

    help = "Loads historical budget authority data from a CSV"
    DIRECTORY_PATH = os.path.join('usaspending_api', 'data', 'budget_authority')

    def add_arguments(self, parser):
        parser.add_argument(
            '-q',
            '--quarter',
            type=valid_quarter,
            help='Quarter to load from spreadsheets in data/budget_authority/quarterly',
        )

    def load_quarterly_spreadsheets(self, quarter, results):
        """Special procedure for getting quarterly update .xls files

        These are downloaded from
        MAX Information and Reports (Executive, Legislative, and Judicial Users)
        https://max.omb.gov/maxportal/document/SF133/Budget/FY%202017%20-%20SF%20133%20Reports%20on%20Budget%20Execution%20and%20Budgetary%20Resources.html
        """

        QUARTERLY_PATH = os.path.join(self.DIRECTORY_PATH, 'quarterly', '*.xls')
        this_fy = fy(date.today())
        amount_column = 'Q{}_AMT'.format(quarter)
        for filename in glob.glob(QUARTERLY_PATH):
            workbook = open_workbook(filename)
            sheet = workbook.sheets()[0]
            headers = [cell.value for cell in sheet.row(0)]
            for i in range(1, sheet.nrows):
                row = dict(zip(headers, (cell.value for cell in sheet.row(i))))
                if row['LNO'] == '2500':
                    results[(row['TRAG'], this_fy)] = int(row[amount_column]) * 1000

    def handle(self, *args, **options):

        DIRECTORY_PATH = os.path.join('usaspending_api', 'data', 'budget_authority')
        HISTORICAL_PATH = os.path.join(DIRECTORY_PATH, 'budget_authority.csv')

        overall_totals = defaultdict(int)
        results = defaultdict(int)
        with open(HISTORICAL_PATH) as infile:
            BudgetAuthority.objects.all().delete()
            results = defaultdict(int)
            reader = csv.DictReader(infile)
            for row in reader:
                agency_identifier = row['Treasury Agency Code'].zfill(3)
                for year in range(1976, 2023):
                    amount = row[str(year)]
                    amount = int(amount.replace(',', '')) * 1000
                    results[(agency_identifier, year)] += amount
                    overall_totals[year] += amount

        quarter = options['quarter']
        if quarter:
            self.load_quarterly_spreadsheets(options['quarter'], results)
        else:
            print('No quarter given. Quarterly spreadsheets not loaded')

        BudgetAuthority.objects.bulk_create(BudgetAuthority(
            agency_identifier=agency_identifier, year=year, amount=amount)
            for ((agency_identifier, year), amount) in results.items())
        OverallTotals.objects.bulk_create(
            OverallTotals(fiscal_year=year, total_budget_authority=amount)
            for (year, amount) in overall_totals.items())
