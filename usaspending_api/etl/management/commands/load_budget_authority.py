import csv
import glob
import logging
import os.path
from argparse import ArgumentTypeError
from collections import defaultdict
from datetime import date

from django.core.management.base import BaseCommand
from xlrd import open_workbook

from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.common.helpers import fy
from usaspending_api.references.models import OverallTotals, FrecMap

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
    DIRECTORY_PATH = os.path.join('usaspending_api', 'data',
                                  'budget_authority')

    def add_arguments(self, parser):
        parser.add_argument(
            '-q',
            '--quarter',
            type=valid_quarter,
            help='Quarter to load from spreadsheets in data/budget_authority/quarterly',
        )

    def load_frec_map(self):

        # import ipdb; ipdb.set_trace()

        FREC_MAP_PATH = os.path.join(self.DIRECTORY_PATH, 'broker_rules_fr_entity.xlsx')

        workbook = open_workbook(FREC_MAP_PATH)
        sheet = workbook.sheets()[1]
        headers = [cell.value for cell in sheet.row(3)]

        FrecMap.objects.all().delete()

        instances = []
        for i in range(4, sheet.nrows):
            row = dict(zip(headers, (cell.value for cell in sheet.row(i))))
            instance = FrecMap(agency_identifier=row['AID'],
            main_account_code=row['MAIN'],
            treasury_appropriation_account_title = row['GWA_TAS NAME'],
            sub_function_code = row['Sub Function Code'],
            fr_entity_code = row['FR Entity Type'])
            instances.append(instance)
        FrecMap.objects.bulk_create(instances)


    def load_quarterly_spreadsheets(self, quarter, results):
        """Special procedure for getting quarterly update .xls files

        These are downloaded from
        MAX Information and Reports (Executive, Legislative, and Judicial Users)
        https://max.omb.gov/maxportal/document/SF133/Budget/FY%202017%20-%20SF%20133%20Reports%20on%20Budget%20Execution%20and%20Budgetary%20Resources.html
        """

        QUARTERLY_PATH = os.path.join(self.DIRECTORY_PATH, 'quarterly',
                                      '*.xls')
        this_fy = fy(date.today())
        amount_column = 'Q{}_AMT'.format(quarter)
        for filename in glob.glob(QUARTERLY_PATH):
            workbook = open_workbook(filename)
            sheet = workbook.sheets()[0]
            headers = [cell.value for cell in sheet.row(0)]
            for i in range(1, sheet.nrows):
                row = dict(zip(headers, (cell.value for cell in sheet.row(i))))
                if row['LNO'] == '2500':
                    results[(row['TRAG'], None,
                             this_fy)] = int(row[amount_column]) * 1000

    def find_frec(self, agency_identifier, row):
        frec_inst = FrecMap.objects.filter(agency_identifier=agency_identifier,
            main_account_code=row['Account Code'],
            sub_function_code=row['Subfunction Code'])
        if frec_inst.exists():

            return frec_inst.first().fr_entity_code

    def handle(self, *args, **options):

        DIRECTORY_PATH = os.path.join('usaspending_api', 'data',
                                      'budget_authority')
        self.load_frec_map()

        HISTORICAL_PATH = os.path.join(DIRECTORY_PATH, 'budget_authority.csv')
        overall_totals = defaultdict(int)
        results = defaultdict(int)
        with open(HISTORICAL_PATH) as infile:
            BudgetAuthority.objects.all().delete()
            results = defaultdict(int)
            reader = csv.DictReader(infile)
            for row in reader:
                agency_identifier = row['Treasury Agency Code'].zfill(3)
                frec_inst = FrecMap.objects.filter(agency_identifier=agency_identifier,
                    main_account_code=row['Account Code'],
                    sub_function_code=row['Subfunction Code'])
                if frec_inst.exists():
                    frec = frec_inst.first()
                else:
                    frec = None
                frec = self.find_frec(agency_identifier, row)
                for year in range(1976, 2023):
                    amount = row[str(year)]
                    amount = int(amount.replace(',', '')) * 1000
                    results[(agency_identifier, frec, year)] += amount
                    overall_totals[year] += amount

        quarter = options['quarter']
        if quarter:
            self.load_quarterly_spreadsheets(options['quarter'], results)
        else:
            print('No quarter given. Quarterly spreadsheets not loaded')

        BudgetAuthority.objects.bulk_create(
            BudgetAuthority(
                agency_identifier=agency_identifier,
                fr_entity_code=frec,
                year=year,
                amount=amount)
            for ((agency_identifier, frec, year), amount) in results.items())
        OverallTotals.objects.bulk_create(
            OverallTotals(
                fiscal_year=year, total_budget_authority=amount)
            for (year, amount) in overall_totals.items())
