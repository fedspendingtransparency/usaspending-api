import csv
import glob
import logging
import os.path
from argparse import ArgumentTypeError
from collections import defaultdict
from datetime import date

from django.core.management.base import BaseCommand
from xlrd3 import open_workbook

from usaspending_api.accounts.models import BudgetAuthority
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.references.models import OverallTotals, FrecMap

logger = logging.getLogger("script")
exception_logger = logging.getLogger("exceptions")


def valid_quarter(raw):
    result = int(raw)
    if not 1 <= result <= 4:
        raise ArgumentTypeError("Quarter should be 1-4")
    return result


class Command(BaseCommand):
    """
    Loads historical budget authority data from spreadsheets.

    Expects the following files:

        budget_authority.csv: Historical budget authority by FY
        broker_rules_fr_entity.xlsx: fr_entity_codes by agency ID, MAC, subfunction code
        quarterly/*.xls: quarter-by-quarter SF133 worksheets for this fiscal year

    Files in `quarterly` are only read if which quarter is specified
    with the `--quarter` option.  They're currently downloaded from
    the "Quarterly SF133s in Excel" section of "MAX Information and Reports":

    https://max.omb.gov/maxportal/document/SF133/Budget/FY%202017%20-%20SF%20133%20Reports%20on%20Budget%20Execution%20and%20Budgetary%20Resources.html

    By default, all the files are expected in the
    usaspending_api/data/budget_authority directory
    (they are checked into the repository there).
    A different directory can be searched instead
    using the --directory option.  (This is done for
    testing to test against smaller versions of the files.)
    """

    help = "Loads historical budget authority data from a CSV"
    DEFAULT_DIRECTORY_PATH = os.path.join("usaspending_api", "data", "budget_authority")

    def add_arguments(self, parser):
        parser.add_argument(
            "-q",
            "--quarter",
            type=valid_quarter,
            help="Quarter to load from spreadsheets in data/budget_authority/quarterly",
        )

        parser.add_argument(
            "-d",
            "--directory",
            default=self.DEFAULT_DIRECTORY_PATH,
            help="Directory containing broker_rules_fr_entity.xlsx, budget_authority.csv, and quarterly/ directory",
        )

    def load_frec_map(self):

        frec_map_path = os.path.join(self.directory, "broker_rules_fr_entity.xlsx")

        workbook = open_workbook(frec_map_path)
        sheet = list(workbook.sheets())[1]
        headers = [cell.value for cell in sheet.row(3)]

        FrecMap.objects.all().delete()

        instances = []
        for i in range(4, sheet.nrows):
            row = dict(zip(headers, (cell.value for cell in sheet.row(i))))

            logger.info("Load FREC Map: AID {} / MAC {} / TAS {}".format(row["AID"], row["MAIN"], row["GWA_TAS_NAME"]))
            instance = FrecMap(
                agency_identifier=row["AID"],
                main_account_code=row["MAIN"],
                treasury_appropriation_account_title=row["GWA_TAS_NAME"],
                sub_function_code=row["Sub Function Code"],
                fr_entity_code=row["FR Entity Type"],
            )

            instances.append(instance)

        logger.info("Running bulk create across FREC map...")
        FrecMap.objects.bulk_create(instances)

    def load_quarterly_spreadsheets(self, quarter, results, overall_totals):
        """Special procedure for getting quarterly update .xls files

        These are downloaded from
        MAX Information and Reports (Executive, Legislative, and Judicial Users)
        https://max.omb.gov/maxportal/document/SF133/Budget/FY%202017%20-%20SF%20133%20Reports%20on%20Budget%20Execution%20and%20Budgetary%20Resources.html
        """

        quarterly_path = os.path.join(self.directory, "quarterly", "*.xls")
        this_fy = fy(date.today())
        overall_totals[this_fy] = 0
        amount_column = "Q{}_AMT".format(quarter)
        for filename in glob.glob(quarterly_path):
            workbook = open_workbook(filename)
            sheet = list(workbook.sheets())[0]
            headers = [cell.value for cell in sheet.row(0)]
            for i in range(1, sheet.nrows):
                row = dict(zip(headers, (cell.value for cell in sheet.row(i))))
                if row["LNO"] == "2500":
                    dollars = int(row[amount_column]) * 1000
                    results[(row["TRAG"], None, this_fy)] = dollars
                    overall_totals[this_fy] += dollars

    def find_frec(self, agency_identifier, row):
        frec_inst = FrecMap.objects.filter(
            agency_identifier=agency_identifier,
            main_account_code=row["Account Code"],
            sub_function_code=row["Subfunction Code"],
        )
        if frec_inst.exists():

            return frec_inst.first().fr_entity_code

    def handle(self, *args, **options):

        self.directory = options["directory"]
        self.load_frec_map()

        historical_path = os.path.join(self.directory, "budget_authority.csv")
        overall_totals = defaultdict(int)
        results = defaultdict(int)
        with open(historical_path) as infile:
            BudgetAuthority.objects.all().delete()
            results = defaultdict(int)
            reader = csv.DictReader(infile)
            for row in reader:
                agency_identifier = row["Treasury Agency Code"].zfill(3)

                logger.info("Loading agency identifier from file: {}".format(agency_identifier))
                frec_inst = FrecMap.objects.filter(
                    agency_identifier=agency_identifier,
                    main_account_code=row["Account Code"],
                    sub_function_code=row["Subfunction Code"],
                )
                if frec_inst.exists():
                    frec = frec_inst.first()
                else:
                    frec = None
                frec = self.find_frec(agency_identifier, row)
                for year in range(1976, 2023):
                    amount = row[str(year)]
                    amount = int(amount.replace(",", "")) * 1000
                    results[(agency_identifier, frec, year)] += amount
                    overall_totals[year] += amount

        quarter = options["quarter"]
        if quarter:
            self.load_quarterly_spreadsheets(options["quarter"], results, overall_totals)
        else:
            logger.info("No quarter given. Quarterly spreadsheets not loaded.")

        logger.info("Running bulk create across agencies...")
        BudgetAuthority.objects.bulk_create(
            BudgetAuthority(agency_identifier=agency_identifier, fr_entity_code=frec, year=year, amount=amount)
            for ((agency_identifier, frec, year), amount) in results.items()
        )

        logger.info("Running bulk create across totals...")
        OverallTotals.objects.bulk_create(
            OverallTotals(fiscal_year=year, total_budget_authority=amount) for (year, amount) in overall_totals.items()
        )
