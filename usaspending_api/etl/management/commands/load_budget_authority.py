import logging
import csv
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connections

from usaspending_api.accounts.models import FederalAccount, BudgetAuthority
from usaspending_api.references.models import OverallTotals
logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    """
    Loads historical budget authority data from a CSV
    """
    help = "Loads historical budget authority data from a CSV"

    def add_arguments(self, parser):
        parser.add_argument(
            '-f',
            '--file',
            default='usaspending_api/data/budget_authority.csv',
            help='CSV file to load from',
        )

    def handle(self, *args, **options):

        overall_totals = defaultdict(int)
        results = defaultdict(int)
        with open(options['file']) as infile:
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

        BudgetAuthority.objects.bulk_create(BudgetAuthority(
            agency_identifier=agency_identifier, year=year, amount=amount)
            for ((agency_identifier, year), amount) in results.items())
        OverallTotals.objects.bulk_create(
            OverallTotals(fiscal_year=year, total_budget_authority=amount)
            for (year, amount) in overall_totals.items())
