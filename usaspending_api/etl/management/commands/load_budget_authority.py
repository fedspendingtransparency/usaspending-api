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
        # Grab the data broker database connections

        overall_totals = defaultdict(int)
        results = defaultdict(int)
        failures = 0
        successes = 0
        with open(options['file']) as infile:
            BudgetAuthority.objects.all().delete()
            results = defaultdict(int)
            reader = csv.DictReader(infile)
            for row in reader:
                cgac = row['Treasury Agency Code'].zfill(3)
                mac = row['Account Code'].zfill(4)
                federal_account = FederalAccount.objects.filter(
                    agency_identifier=cgac, main_account_code=mac).first()
                if federal_account:
                    successes += 1
                    for year in range(1976, 2023):
                        amount = row[str(year)]
                        amount = int(amount.replace(',', '')) * 1000
                        results[(federal_account, year)] += amount
                        overall_totals[year] += amount
                else:
                    failures += 1
                    logger.error('No federal account for Treasury Account Code (GCAC) {}, Account Code (MAC) {}'
                                 .format(cgac, mac))

        BudgetAuthority.objects.bulk_create(BudgetAuthority(
            federal_account=federal_account, year=year, amount=amount)
            for ((federal_account, year), amount) in results.items())
        logger.info('{} successes, {} failures'.format(successes, failures))
        OverallTotals.objects.bulk_create(
            OverallTotals(fiscal_year=year, total_budget_authority=amount)
            for (year, amount) in overall_totals.items() )
