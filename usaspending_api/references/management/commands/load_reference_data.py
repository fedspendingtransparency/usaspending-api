from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command
import os
import csv
import logging
import django
from usaspending_api.references.models import RefCityCountyCode


class Command(BaseCommand):
    help = "Loads reference data into the database. This should be run after \
            creating a new database. This command should be run in the same \
            directory where manage.py is located"

    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        self.logger.info("Beginning reference data loading. This may take a few minutes.")

        self.logger.info("Loading reference_fixture.json")
        call_command('loaddata', 'reference_fixture')

        self.logger.info("Loading agency list")
        call_command('load_agencies')

        # TAS's should only be loaded after agencies to ensure they can properly link to agencies
        self.logger.info("Loading tas_list.csv")
        call_command('loadtas', 'usaspending_api/data/tas_list.csv')

        self.logger.info("Loading program_activity.csv")
        call_command('loadprogramactivity', 'usaspending_api/data/program_activity.csv')

        self.logger.info("Loading ref_city_county_code.csv")
        call_command('load_reference_csv', 'RefCityCountyCode', 'usaspending_api/data/ref_city_county_code.csv', 'Latin-1')
        RefCityCountyCode.canonicalize()

        self.logger.info("Loading CFDA data")
        call_command('loadcfda')

        self.logger.info("Loading descriptions of commonly used terms")
        call_command('load_glossary')

        self.logger.info("Loading budget authority data")
        call_command('load_budget_authority')

        self.logger.info("Reference data loaded.")
