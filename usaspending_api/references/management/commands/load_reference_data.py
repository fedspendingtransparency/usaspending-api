from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.conf import settings
import logging
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

        self.logger.info("Loading state data")
        call_command('load_state_data')

        # TAS's should only be loaded after agencies to ensure they can properly link to agencies
        self.logger.info("Loading tas_list.csv")
        if settings.IS_LOCAL:
            call_command('loadtas', 'usaspending_api/data/tas_list.csv')
        else:
            call_command('loadtas', 'gtas-sf133')

        self.logger.info("Loading program_activity.csv")
        if settings.IS_LOCAL:
            call_command('load_program_activity', 'usaspending_api/data/program_activity.csv')
        else:
            call_command('load_program_activity')

        self.logger.info("Loading ref_city_county_code.csv")
        call_command('load_reference_csv', 'RefCityCountyCode', 'usaspending_api/data/ref_city_county_code.csv',
                     'Latin-1')
        RefCityCountyCode.canonicalize()

        self.logger.info("Loading CFDA data")
        call_command('loadcfda', 'https://files.usaspending.gov/reference_data/cfda.csv')

        self.logger.info("Loading descriptions of commonly used terms")
        call_command('load_glossary')

        self.logger.info("Loading static data")
        call_command('load_broker_static_data')
        call_command('load_download_static_data')

        self.logger.info("Loading budget authority data")
        call_command('load_budget_authority')

        self.logger.info("Loading NAICS codes and descriptions")
        call_command('load_naics')

        self.logger.info("Loading PSC codes and descriptions")
        call_command('load_psc')

        self.logger.info("Loading GTAS Total Obligation data")
        self.logger.warning("GTAS Total Obligation loader requires access to a broker database with the relevant data")
        call_command('load_gtas')

        self.logger.info("Reference data loaded.")
