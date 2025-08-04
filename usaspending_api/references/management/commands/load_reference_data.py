import logging

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Loads reference data into the database. This should be run after \
            creating a new database. This command should be run in the same \
            directory where manage.py is located"

    logger = logging.getLogger("script")

    def handle(self, *args, **options):
        self.logger.info("Beginning reference data loading. This may take a few minutes.")

        self.logger.info("Loading DEF Codes")
        call_command("load_disaster_emergency_fund_codes")

        self.logger.info("Loading reference_fixture.json")
        call_command("loaddata", "reference_fixture")

        self.logger.info("Loading agency list")
        call_command("load_agencies", settings.AGENCY_DOWNLOAD_URL, "--force")

        self.logger.info("Loading state data")
        call_command("load_state_data")

        self.logger.info("Loading object classes")
        call_command("load_object_classes")

        # TAS's should only be loaded after agencies to ensure they can properly link to agencies
        self.logger.info("Loading TAS")
        if settings.IS_LOCAL:
            call_command("load_tas", location="usaspending_api/data/tas_list.csv")
        else:
            call_command("load_tas")

        self.logger.info("Loading program_activity.csv")
        if settings.IS_LOCAL:
            call_command("load_program_activity", "usaspending_api/data/program_activity.csv")
        else:
            call_command("load_program_activity")

        self.logger.info("Loading ref_city_county_state_code")
        call_command(
            "load_city_county_state_code", "https://geonames.usgs.gov/docs/federalcodes/NationalFedCodes.zip", "--force"
        )

        self.logger.info("Loading CFDA data")
        call_command("loadcfda", "https://files.usaspending.gov/reference_data/cfda.csv")

        self.logger.info("Loading Census Population Data")
        call_command(
            "load_population_data",
            file="https://files.usaspending.gov/reference_data/census_2020_population_county.csv",
            type="county",
        )
        call_command(
            "load_population_data",
            file="https://files.usaspending.gov/reference_data/census_2021_population_congressional_district.csv",
            type="district",
        )
        call_command(
            "load_population_data",
            file="https://files.usaspending.gov/reference_data/latest_country_population_data.csv",
            type="country",
        )

        self.logger.info("Loading descriptions of commonly used terms")
        call_command("load_glossary")

        self.logger.info("Loading static data")
        call_command("load_broker_static_data")
        call_command("load_download_static_data")

        self.logger.info("Loading budget authority data")
        call_command("load_budget_authority")

        self.logger.info("Loading NAICS codes and descriptions")
        call_command("load_naics")

        self.logger.info("Loading PSC codes and descriptions")
        call_command("load_psc")

        self.logger.info("Loading GTAS Total Obligation data")
        self.logger.warning("GTAS Total Obligation loader requires access to a broker database with the relevant data")
        call_command("load_gtas")

        self.logger.info("Loading DABS Submission Schedule Windows")
        call_command(
            "load_dabs_submission_window_schedule", file="usaspending_api/data/dabs_submission_window_schedule.csv"
        )

        self.logger.info("Reference data loaded.")
