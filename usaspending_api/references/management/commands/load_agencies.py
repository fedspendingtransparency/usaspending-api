from django.core.management.base import BaseCommand, CommandError
from usaspending_api.references.models import ToptierAgency, SubtierAgency, OfficeAgency, Agency
import os
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads agencies and sub-tier agencies from authoritative OMB list in \
            the folder of this management command."

    logger = logging.getLogger('console')

    def handle(self, *args, **options):

        try:
            with open(os.path.join(django.conf.settings.BASE_DIR,
                      'usaspending_api', 'data', 'authoritative_agency_list.csv'), encoding="Latin-1") \
                    as csvfile:

                reader = csv.DictReader(csvfile)
                counter = 0
                for row in reader:
                    fpds_code = row.get('FPDS DEPARTMENT ID', '')
                    cgac_code = row.get('CGAC AGENCY CODE', '')
                    frec_code = row.get('FREC', '')
                    department_name = row.get('AGENCY NAME', '')
                    department_abbr = row.get('AGENCY ABBREVIATION', '')
                    subtier_name = row.get('SUBTIER NAME', '')
                    subtier_code = row.get('SUBTIER CODE', '')
                    subtier_abbr = row.get('SUBTIER ABBREVIATION', '')
                    mission = row.get('MISSION', '')
                    website = row.get('WEBSITE', '')
                    icon_filename = row.get('ICON FILENAME', '')

                    toptier_agency = None
                    subtier_agency = None

                    # Toptier agency
                    # First, see if we have a toptier agency that matches our fpds and cgac codes
                    # We use only these codes here to make sure we are idempotent with previous
                    # versions of this agency loader
                    toptier_agency, created = ToptierAgency.objects.get_or_create(
                        cgac_code=cgac_code,
                        fpds_code=fpds_code,
                        frec_code=frec_code)

                    toptier_flag = (subtier_name == department_name)

                    # Set or update the department name and abbreviation
                    toptier_agency.name = department_name
                    toptier_agency.abbreviation = department_abbr

                    if toptier_flag:
                        toptier_agency.mission = mission
                        toptier_agency.website = website
                        toptier_agency.icon_filename = icon_filename

                    toptier_agency.save()

                    # Do the same with the subtier that exists, but only on subtier code
                    # Only do this if subtier_code is not "Unknown"
                    if "unknown" not in subtier_code.lower():
                        subtier_agency, created = SubtierAgency.objects.get_or_create(subtier_code=subtier_code)
                        subtier_agency.name = subtier_name
                        subtier_agency.abbreviation = subtier_abbr
                        subtier_agency.save()

                    # Create new summary agency object
                    # Top tier flag is set after to insure variable is idempotent.
                    agency, created = Agency.objects.get_or_create(toptier_agency=toptier_agency,
                                                                   subtier_agency=subtier_agency)
                    agency.toptier_flag = toptier_flag
                    agency.save()

        except IOError:
            self.logger.log("Could not open file to load from")
