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
                      'usaspending_api/references/management/commands/agency_list.csv')) \
                      as csvfile:

                reader = csv.DictReader(csvfile)
                for row in reader:
                    fpds_code = row.get('FPDS DEPARTMENT ID', '')
                    cgac_code = row.get('CGAC AGENCY CODE', '')
                    department_name = row.get('AGENCY NAME', '')
                    subtier_name = row.get('SUB TIER NAME', '')
                    subtier_code = row.get('SUB TIER CODE', '')

                    toptier_agency = None
                    subtier_agency = None

                    # Toptier agency
                    toptier_agency, created = ToptierAgency.objects.get_or_create(cgac_code=cgac_code,
                                                                                  fpds_code=fpds_code,
                                                                                  name=department_name)

                    subtier_agency, created = SubtierAgency.objects.get_or_create(subtier_code=subtier_code,
                                                                                  name=subtier_name)

                    # Create new summary agency object
                    agency, created = Agency.objects.get_or_create(toptier_agency=toptier_agency,
                                                                   subtier_agency=subtier_agency)

                    # self.logger.log(20, "loaded %s" % agency)

        except IOError:
            self.logger.log("Could not open file to load from")
