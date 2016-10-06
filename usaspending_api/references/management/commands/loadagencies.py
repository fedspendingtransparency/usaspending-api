from django.core.management.base import BaseCommand, CommandError
from usaspending_api.references.models import Agency
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
                    agency, created = Agency.objects.get_or_create(
                                    cgac_code=row['CGAC AGENCY CODE'],
                                    fpds_code=row['FPDS DEPARTMENT ID'],
                                    subtier_code=row['SUB TIER CODE'])

                    agency.name = row['AGENCY NAME']

                    if agency.subtier_code != agency.fpds_code:
                        # it's not department level
                        department = Agency.objects.get(
                                    cgac_code=agency.cgac_code,
                                    fpds_code=agency.fpds_code,
                                    subtier_code=agency.fpds_code)

                        agency.department = department
                        agency.name = row['SUB TIER NAME']

                    agency.save()

                    #self.logger.log(20, "loaded %s" % agency)

        except IOError:
            self.logger.log("Could not open file to load from")
