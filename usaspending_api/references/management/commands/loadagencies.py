from django.core.management.base import BaseCommand, CommandError
from usaspending_api.references.models import ToptierAgency, SubtierAgency, OfficeAgency
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
                    toptier_agency, created = ToptierAgency.objects.get_or_create(
                                    cgac_code=row['CGAC AGENCY CODE'])

                    toptier_agency.name = row['AGENCY NAME']

                    toptier_agency.save()

                    subtier_agency, created = SubtierAgency.objects.get_or_create(
                                    subtier_code=row['SUB TIER CODE'])

                    subtier_agency.name = row['SUB TIER NAME']

                    subtier_agency.save()

                    # self.logger.log(20, "loaded %s" % agency)

        except IOError:
            self.logger.log("Could not open file to load from")
