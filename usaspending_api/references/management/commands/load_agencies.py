from django.core.management.base import BaseCommand, CommandError
from usaspending_api.references.models import Agency, SubtierAgency
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
                                    cgac_code=row['CGAC AGENCY CODE'])

                    agency.name = row['AGENCY NAME']
                    agency.fpds_code = row['FPDS DEPARTMENT ID']
                    agency.save()

                    self.logger.log(20, "loaded %s" % agency)

                    subtier, created = SubtierAgency.objects.get_or_create(
                                    code=row['SUB TIER CODE'],
                                    name=row['SUB TIER NAME'])

                    subtier.agency = agency
                    subtier.save()

                    self.logger.log(20, "loaded %s" % subtier)

        except IOError:
            self.logger.log("Could not open file to load from")
