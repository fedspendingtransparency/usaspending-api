from django.core.management.base import BaseCommand, CommandError
from usaspending_api.awards.models import Award
from datetime import datetime
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads contracts from a usaspending contracts download"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        try:
            with open(options['file'][0]) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        award, created = Award.objects.get_or_create(award_id=row['piid'], type='C')
                        # need to update existing instance, probably add as child transaction
                        # right now awards get overwritten -- this needs to be updated after table
                        # structure more finalized
                        updated_fields = {
                            'obligated_amount': row['dollarsobligated'],
                            'awarding_agency': row['maj_agency_cat'],
                            'date_signed': self.convert_date(row['signeddate']),
                            'recipient_name': row['vendorname']
                        }

                        for key, value in updated_fields.items():
                            setattr(award, key, value)

                        award.save()

                    except django.db.utils.IntegrityError:
                        self.logger.log(20, "Could not insert duplicate award")

        except IOError:
            print("Please specify a file to load from")

    def convert_date(self, date):
        return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
