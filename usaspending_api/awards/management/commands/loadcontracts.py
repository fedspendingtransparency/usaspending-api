from django.core.management.base import BaseCommand, CommandError
from usaspending_api.awards.models import Award, Procurement
from usaspending_api.references.models import LegalEntity, Agency
from datetime import datetime
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads contracts from a usaspending contracts download. \
            Usage: `python manage.py loadcontracts source_file_path`"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        try:
            with open(options['file'][0]) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # try:
                    award, created = Award.objects.get_or_create(piid=row['piid'], type='C')
                    # need to update existing instance, probably add as child transaction
                    # right now awards get overwritten -- this needs to be updated after table
                    # structure more finalized
                    mod, created = Procurement.objects.get_or_create(piid=row['piid'],
                                                                     award_modification_amendment_number=row['modnumber'],
                                                                     award=award)

                    recipient, created = LegalEntity.objects.get_or_create(awardee_or_recipient_legal=row['dunsnumber'])
                    recipient_data = {
                        'vendor_doing_as_business_n': row['vendordoingasbusinessname'],
                    }
                    recipient.save()

                    awarding_agency = Agency.objects.get(fpds_code=self.get_agency_code(row['maj_agency_cat']))

                    updated_fields = {
                        'federal_action_obligation': row['dollarsobligated'],
                        'awarding_agency': awarding_agency,
                        'action_date': self.convert_date(row['signeddate']),
                        'recipient': recipient,
                        'award': award,
                        'description': row['descriptionofcontractrequirement'],
                    }

                    for key, value in updated_fields.items():
                        setattr(mod, key, value)

                    mod.save()

                    award.update_from_mod(mod)
                    # except django.db.utils.IntegrityError:
                    #    self.logger.log(20, "Could not insert duplicate award")

        except IOError:
            self.logger.log(20, "Please specify a file to load from")

    def convert_date(self, date):
        return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    def get_agency_code(self, maj_agency_cat):
        return maj_agency_cat.split(':')[0]
