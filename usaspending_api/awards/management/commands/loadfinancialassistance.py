from django.core.management.base import BaseCommand, CommandError
from usaspending_api.awards.models import Award, FinancialAssistanceAward
from usaspending_api.references.models import LegalEntity, Agency, Location
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from datetime import datetime
import csv
import logging
import django

"""

No PIID in the download from https://www.usaspending.gov/DownloadCenter/Pages/dataarchives.aspx ?

"""

class Command(BaseCommand):
    help = "Loads contracts from a usaspending financial assistance download. \
            Usage: `python manage.py loadcontracts source_file_path`"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        # Create a new submission attributes object for this timestamp

        subattr = SubmissionAttributes()
        subattr.usaspending_update = datetime.now()
        subattr.save()

        # in input source : in our schema  WRONG
        # name in our schema: name in input source
        field_map = {
            "award_id": "federal_award_id",
            "federal_action_obligation": "fed_funding_amount",
            "non_federal_funding_amount": "non_fed_funding_amount",
            "cfda_number": "cfda_program_num",
            "cfda_title": "cfda_program_title",
            "uri": "unique_transaction_id",
            "face_value_loan_guarantee": "face_loan_guran",
            "original_loan_subsidy_cost": "orig_sub_guran", # ??
            "award_description": "project_description", # ??
        }

        # TODO: input file tracks recipient's location only as far as country & state,
        # but legal_entity joins to Location, much more precise

        # TODO: these are all going to need to be normalized/cleansed,
        # or else multiple values will appear in all the parent tables

        # TODO: csv contains `exec1_amount`... `exec5_amount` and
        # `exec1_fullname`... `exec5_fullname`.  We seem to be ignoring those.

        # TODO: What are all the drv_ fields in financial_assistance_award?

        value_map = {
            "data_source": "USA",
            "submitted_type": "C", # ?? For CSV?
            "award": lambda row: Award.objects.get_or_create(fain=row['federal_award_id'], type='F')[0], # this is a guess!
            "recipient": lambda row: LegalEntity.objects.get_or_create(
                recipient_name=row['recipient_name'],
                location=self.get_recipient_location(row),
                # TODO: row['recip_cat_type'],
                )[0],
                # TODO: get recipient.for_profit_organization, etc. from recipient_type
            "location": self.get_location,
            "awarding_agency": lambda row: Agency.objects.get(subtier_code=self.get_agency_code(self.pre_colon(row['maj_agency_cat']))),
            "action_date": lambda row: self.convert_date(row['obligation_action_date']),  # FIXED
            "last_modified_date": lambda row: self.convert_date(row['last_modified_date']),
            "gfe_gfp": lambda row: self.pre_colon(row['gfe_gfp']),
            "action_type": lambda row: self.pre_colon(row['action_type']),
            "assistance_type": lambda row: self.pre_colon(row['assistance_type']),
            "record_type": lambda row: int(self.pre_colon(row['record_type'])),
            "submission": subattr
        }

        loader = ThreadedDataLoader(FinancialAssistanceAward, field_map=field_map, value_map=value_map)
        loader.load_from_file(options['file'][0])

    def convert_date(self, date):
        return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    def get_agency_code(self, maj_agency_cat):
        return maj_agency_cat.split(':')[0]

    def pre_colon(self, val):
        return val.split(':')[0]

    def get_recipient_location(self, row):
        result = Location.objects.get_or_create(
          location_country_name=row['recipient_country_code'],
          location_state_code=row['recipient_state_code'],
          )
        return result[0]

    def get_location(self, row):
        result = Location.objects.get_or_create(
          location_country_name=row['principle_place_country_code'],
          location_state_name=row['principle_place_state'],
          location_state_code=row['principle_place_state_code'],
          location_county_name=row['principle_place_cc'],
          location_zip4=row['principal_place_zip'],
        )
        return result[0]
