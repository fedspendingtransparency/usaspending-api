import logging
from datetime import datetime

from django.core.management.base import BaseCommand

import usaspending_api.awards.management.commands.helpers as h
from usaspending_api.awards.models import Award, FinancialAssistanceAward
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from usaspending_api.references.models import Agency, LegalEntity
from usaspending_api.submissions.models import SubmissionAttributes


class Command(BaseCommand):
    help = "Loads awards from a usaspending financial assistance download. \
            Usage: `python manage.py loadcontracts source_file_path`"

    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        # Create a new submission attributes object for this timestamp

        subattr = SubmissionAttributes()
        subattr.usaspending_update = datetime.now()
        subattr.save()

        # field_map is used to simply translate a column name in our schema
        # to the field name in the input source
        field_map = {
            "federal_action_obligation": "fed_funding_amount",
            "non_federal_funding_amount": "non_fed_funding_amount",
            "cfda_number": "cfda_program_num",
            "cfda_title": "cfda_program_title",
            "face_value_loan_guarantee": "face_loan_guran",
            "original_loan_subsidy_cost": "orig_sub_guran",  # ??
            "award_description": "project_description",  # ??
            "fiscal_year_and_quarter_correction": "fyq_correction",
            "usaspending_unique_transaction_id": "unique_transaction_id",
            # uri in the CSV is often empty, so not useful
        }

        # TODO: csv contains `exec1_amount`... `exec5_amount` and
        # `exec1_fullname`... `exec5_fullname`.  We seem to be ignoring those.

        # TODO: What are all the drv_ fields in financial_assistance_award?

        # progsrc - program source?
        # I’ve also got a “progsrc_acnt_code”, “progsrc_agen_code”, “progsrc_subacnt_code”, which I’m guessing stands for Program Source, and corresponds to… Funding Agency as opposed to Awarding Agency?
        # none of those really look like enough to identify an agency
        # how to identify the funding agency?

        value_map = {
            "data_source": "USA",
            "submitted_type": "C",  # ?? For CSV?
            "correction_late_delete_indicator":
            lambda row: h.up2colon(row['correction_late_ind']),
            "recipient": self.get_or_create_recipient,
            "award": self.get_or_create_award,
            "place_of_performance":
            lambda row: h.get_or_create_location(row, location_mapper_fin_assistance_principal_place),
            "awarding_agency": self.get_awarding_agency,
            "assistance_type": lambda row: h.up2colon(row['assistance_type']),
            "record_type": lambda row: int(h.up2colon(row['record_type'])),
            "action_type": lambda row: h.up2colon(row['action_type']),
            "action_date":
            lambda row: h.convert_date(row['obligation_action_date']),
            "last_modified_date":
            lambda row: h.convert_date(row['last_modified_date']),
            "transaction_number":
            lambda row: self.parse_first_character(row['transactionnumber']),
            "solicitation_identifier":
            lambda row: self.parse_first_character(row['solicitationid']),
            "submission": subattr,
            "period_of_performance_start_date":
            lambda row: h.convert_date(row['starting_date']),
            "period_of_performance_current_end_date":
            lambda row: h.convert_date(row['ending_date']),
        }

        loader = ThreadedDataLoader(
            FinancialAssistanceAward, field_map=field_map, value_map=value_map,
            collision_field='usaspending_unique_transaction_id',
            collision_behavior='skip', )
        loader.load_from_file(options['file'][0])

    def get_or_create_award(self, row):
        fain = row.get("fain", None)
        uri = row.get("unique_transaction_id", None)
        award = Award.get_or_create_summary_award(fain=fain, uri=uri)
        return award

    def recipient_flags_by_type(self, type_name):
        """Translates `type_name` to a T in the appropriate flag value.

        Assumes that all other flags are null.
        """

        flags = {}

        mappings = {
            'state government': 'us_state_government',
            'county government': 'county_local_government',
            'city or township government': 'city_township_government',
            # or city_local_government, county_local_government, municipality_local_government ?
            'special district government': 'special_district_government',
            'independent school district': 'school_district_local_government',
            'state controlled institution of higher education':
            'educational_institution',
            'indian tribe': 'us_tribal_government',
            # or indian_tribe_federally_recognized ?
            'other nonprofit': 'nonprofit_organization',
            'private higher education': 'educational_institution',
            'individual': 'individual',
            'profit organization': 'for_profit_organization',
            'small business': 'small_business',
            # should the more specific contract flags for small businesses also set this to y?
            'all other': '',
        }
        flag = mappings.get(type_name.lower(), "")
        if flag:
            flags[flag] = 'Y'
        else:
            self.logger.error('No known column for recipient_type {}'.format(
                type_name))
        return flags

    def get_or_create_recipient(self, row):
        recipient_dict = {
            "location_id": h.get_or_create_location(
                row,
                mapper=location_mapper_fin_assistance_recipient).location_id,
            "recipient_name": row['recipient_name'],
            "recipient_unique_id": row['duns_no'],
        }

        recipient_type = row.get("recipient_type", ":").split(":")[1].strip()
        recipient_dict.update(self.recipient_flags_by_type(recipient_type))

        le = LegalEntity.objects.filter(
            recipient_unique_id=row['duns_no']).first()
        if not le:
            le = LegalEntity.objects.create(**recipient_dict)

        return le

    def get_awarding_agency(self, row):
        toptier_code = h.up2colon(row['maj_agency_cat'])
        subtier_code = h.up2colon(row['agency_code'])
        return self.get_agency(toptier_code, subtier_code)

    def get_agency(self, toptier_code, subtier_code):
        agency = Agency.objects.filter(
            subtier_agency__subtier_code=subtier_code).filter(
                toptier_agency__fpds_code=toptier_code).first()
        if not agency:
            self.logger.error("Missing agency: {} {}".format(toptier_code,
                                                             subtier_code))
        return agency


def location_mapper_fin_assistance_principal_place(row):
    loc = {
        "location_county_name": row.get("principal_place_cc", ""),
        "location_country_code": row.get("principal_place_country_code", ""),
        "location_zip": row.get("principal_place_zip", "").replace(
            "-", ""),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "location_state_code": row.get("principal_place_state_code", ""),
        "location_state_name": row.get("principal_place_state", ""),
    }
    return loc


def location_mapper_fin_assistance_recipient(row):
    loc = {
        "location_county_code": row.get("recipient_county_code", ""),
        "location_county_name": row.get("recipient_county_name", ""),
        "location_country_code": row.get("recipient_country_code", ""),
        "location_city_code": row.get("recipient_city_code"
                                      ""),
        "location_city_name": row.get("recipient_city_name"
                                      ""),
        "location_zip": row.get("recipient_zip", "").replace(
            "-", ""),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "location_state_code": row.get("recipient_state_code"),
        "location_address_line1": row.get("receip_addr1"),
        "location_address_line2": row.get("receip_addr2"),
        "location_address_line3": row.get("receip_addr3"),
    }
    return loc
