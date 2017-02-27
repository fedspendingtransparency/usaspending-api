import logging
from datetime import datetime

from django.core.management.base import BaseCommand

from usaspending_api.awards.models import Award, AWARD_TYPES, Transaction, TransactionAssistance
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards
from usaspending_api.etl.csv_data_reader import CsvDataReader
import usaspending_api.etl.helpers as h
from usaspending_api.references.models import Agency, LegalEntity
from usaspending_api.submissions.models import SubmissionAttributes


class Command(BaseCommand):
    help = "Loads awards from a usaspending financial assistance download. \
            Usage: `python manage.py load_usaspending_assistance source_file_path`"

    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):

        csv_file = options['file'][0]
        self.logger.info("Starting load for file {}".format(csv_file))

        # Create the csv reader
        reader = CsvDataReader(csv_file)

        # Create a new submission attributes object for this timestamp
        subattr = SubmissionAttributes()
        subattr.usaspending_update = datetime.now()
        subattr.save()

        # Create lists to hold model instances for bulk insert
        txn_list = []
        txn_assistance_list = []

        # Store some additional support data needed for the laod
        award_type_dict = {a[0]: a[1] for a in AWARD_TYPES}

        for idx, row in enumerate(reader):
            if len(reader) % 1000 == 0:
                self.logger.info("Read row {}".format(len(reader)))
            row = h.cleanse_values(row)

            awarding_agency = self.get_awarding_agency(row)  # todo: use agency dict?

            # Create the transaction object for this row
            txn_dict = {
                "submission": subattr,
                "action_date": h.convert_date(row['obligation_action_date']),
                "action_type": h.up2colon(row['action_type']),
                "award": self.get_or_create_award(row, awarding_agency=awarding_agency),
                "awarding_agency": awarding_agency,
                "description": row["project_description"],  # ?? account_title is anther contender?
                "data_source": "USA",
                "federal_action_obligation": row["fed_funding_amount"],
                "last_modified_date": h.convert_date(row['last_modified_date']),
                "modification_number": row["federal_award_mod"],  # ??
                "period_of_performance_start_date": h.convert_date(row['starting_date']),
                "period_of_performance_current_end_date": h.convert_date(row['ending_date']),
                "place_of_performance": h.get_or_create_location(row, location_mapper_fin_assistance_principal_place),
                "recipient": self.get_or_create_recipient(row),
                "type": h.up2colon(row['assistance_type']),
                "type_description": award_type_dict.get(h.up2colon(row['assistance_type'])),
                "usaspending_unique_transaction_id": row["unique_transaction_id"],

                # ??"funding_agency_id":
                # ?? "certified date":

            }
            txn = Transaction(**txn_dict)
            txn_list.append(txn)

            # Create the transaction contract object for this row
            txn_assistance_dict = {
                "submission": subattr,
                "fain": row["federal_award_id"],
                "uri": row["uri"],
                "cfda_number": row["cfda_program_num"],
                "cfda_title": row["cfda_program_title"],
                "correction_late_delete_indicator": h.up2colon(row['correction_late_ind']),
                "face_value_loan_guarantee": row["face_loan_guran"],
                "fiscal_year_and_quarter_correction": row["fyq_correction"],
                "non_federal_funding_amount": row["non_fed_funding_amount"],
                "original_loan_subsidy_cost": row["orig_sub_guran"],  # ??
                "record_type": int(h.up2colon(row['record_type'])),
                "sai_number": row["sai_number"],
                "submitted_type": "C",  # ?? For CSV?
            }
            # ?? business_funds_indicator
            # ?? reporting period start/end??

            txn_assistance = TransactionAssistance(**txn_assistance_dict)
            txn_assistance_list.append(txn_assistance)

        # Bulk insert transaction rows
        self.logger.info("Starting Transaction bulk insert ({} records)".format(len(txn_list)))
        Transaction.objects.bulk_create(txn_list)
        self.logger.info("Completed Transaction bulk insert")
        # Update txn assistance list with newly-inserted transactions
        award_id_list = []  # we'll need this when updating the awards later on
        for idx, t in enumerate(txn_assistance_list):
            t.transaction = txn_list[idx]
            award_id_list.append(txn_list[idx].award_id)
        # Bulk insert transaction assistance rows
        self.logger.info("Starting TransactionAssistance bulk insert ({} records)".format(len(txn_assistance_list)))
        TransactionAssistance.objects.bulk_create(txn_assistance_list)
        self.logger.info("Completed TransactionAssistance bulk insert")

        # Update awards to reflect latest transaction information
        # (note that this can't be done via signals or a save()
        # override in the model itself, because those aren't
        # triggered by a bulk update
        self.logger.info("Starting Awards update")
        count = update_awards(tuple(award_id_list))
        update_contract_awards(tuple(award_id_list))
        self.logger.info("Completed Awards update ({} records)".format(count))

    def get_or_create_award(self, row, awarding_agency):
        fain = row.get("federal_award_id", None)
        uri = row.get("unique_transaction_id", None)  # ask: why unique_transaction_id instead of uri?
        award = Award.get_or_create_summary_award(fain=fain, uri=uri, awarding_agency=awarding_agency)
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
        "county_name": row.get("principal_place_cc", ""),
        "location_country_code": row.get("principal_place_country_code", ""),
        "location_zip": row.get("principal_place_zip", "").replace(
            "-", ""),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "state_code": row.get("principal_place_state_code", ""),
        "state_name": row.get("principal_place_state", ""),
    }
    return loc


def location_mapper_fin_assistance_recipient(row):
    loc = {
        "county_code": row.get("recipient_county_code", ""),
        "county_name": row.get("recipient_county_name", ""),
        "location_country_code": row.get("recipient_country_code", ""),
        "city_code": row.get("recipient_city_code"
                             ""),
        "city_name": row.get("recipient_city_name"
                             ""),
        "location_zip": row.get("recipient_zip", "").replace(
            "-", ""),  # Either ZIP5, or ZIP5+4, sometimes with hypens
        "state_code": row.get("recipient_state_code"),
        "address_line1": row.get("receip_addr1"),
        "address_line2": row.get("receip_addr2"),
        "address_line3": row.get("receip_addr3"),
    }
    return loc
