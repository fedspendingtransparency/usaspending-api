import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction
from django.db.models import Max

from usaspending_api.awards.models import Award, Subaward
from usaspending_api.references.models import LegalEntity, Agency
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.helpers import get_or_create_location

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

QUERY_LIMIT = 10000


class Command(BaseCommand):
    help = "Load new FSRS data from broker."

    def get_award_data(self, db_cursor, award_type, max_id):
        """ Gets data for all new awards from broker with ID greater than the ones already stored for the given 
            award type
        """
        query_columns = ['internal_id', 'contract_number', 'idv_reference_number', 'contracting_office_aid', 'duns',
                         'parent_duns', 'company_name', 'company_address_country', 'company_address_city',
                         'company_address_zip', 'company_address_state', 'company_address_street',
                         'company_address_district', 'principle_place_country', 'principle_place_city',
                         'principle_place_zip', 'principle_place_state', 'principle_place_street',
                         'principle_place_district']
        query = "SELECT " + ",".join(query_columns) + " FROM fsrs_" + award_type +\
                " WHERE id > " + str(max_id) + " ORDER BY id"

        db_cursor.execute(query)

        logger.info("Running dictfetchall on db_cursor")
        return dictfetchall(db_cursor)

    def gather_shared_award_data(self, data, award_type):
        """ Creates a dictionary with internal IDs as keys that stores data for each award that's shared between all
            its subawards so we don't have to query the DB repeatedly
        """
        shared_data = {}
        logger.info("Starting shared data gathering")
        counter = 0
        new_awards = len(data)
        for row in data:
            counter += 1
            if counter % 1000 == 0:
                logger.info("Processed " + str(counter) + " of " + str(new_awards) + " new awards")

            agency = get_valid_awarding_agency(row)

            if not agency:
                logger.warning(
                    "Internal ID {} cannot find matching agency with subtier code {}".
                    format(row['internal_id'], row['contracting_office_aid']))
                continue

            # Find the award to attach this sub-contract to. We perform this lookup by finding the Award containing a
            # transaction with a matching agency, parent award id, and piid
            award = Award.objects.filter(
                awarding_agency=agency,
                latest_transaction__contract_data__piid=row['contract_number'],
                latest_transaction__contract_data__parent_award_id=row['idv_reference_number']).distinct().order_by(
                "-date_signed").first()

            # We don't have a matching award for this subcontract, log a warning and continue to the next row
            if not award:
                logger.warning(
                    "Internal ID {} cannot find award with piid {}, parent_award_id {}; skipping...".
                    format(row['internal_id'], row['contract_number'], row['idv_reference_number']))
                continue

            # Get or create unique DUNS-recipient pair
            recipient, created = LegalEntity.objects.get_or_create(
                recipient_unique_id=row['duns'],
                recipient_name=row['company_name']
            )

            if created:
                recipient.parent_recipient_unique_id = row['parent_duns']
                recipient.location = get_or_create_location(row, location_d1_recipient_mapper)
                recipient.save()

            # Get or create POP
            place_of_performance = get_or_create_location(row, pop_mapper)

            # set shared data content
            shared_data[row['internal_id']] = {'award': award,
                                               'recipient': recipient,
                                               'place_of_performance': place_of_performance}
        logger.info("Completed shared data gathering")

        return shared_data

    def gather_next_subawards(self, db_cursor, award_type, subaward_type, max_id, offset):
        """ Get next batch of subawards of the relevant type starting at a given offset """
        query_columns = ['award.internal_id', 'award.id', 'sub_award.subcontract_num', 'sub_award.subcontract_amount',
                         'sub_award.overall_description', 'sub_award.recovery_model_q1', 'sub_award.recovery_model_q2',
                         'sub_award.subcontract_date', 'award.report_period_mon', 'award.report_period_year']

        query = "SELECT " + ",".join(query_columns) + " FROM fsrs_" + award_type + " AS award " + \
                "JOIN fsrs_" + subaward_type + " AS sub_award ON sub_award.parent_id = award.id WHERE award.id > " + \
                str(max_id) + " ORDER BY award.id, sub_award.id LIMIT " + str(QUERY_LIMIT) + " OFFSET " + str(offset)

        db_cursor.execute(query)

        return dictfetchall(db_cursor)

    def process_subawards(self, db_cursor, shared_award_mappings, award_type, subaward_type, max_id):
        """ Process the subawards and insert them as we go """
        current_offset = 0

        subaward_dict = self.gather_next_subawards(db_cursor, award_type, subaward_type, max_id, current_offset)

        # run as long as we get any results for the subawards, stop as soon as we run out
        while len(subaward_dict) > 0:
            logger.info("Processing next 10,000 subawards, starting at offset: " + str(current_offset))
            for row in subaward_dict:
                # only insert the subaward if the internal_id is in our mappings, otherwise there was a problem
                # finding one or more parts of the shared data for it and we don't want to insert it.
                if row['internal_id'] in shared_award_mappings:
                    shared_mappings = shared_award_mappings[row['internal_id']]

                    procurement_dict = {
                        'award': shared_mappings['award'],
                        'recipient': shared_mappings['recipient'],
                        'data_source': "DBR",
                        'cfda': None,
                        'awarding_agency': shared_mappings['award'].awarding_agency,
                        'funding_agency': shared_mappings['award'].funding_agency,
                        'place_of_performance': shared_mappings['place_of_performance'],
                        'subaward_number': row['subcontract_num'],
                        'amount': row['subcontract_amount'],
                        'description': row['overall_description'],
                        'recovery_model_question1': row['recovery_model_q1'],
                        'recovery_model_question2': row['recovery_model_q2'],
                        'action_date': row['subcontract_date'],
                        'award_report_fy_month': row['report_period_mon'],
                        'award_report_fy_year': row['report_period_year'],
                        'broker_award_id': row['id'],
                        'internal_id': row['internal_id'],
                        'award_type': award_type
                    }

                    # Either we're starting with an empty table in regards to this award type or we've deleted all
                    # subawards related to the internal_id, either way we just create the subaward
                    Subaward.objects.create(**procurement_dict)
            current_offset += QUERY_LIMIT
            subaward_dict = self.gather_next_subawards(db_cursor, award_type, subaward_type, max_id, current_offset)

    def process_award_type(self, db_cursor, award_type, subaward_type):
        """ Do all the processing for the award type given """

        max_id_query = Subaward.objects.filter(award_type=award_type).aggregate(Max('broker_award_id'))
        max_id = max_id_query['broker_award_id__max']
        # set max ID to 0 if we don't have any relevant records
        if not max_id:
            max_id = 0

        fsrs_award_data = self.get_award_data(db_cursor, award_type, max_id)
        shared_award_mappings = self.gather_shared_award_data(fsrs_award_data, award_type)

        # if this is not the initial load, delete all existing subawards with internal IDs matching the list of
        # new ones because they're all updated every time any change is made on broker
        if max_id > 0:
            internal_ids = list(shared_award_mappings)
            Subaward.objects.filter(internal_id__in=internal_ids, award_type=award_type).delete()

        self.process_subawards(db_cursor, shared_award_mappings, award_type, subaward_type, max_id)

    @db_transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting FSRS data load...')

        db_cursor = connections['data_broker'].cursor()

        logger.info('Get Broker FSRS procurement data...')
        self.process_award_type(db_cursor, "procurement", "subcontract")

        logger.info('Completed FSRS data load...')


def get_valid_awarding_agency(row):
    agency = None

    agency_subtier_code = row['contracting_office_aid']
    valid_subtier_code = (agency_subtier_code and len(agency_subtier_code) > 0)

    # Get the awarding agency
    if valid_subtier_code:
        agency = Agency.get_by_subtier(agency_subtier_code)

    return agency


def location_d1_recipient_mapper(row):
    loc = {
        "location_country_code": row.get("company_address_country", ""),
        "city_name": row.get("company_address_city", ""),
        "location_zip": row.get("company_address_zip", ""),
        "state_code": row.get("company_address_state"),
        "address_line1": row.get("company_address_street"),
        "congressional_code": row.get("company_address_district", None)
    }
    return loc


def pop_mapper(row):
    loc = {
        "location_country_code": row.get("principle_place_country", ""),
        "city_name": row.get("principle_place_city", ""),
        "location_zip": row.get("principle_place_zip", ""),
        "state_code": row.get("principle_place_state"),
        "address_line1": row.get("principle_place_street"),
        "congressional_code": row.get("principle_place_district", None)
    }
    return loc
