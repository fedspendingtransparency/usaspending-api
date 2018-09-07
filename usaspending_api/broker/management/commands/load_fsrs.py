import logging
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction
from django.db.models import F, Func, Max, Value

from usaspending_api.awards.models import Award, Subaward
from usaspending_api.recipient.models import RecipientLookup
from usaspending_api.common.helpers.generic_helper import upper_case_dict_values
from usaspending_api.references.models import Agency, Cfda, Location
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.award_helpers import update_award_subawards

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

QUERY_LIMIT = 10000
LOG_LIMIT = 10000

award_update_id_list = []


class Command(BaseCommand):
    help = "Load new FSRS data from broker."

    @staticmethod
    def get_award_data(db_cursor, award_type, max_id, internal_ids=None):
        """ Gets data for all new awards from broker with ID greater than the ones already stored for the given
            award type
        """
        query_columns = ['internal_id']

        # we need different columns depending on if it's a procurement or a grant
        if award_type == 'procurement':
            query_columns.extend(['contract_number', 'idv_reference_number', 'contracting_office_aid',
                                  'contract_agency_code', 'contract_idv_agency_code'])
        else:
            # TODO contracting_office_aid equivalent? Do we even need it?
            query_columns.extend(['fain'])

        if isinstance(internal_ids, list) and len(internal_ids) > 0:
            ids_string = ','.join([str(id).lower() for id in internal_ids])
            query = "SELECT " + ",".join(query_columns) + " FROM fsrs_" + award_type +\
                    " WHERE internal_id = ANY(\'{{{}}}\'::text[]) ORDER BY id".format(ids_string)
        else:
            query = "SELECT " + ",".join(query_columns) + " FROM fsrs_" + award_type + \
                    " WHERE id > " + str(max_id) + " ORDER BY id"

        db_cursor.execute(query)

        logger.info("Running dictfetchall on db_cursor")
        return dictfetchall(db_cursor)

    @staticmethod
    def generate_unique_ids(row, award_type):
        if award_type == 'procurement':
            # "CONT_AW_" + agency_id + referenced_idv_agency_iden + piid + parent_award_id
            # "CONT_AW_" + contract_agency_code + contract_idv_agency_code + contract_number + idv_reference_number
            return \
                'CONT_AW_' + \
                (row['contract_agency_code'].replace('-', '') if row['contract_agency_code'] else '-NONE-') + \
                '_' + \
                (row['contract_idv_agency_code'].replace('-', '') if row['contract_idv_agency_code'] else '-NONE-') + \
                '_' + \
                (row['contract_number'].replace('-', '') if row['contract_number'] else '-NONE-') + \
                '_' + \
                (row['idv_reference_number'].replace('-', '') if row['idv_reference_number'] else '-NONE-')
        else:
            # For assistance awards, 'ASST_AW_' is NOT prepended because we are unable to build the full unique
            # identifier since all the required fields to do so are not provided by the subaward tables from the source.
            # Therefore, we can only use the FAIN to find the closest match instead of generated_unique_award_id.
            return row['fain'].replace('-', '')

    def get_award(self, row, award_type):
        if award_type == 'procurement':
            # we don't need the agency for grants
            agency = get_valid_awarding_agency(row)

            if not agency:
                logger.warning(
                    "Internal ID {} cannot find matching agency with subtier code {}".
                    format(row['internal_id'], row['contracting_office_aid']))
                return None

            # Find the award to attach this sub-contract to, using the generated unique ID:
            award = Award.objects.annotate(
                modified_generated_unique_award_id=Func(
                    F('generated_unique_award_id'),
                    Value('-'), Value(''),
                    function='replace',
                )
            ).\
                filter(modified_generated_unique_award_id=self.generate_unique_ids(row, award_type),
                       latest_transaction_id__isnull=False).\
                distinct().order_by("-date_signed").first()

            # We don't have a matching award for this subcontract, log a warning and continue to the next row
            if not award:
                logger.warning(
                   "Internal ID {} cannot find award with agency_id {}, referenced_idv_agency_iden {}, piid {}, "
                   "parent_award_id {};".format(row['internal_id'], row['contract_agency_code'],
                                                row['contract_idv_agency_code'], row['contract_number'],
                                                row['idv_reference_number']))
                return None
        else:
            # Find the award to attach this sub-contract to. We perform this lookup by finding the Award containing
            # a transaction with a matching fain
            all_awards = Award.objects.annotate(
                modified_fain=Func(
                    F('fain'),
                    Value('-'), Value(''),
                    function='replace',
                )
            ).\
                filter(modified_fain=self.generate_unique_ids(row, award_type), latest_transaction_id__isnull=False).\
                distinct().order_by("-date_signed")

            if all_awards.count() > 1:
                logger.warning("Multiple awards found with fain {}".format(row['fain']))

            award = all_awards.first()

            # We don't have a matching award for this subgrant, log a warning and continue to the next row
            if not award:
                logger.warning(
                    "Internal ID {} cannot find award with fain {};".
                    format(row['internal_id'], row['fain']))
                return None
        return award

    def gather_shared_award_data(self, data, award_type):
        """ Creates a dictionary with internal IDs as keys that stores data for each award that's shared between all
            its subawards so we don't have to query the DB repeatedly
            Note: subawards without a matching award will still be processed, just without a matching id which may be
                  matched later
        """
        shared_data = {}
        logger.info("Starting shared data gathering")
        counter = 0
        new_awards = len(data)
        logger.info(str(new_awards) + " new awards to process.")
        for row in data:
            counter += 1
            if counter % LOG_LIMIT == 0:
                logger.info(
                    "Processed " + str(counter) + " of " + str(new_awards) + " new awards."
                )
            award = self.get_award(row, award_type)

            # set shared data content
            shared_data[row['internal_id']] = {'award': award}
        logger.info("Completed shared data gathering")

        return shared_data

    @staticmethod
    def gather_next_subawards(db_cursor, award_type, subaward_type, max_id, offset):
        """ Get next batch of subawards of the relevant type starting at a given offset """
        query_columns = [
            'award.internal_id', 'award.id',
            'award.report_period_mon', 'award.report_period_year',
            'sub_award.duns AS duns', 'sub_award.parent_duns AS parent_duns',
            'sub_award.dba_name AS dba_name',
            'sub_award.principle_place_country AS principle_place_country',
            'sub_award.principle_place_city AS principle_place_city',
            'sub_award.principle_place_zip AS principle_place_zip',
            'sub_award.principle_place_state AS principle_place_state',
            'sub_award.principle_place_state_name AS principle_place_state_name',
            'sub_award.principle_place_street AS principle_place_street',
            'sub_award.principle_place_district AS principle_place_district',
            'sub_award.top_paid_fullname_1',
            'sub_award.top_paid_amount_1',
            'sub_award.top_paid_fullname_2',
            'sub_award.top_paid_amount_2',
            'sub_award.top_paid_fullname_3',
            'sub_award.top_paid_amount_3',
            'sub_award.top_paid_fullname_4',
            'sub_award.top_paid_amount_4',
            'sub_award.top_paid_fullname_5',
            'sub_award.top_paid_amount_5',
        ]

        # We need different columns depending on if it's a procurement or a grant. Setting some columns to have labels
        # so we can easily access them without making two different dictionaries.
        if award_type == 'procurement':
            query_columns.extend(['sub_award.subcontract_num AS subaward_num',
                                  'sub_award.subcontract_amount AS subaward_amount',
                                  'sub_award.overall_description', 'sub_award.recovery_model_q1 AS q1_flag',
                                  'sub_award.recovery_model_q2 AS q2_flag',
                                  'sub_award.subcontract_date AS subaward_date',
                                  'sub_award.company_name AS company_name',
                                  'sub_award.company_address_country AS company_address_country',
                                  'sub_award.company_address_city AS company_address_city',
                                  'sub_award.company_address_zip AS company_address_zip',
                                  'sub_award.company_address_state AS company_address_state',
                                  'sub_award.company_address_state_name AS company_address_state_name',
                                  'sub_award.company_address_street AS company_address_street',
                                  'sub_award.company_address_district AS company_address_district',
                                  'sub_award.parent_company_name AS parent_company_name',
                                  'sub_award.bus_types AS bus_types'
                                  ])

            query = "SELECT " + ",".join(query_columns) + " FROM fsrs_" + award_type + " AS award " + \
                    "JOIN fsrs_" + subaward_type +\
                    " AS sub_award ON sub_award.parent_id = award.id WHERE award.id > " + str(max_id) +\
                    " AND sub_award.subcontract_num IS NOT NULL ORDER BY award.id, sub_award.id LIMIT " + \
                    str(QUERY_LIMIT) + " OFFSET " + str(offset)
        else:
            query_columns.extend(['sub_award.cfda_numbers', 'sub_award.subaward_num', 'sub_award.subaward_amount',
                                  'sub_award.project_description AS overall_description',
                                  'sub_award.compensation_q1 AS q1_flag', 'sub_award.compensation_q2 AS q2_flag',
                                  'sub_award.subaward_date',
                                  'sub_award.awardee_name AS awardee_name',
                                  'sub_award.awardee_address_country AS awardee_address_country',
                                  'sub_award.awardee_address_city AS awardee_address_city',
                                  'sub_award.awardee_address_zip AS awardee_address_zip',
                                  'sub_award.awardee_address_state AS awardee_address_state',
                                  'sub_award.awardee_address_state_name AS awardee_address_state_name',
                                  'sub_award.awardee_address_street AS awardee_address_street',
                                  'sub_award.awardee_address_district AS awardee_address_district'])

            query = "SELECT " + ",".join(query_columns) + " FROM fsrs_" + award_type + " AS award " + \
                    "JOIN fsrs_" + subaward_type +\
                    " AS sub_award ON sub_award.parent_id = award.id WHERE award.id > " + \
                    str(max_id) + " AND sub_award.subaward_num IS NOT NULL ORDER BY award.id, sub_award.id LIMIT " + \
                    str(QUERY_LIMIT) + " OFFSET " + str(offset)

        db_cursor.execute(query)

        return dictfetchall(db_cursor)

    def create_subaward(self, row, shared_award_mappings, award_type):
        """ Creates a subaward if the internal ID of the current row is in the shared award mappings (this was made
            to satisfy codeclimate complexity issues)
        """

        # only insert the subaward if the internal_id is in our mappings, otherwise there was a problem
        # finding one or more parts of the shared data for it and we don't want to insert it.
        if row['internal_id'] in shared_award_mappings:
            shared_mappings = shared_award_mappings[row['internal_id']]

            prime_award_dict = {}
            if shared_mappings['award']:
                prime_award_dict['prime_recipient'] = shared_mappings['award'].recipient
                if prime_award_dict['prime_recipient']:
                    prime_award_dict['prime_recipient_name'] = shared_mappings['award'].recipient.recipient_name
                    prime_award_dict['business_categories'] = (
                        shared_mappings['award'].recipient.business_categories or []
                    )

            upper_case_dict_values(row)

            cfda = None
            # check if the key exists and if it isn't empty (only here for grants)
            if 'cfda_numbers' in row and row['cfda_numbers']:
                only_num = row['cfda_numbers'].split(' ')
                cfda = Cfda.objects.filter(program_number=only_num[0]).first()

            if award_type == 'procurement':
                le_location_map = location_d1_recipient_mapper(row)
                recipient_name = row['company_name']
                parent_recipient_name = row['parent_company_name']
                business_type_code = None
                business_types_description = row['bus_types']
            else:
                le_location_map = location_d2_recipient_mapper(row)
                recipient_name = row['awardee_name']
                parent_recipient_name = None
                business_type_code = None
                business_types_description = None

            if le_location_map["location_zip"]:
                le_location_map.update(
                    zip4=le_location_map["location_zip"],
                    zip5=le_location_map["location_zip"][:5],
                    zip_last4=le_location_map["location_zip"][5:]
                )

            le_location_map.pop("location_zip")
            recipient_location = Location(**le_location_map)
            recipient_location.pre_save()

            pop_value_map = pop_mapper(row)
            pop_value_map['place_of_performance_flag'] = True

            if pop_value_map["location_zip"]:
                pop_value_map.update(
                    zip4=pop_value_map["location_zip"],
                    zip5=pop_value_map["location_zip"][:5],
                    zip_last4=pop_value_map["location_zip"][5:]
                )

            pop_value_map.pop("location_zip")
            place_of_performance = Location(**pop_value_map)
            place_of_performance.pre_save()

            if not parent_recipient_name and row.get('parent_duns'):
                duns_obj = RecipientLookup.objects.filter(duns=row['parent_duns'], legal_business_name__isnull=False) \
                    .values('legal_business_name').first()
                if duns_obj:
                    parent_recipient_name = duns_obj['legal_business_name']

            subaward_dict = {
                'award': shared_mappings['award'],
                'recipient_unique_id': row['duns'],
                'recipient_name': recipient_name,
                'dba_name': row['dba_name'],
                'parent_recipient_unique_id': row['parent_duns'],
                'parent_recipient_name': parent_recipient_name,
                'business_type_code': business_type_code,
                'business_type_description': business_types_description,

                'prime_recipient': prime_award_dict.get('prime_recipient', None),
                'prime_recipient_name': prime_award_dict.get('prime_recipient_name', None),
                'business_categories': prime_award_dict.get('business_categories', []),

                'recipient_location_country_code': recipient_location.location_country_code,
                'recipient_location_country_name': recipient_location.country_name,
                'recipient_location_state_code': recipient_location.state_code,
                'recipient_location_state_name': recipient_location.state_name,
                'recipient_location_county_code': recipient_location.county_code,
                'recipient_location_county_name': recipient_location.county_name,
                'recipient_location_city_code': recipient_location.city_code,
                'recipient_location_city_name': recipient_location.city_name,
                'recipient_location_zip4': recipient_location.zip4,
                'recipient_location_zip5': recipient_location.zip5,
                'recipient_location_street_address': recipient_location.address_line1,
                'recipient_location_congressional_code': recipient_location.congressional_code,
                'recipient_location_foreign_postal_code': recipient_location.foreign_postal_code,

                'officer_1_name': row['top_paid_fullname_1'],
                'officer_1_amount': row['top_paid_amount_1'],
                'officer_2_name': row['top_paid_fullname_2'],
                'officer_2_amount': row['top_paid_amount_2'],
                'officer_3_name': row['top_paid_fullname_3'],
                'officer_3_amount': row['top_paid_amount_3'],
                'officer_4_name': row['top_paid_fullname_4'],
                'officer_4_amount': row['top_paid_amount_4'],
                'officer_5_name': row['top_paid_fullname_5'],
                'officer_5_amount': row['top_paid_amount_5'],

                'data_source': "DBR",
                'cfda': cfda,
                'awarding_agency': shared_mappings['award'].awarding_agency if shared_mappings['award'] else None,
                'funding_agency': shared_mappings['award'].funding_agency if shared_mappings['award'] else None,
                'subaward_number': row['subaward_num'],
                'amount': row['subaward_amount'],
                'description': row['overall_description'],
                'recovery_model_question1': row['q1_flag'],
                'recovery_model_question2': row['q2_flag'],
                'action_date': row['subaward_date'],
                'award_report_fy_month': row['report_period_mon'],
                'award_report_fy_year': row['report_period_year'],
                'broker_award_id': row['id'],
                'internal_id': row['internal_id'],
                'award_type': award_type,

                'pop_country_code': row['principle_place_country'],
                'pop_country_name': place_of_performance.country_name,
                'pop_state_code': row['principle_place_state'],
                'pop_state_name': row['principle_place_state_name'],
                'pop_county_code': place_of_performance.county_code,
                'pop_county_name': place_of_performance.county_name,
                'pop_city_code': place_of_performance.city_code,
                'pop_city_name': row['principle_place_city'],
                'pop_zip4': row['principle_place_zip'],
                'pop_street_address': row['principle_place_street'],
                'pop_congressional_code': row['principle_place_district'],
                'updated_at': datetime.utcnow()
            }

            # Either we're starting with an empty table in regards to this award type or we've deleted all
            # subawards related to the internal_id, either way we just create the subaward
            Subaward.objects.create(**subaward_dict)
            if shared_mappings['award']:
                award_update_id_list.append(shared_mappings['award'].id)

    def process_subawards(self, db_cursor, shared_award_mappings, award_type, subaward_type, max_id):
        """ Process the subawards and insert them as we go """
        current_offset = 0

        subaward_list = self.gather_next_subawards(db_cursor, award_type, subaward_type, max_id, current_offset)

        # run as long as we get any results for the subawards, stop as soon as we run out
        while len(subaward_list) > 0:
            logger.info("Processing next 10,000 subawards, starting at offset: " + str(current_offset))
            for row in subaward_list:
                self.create_subaward(row, shared_award_mappings, award_type)

            current_offset += QUERY_LIMIT
            subaward_list = self.gather_next_subawards(db_cursor, award_type, subaward_type, max_id, current_offset)

    def process_award_type(self, db_cursor, award_type, subaward_type):
        """ Do all the processing for the award type given """

        # getting the starting point for new awards of the given type
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
            internal_ids = [internal_id.upper() for internal_id in shared_award_mappings]
            Subaward.objects.filter(internal_id__in=internal_ids, award_type=award_type).delete()

        self.process_subawards(db_cursor, shared_award_mappings, award_type, subaward_type, max_id)

    def cleanup_broken_links(self, db_cursor):
        broken_links = 0
        fixed_ids = []
        for award_type in ['procurement', 'grant']:
            broken_subawards = Subaward.objects.filter(award_id__isnull=True, award_type=award_type)
            if not broken_subawards:
                continue
            broken_links += len(broken_subawards)
            broker_internal_ids = [broken_subaward.internal_id for broken_subaward in broken_subawards]
            broker_award_data = self.get_award_data(db_cursor, award_type, None, internal_ids=broker_internal_ids)
            broker_mappings = self.gather_shared_award_data(broker_award_data, award_type)
            for broken_subaward in broken_subawards:
                award_data = broker_mappings.get(broken_subaward.internal_id.lower())
                award = award_data.get('award') if award_data else None
                if award:
                    broken_subaward.award = award
                    broken_subaward.awarding_agency = award.awarding_agency
                    broken_subaward.funding_agency = award.funding_agency
                    broken_subaward.prime_recipient = award.recipient
                    broken_subaward.updated_at = datetime.utcnow()
                    broken_subaward.save()
                    fixed_ids.append(award.id)
        award_update_id_list.extend(fixed_ids)
        logger.info('Fixed {} of {} broken links'.format(len(fixed_ids), broken_links))

    @db_transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting FSRS data load...')

        db_cursor = connections['data_broker'].cursor()

        # This is called at the start to prevent duplicating efforts
        # It may be called later but requires update_award_subawards to be called after
        logger.info('Cleaning up previous subawards without parent awards...')
        self.cleanup_broken_links(db_cursor)

        logger.info('Get Broker FSRS procurement data...')
        self.process_award_type(db_cursor, "procurement", "subcontract")

        logger.info('Get Broker FSRS grant data...')
        self.process_award_type(db_cursor, "grant", "subgrant")

        logger.info('Completed FSRS data load...')

        logger.info('Updating related award metadata...')
        update_award_subawards(tuple(award_update_id_list))
        logger.info('Finished updating award metadata...')

        logger.info('Load FSRS Script Complete!')


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
        "state_name": row.get("company_address_state_name"),
        "address_line1": row.get("company_address_street"),
        "congressional_code": row.get("company_address_district", None)
    }
    return loc


def location_d2_recipient_mapper(row):
    loc = {
        "location_country_code": row.get("awardee_address_country", ""),
        "city_name": row.get("awardee_address_city", ""),
        "location_zip": row.get("awardee_address_zip", ""),
        "state_code": row.get("awardee_address_state"),
        "state_name": row.get("awardee_address_state_name"),
        "address_line1": row.get("awardee_address_street"),
        "congressional_code": row.get("awardee_address_district")
    }
    return loc


def pop_mapper(row):
    loc = {
        "location_country_code": row.get("principle_place_country", ""),
        "city_name": row.get("principle_place_city", ""),
        "location_zip": row.get("principle_place_zip", ""),
        "state_code": row.get("principle_place_state"),
        "state_name": row.get("principle_place_state_name"),
        "address_line1": row.get("principle_place_street"),
        "congressional_code": row.get("principle_place_district")
    }
    return loc
