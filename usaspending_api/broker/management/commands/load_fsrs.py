import logging
from datetime import datetime, timezone

from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.models import F, Max

from usaspending_api.awards.models import Award, Subaward, TransactionFPDS
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.etl.award_helpers import update_award_subawards
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import Agency, Cfda, RefCountryCode, RefCityCountyCode, LegalEntity

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

QUERY_LIMIT = 10000
LOG_LIMIT = 10000

award_update_id_list = set()


class Command(BaseCommand):
    help = "Load new FSRS data from broker."

    @staticmethod
    def get_award_data(db_cursor, award_type, max_id, internal_ids=None):
        """ Gets data for all new awards from broker with ID greater than the ones already stored for the given
            award type
        """

        if award_type == 'procurement':
            _type = 'sub-contract'
        else:
            _type = 'sub-grant'

        if isinstance(internal_ids, list) and len(internal_ids) > 0:
            ids_string = ','.join([str(id).lower() for id in internal_ids])
            query = (
                "SELECT DISTINCT unique_award_key, internal_id, awarding_sub_tier_agency_c, prime_id "
                "FROM subaward "
                "WHERE subaward_type = '{}' and internal_id = ANY(\'{{{}}}\'::text[]) "
                "ORDER BY prime_id".format(_type, ids_string)
            )
        else:
            query = (
                "SELECT DISTINCT unique_award_key, internal_id, awarding_sub_tier_agency_c, prime_id "
                "FROM subaward "
                "WHERE subaward_type = '{}' and prime_id > {} "
                "ORDER BY prime_id".format(_type, str(max_id))
            )

        db_cursor.execute(query)

        return dictfetchall(db_cursor)

    def get_award(self, row, award_type):
        award = None
        if award_type == 'procurement':

            agency = get_valid_awarding_agency(row)
            if not agency:
                logger.warning(
                    "Internal ID {} cannot find matching agency with subtier code {}".format(
                        row['internal_id'], row['awarding_sub_tier_agency_c']
                    )
                )
                return None

            # Find the award to attach this sub-contract to using the generated unique ID (unique_award_key):
            if row['unique_award_key'] is not None:
                award = (
                    Award.objects
                    .filter(
                        generated_unique_award_id=row['unique_award_key'],
                        latest_transaction_id__isnull=False,
                    )
                    .distinct()
                    .order_by("-date_signed")
                    .first()
                )

        else:

            # Find the award to attach this sub-grant to using the generated unique ID (unique_award_key):
            if row['unique_award_key'] is not None:
                all_awards = (
                    Award.objects
                    .filter(
                        generated_unique_award_id=row['unique_award_key'],
                        latest_transaction_id__isnull=False,
                    )
                    .distinct()
                    .order_by("-date_signed")
                )

                if all_awards.count() > 1:
                    logger.warning(
                        "Multiple awards found with generated_unique_award_id '{}'".format(row['unique_award_key']))

                award = all_awards.first()

        if not award:
            msg = "[Internal ID {}] Award not found for unique_award_key '{}'"
            logger.warning(msg.format(row['internal_id'], row['unique_award_key']))
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
        logger.info("{} new awards to process".format(new_awards))
        for row in data:
            counter += 1
            if counter % LOG_LIMIT == 0:
                logger.info("Processed {} of {} new awards".format(counter, new_awards))
            award = self.get_award(row, award_type)

            # set shared data content
            shared_data[row['internal_id']] = {'award': award}
        logger.info("Completed shared data gathering")

        return shared_data

    @staticmethod
    def gather_next_subawards(db_cursor, award_type, subaward_type, max_id, offset):
        """ Get next batch of subawards of the relevant type starting at a given offset """
        query_columns = [
            'award.internal_id',
            'award.id',
            'award.report_period_mon',
            'award.report_period_year',
            'sub_award.duns AS duns',
            'sub_award.parent_duns AS parent_duns',
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
            query_columns.extend(
                [
                    'award.contract_number AS piid',
                    'sub_award.naics AS naics_code',
                    'sub_award.subcontract_num AS subaward_num',
                    'sub_award.subcontract_amount AS subaward_amount',
                    'sub_award.overall_description AS description',
                    'sub_award.recovery_model_q1 AS q1_flag',
                    'sub_award.recovery_model_q2 AS q2_flag',
                    'sub_award.subcontract_date AS action_date',
                    'sub_award.company_name AS recipient_name',
                    'sub_award.company_address_country AS recipient_location_country_code',
                    'sub_award.company_address_city AS recipient_location_city_name',
                    'sub_award.company_address_zip AS recipient_location_zip4',
                    'LEFT(sub_award.company_address_zip, 5) AS recipient_location_zip5',
                    'sub_award.company_address_state AS recipient_location_state_code',
                    'sub_award.company_address_state_name AS recipient_location_state_name',
                    'sub_award.company_address_street AS recipient_location_street_address',
                    'sub_award.company_address_district AS recipient_location_congressional_code',
                    'sub_award.parent_company_name AS parent_recipient_name',
                    'sub_award.bus_types AS bus_types',
                ]
            )
            _where = "WHERE award.id > {} AND sub_award.subcontract_num IS NOT NULL"

        else:  # grant
            query_columns.extend(
                [
                    'sub_award.cfda_numbers',
                    'sub_award.subaward_num',
                    'sub_award.subaward_amount',
                    'sub_award.project_description AS description',
                    'sub_award.compensation_q1 AS q1_flag',
                    'sub_award.compensation_q2 AS q2_flag',
                    'sub_award.subaward_date AS action_date',
                    'sub_award.awardee_name AS recipient_name',
                    'sub_award.awardee_address_country AS recipient_location_country_code',
                    'sub_award.awardee_address_city AS recipient_location_city_name',
                    'sub_award.awardee_address_zip AS recipient_location_zip4',
                    'LEFT(sub_award.awardee_address_zip, 5) AS recipient_location_zip5',
                    'sub_award.awardee_address_state AS recipient_location_state_code',
                    'sub_award.awardee_address_state_name AS recipient_location_state_name',
                    'sub_award.awardee_address_street AS recipient_location_street_address',
                    'sub_award.awardee_address_district AS recipient_location_congressional_code',
                    'UPPER(award.fain) AS fain',
                ]
            )
            _where = "WHERE award.id > {} AND sub_award.subaward_num IS NOT NULL"

        _select = "SELECT {}"
        _from = "FROM fsrs_{} AS award JOIN fsrs_{} AS sub_award ON sub_award.parent_id = award.id"
        _other = "ORDER BY award.id, sub_award.id LIMIT {} OFFSET {}"
        query = " ".join([_select, _from, _where, _other]).format(
            ",".join(query_columns), award_type, subaward_type, str(max_id), str(QUERY_LIMIT), str(offset)
        )

        db_cursor.execute(query)

        return dictfetchall(db_cursor)

    def create_subaward(self, row, shared_award_mappings, award_type):
        """ Creates a subaward if the internal ID of the current row is in the shared award mappings (this was made
            to satisfy codeclimate complexity issues)
        """

        # only insert the subaward if the internal_id is in our mappings, otherwise there was a problem
        # finding one or more parts of the shared data for it and we don't want to insert it.
        if row['internal_id'] not in shared_award_mappings:
            logger.info("[Internal ID {}] not in 'shared_award_mappings'".format(row["internal_id"]))
        else:
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

            subaward_dict = {
                'recipient_unique_id': row['duns'],
                'recipient_name': row['recipient_name'],
                'dba_name': row['dba_name'],
                'parent_recipient_unique_id': row['parent_duns'],
                'parent_recipient_name': row.get('parent_recipient_name', None),
                'business_type_description': row.get('bus_types', None),
                'prime_recipient': prime_award_dict.get('prime_recipient', None),
                'prime_recipient_name': prime_award_dict.get('prime_recipient_name', None),
                'business_categories': prime_award_dict.get('business_categories', []),
                'recipient_location_country_code': row['recipient_location_country_code'],
                'recipient_location_state_code': row['recipient_location_state_code'],
                'recipient_location_state_name': row['recipient_location_state_name'],
                'recipient_location_city_name': row['recipient_location_city_name'],
                'recipient_location_zip4': row['recipient_location_zip4'],
                'recipient_location_zip5': row['recipient_location_zip5'],
                'recipient_location_street_address': row['recipient_location_street_address'],
                'recipient_location_congressional_code': row['recipient_location_congressional_code'],
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
                'subaward_number': row['subaward_num'],
                'amount': row['subaward_amount'],
                'description': row['description'],
                'recovery_model_question1': row['q1_flag'],
                'recovery_model_question2': row['q2_flag'],
                'action_date': row['action_date'],
                'award_report_fy_month': row['report_period_mon'],
                'award_report_fy_year': row['report_period_year'],
                'broker_award_id': row['id'],
                'internal_id': row['internal_id'],
                'award_type': award_type,
                'pop_country_code': row['principle_place_country'],
                'pop_state_code': row['principle_place_state'],
                'pop_state_name': row['principle_place_state_name'],
                'pop_city_name': row['principle_place_city'],
                'pop_zip4': row['principle_place_zip'],
                'pop_street_address': row['principle_place_street'],
                'pop_congressional_code': row['principle_place_district'],
                'piid': row.get('piid', None),
                'fain': row.get('fain', None),
                'updated_at': datetime.now(timezone.utc),

                # keyword_ts_vector               # leave NULL, matview SQL will populate
                # award_ts_vector                 # leave NULL, matview SQL will populate
                # recipient_name_ts_vector        # leave NULL, matview SQL will populate
                # product_or_service_description  # leave NULL, matview SQL will populate
                # total_obl_bin                   # leave NULL, matview SQL will populate
                # business_type_code              # always NULL
                # extent_competed                 # always NULL
            }

            if shared_mappings['award']:
                subaward_dict.update(
                    {
                        "award_id": shared_mappings["award"].id,
                        "prime_award_type": shared_mappings["award"].type,
                        "last_modified_date": shared_mappings["award"].last_modified_date,
                        "latest_transaction_id": shared_mappings["award"].latest_transaction_id,
                        "business_categories": get_le_business_categories(shared_mappings["award"].recipient_id),
                    }
                )
                funding_agency = get_agency_values(shared_mappings["award"].funding_agency)
                awarding_agency = get_agency_values(shared_mappings["award"].awarding_agency)
                contract_data = get_contract_fields(shared_mappings["award"].latest_transaction_id)
                if contract_data:
                    subaward_dict.update(
                        {

                            'pulled_from': contract_data['pulled_from'],
                            'product_or_service_code': contract_data['product_or_service_code'],
                            # 'product_or_service_description': None,
                            'type_of_contract_pricing': contract_data['type_of_contract_pricing'],
                            'type_set_aside': contract_data['type_set_aside'],
                        }
                    )

            else:
                funding_agency, awarding_agency = None, None

            if funding_agency:
                subaward_dict.update(
                    {
                        "funding_agency_id": funding_agency["agency_id"],
                        "funding_toptier_agency_abbreviation": funding_agency["toptier_agency_abbreviation"],
                        "funding_toptier_agency_name": funding_agency["toptier_agency_name"],
                        "funding_subtier_agency_abbreviation": funding_agency["subtier_agency_abbreviation"],
                        "funding_subtier_agency_name": funding_agency["subtier_agency_name"],
                    }
                )
            if awarding_agency:
                subaward_dict.update(
                    {
                        "awarding_agency_id": awarding_agency["agency_id"],
                        "awarding_toptier_agency_abbreviation": awarding_agency["toptier_agency_abbreviation"],
                        "awarding_toptier_agency_name": awarding_agency["toptier_agency_name"],
                        "awarding_subtier_agency_abbreviation": awarding_agency["subtier_agency_abbreviation"],
                        "awarding_subtier_agency_name": awarding_agency["subtier_agency_name"],
                    }
                )

            cfda = None
            if 'cfda_numbers' in row and row['cfda_numbers']:
                only_num = row['cfda_numbers'].split(' ')
                cfda = Cfda.objects.filter(program_number=only_num[0]).first()

            if cfda:
                subaward_dict.update({
                    "cfda_number": cfda.program_number,
                    "cfda_title": cfda.program_title,
                    "cfda_id": cfda.pk,
                })

            subaward_dict['pop_country_name'] = get_country_name_from_code(row['principle_place_country'])
            subaward_dict['recipient_location_country_name'] = get_country_name_from_code(
                row['recipient_location_country_code']
            )

            performance_city_county = get_city_and_county_from_state(
                row['principle_place_state'], row['principle_place_city']
            )
            subaward_dict['pop_county_code'] = performance_city_county.get("county_code")
            subaward_dict['pop_county_name'] = performance_city_county.get("county_name")
            subaward_dict['pop_city_code'] = performance_city_county.get("city_code")

            ref_loc_city_county = get_city_and_county_from_state(
                row['recipient_location_state_code'], row['recipient_location_city_name']
            )
            subaward_dict['recipient_location_county_code'] = ref_loc_city_county.get("county_code")
            subaward_dict['recipient_location_county_name'] = ref_loc_city_county.get("county_name")
            subaward_dict['recipient_location_city_code'] = ref_loc_city_county.get("city_code")

            # Either we're starting with an empty table in regards to this award type or we've deleted all
            # subawards related to the internal_id, either way we just create the subaward
            Subaward.objects.create(**subaward_dict)
            if shared_mappings["award"]:
                award_update_id_list.add(shared_mappings["award"].id)

    def process_subawards(self, db_cursor, shared_award_mappings, award_type, subaward_type, max_id):
        """ Process the subawards and insert them as we go """
        current_offset = 0

        subaward_list = self.gather_next_subawards(db_cursor, award_type, subaward_type, max_id, current_offset)

        # run as long as we get any results for the subawards, stop as soon as we run out
        while len(subaward_list) > 0:
            logger.info("Processing next {:,} subawards, starting at offset: {:,}".format(QUERY_LIMIT, current_offset))
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
        """ Given a db connection, try to update all the unlinked subawards to their parent awards  """
        broken_links = 0
        fixed_links = 0
        fixed_ids = set()
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
                    broken_subaward.updated_at = datetime.now(timezone.utc)
                    broken_subaward.save()
                    fixed_ids.add(award.id)
                    fixed_links += 1
        award_update_id_list.update(fixed_ids)
        logger.info('Fixed {} of {} broken links'.format(fixed_links, broken_links))

    @transaction.atomic
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

    agency_subtier_code = row['awarding_sub_tier_agency_c']
    valid_subtier_code = agency_subtier_code and len(agency_subtier_code) > 0

    # Get the awarding agency
    if valid_subtier_code:
        agency = Agency.get_by_subtier(agency_subtier_code)

    return agency


def get_agency_values(agency):
    if not agency:
        return None
    agency_dict = {
        "toptier_agency_abbreviation": F("toptier_agency__abbreviation"),
        "toptier_agency_name": F("toptier_agency__name"),
        "subtier_agency_abbreviation": F("subtier_agency__abbreviation"),
        "subtier_agency_name": F("subtier_agency__name"),
        "agency_id": F("id"),
    }
    return Agency.objects.filter(id=agency.id).values(**agency_dict).first()


def get_country_name_from_code(country_code):
    ref_loc = RefCountryCode.objects.filter(country_code=country_code).values('country_name').first()
    if ref_loc:
        return ref_loc['country_name']
    return None


def get_city_and_county_from_state(state, city_name):
    ref_loc = RefCityCountyCode.objects.filter(state_code=state, city_name=city_name).values(
        'county_name', 'county_code', 'city_code'
    ).first()
    if ref_loc:
        return ref_loc
    return {}


def get_le_business_categories(le_id):
    le = LegalEntity.objects.get(legal_entity_id=le_id)
    if le:
        return le.business_categories or []
    return []


def get_contract_fields(transaction_id):
    trx = TransactionFPDS.objects.filter(transaction_id=transaction_id).values().first()
    if trx:
        return trx
    return {}
