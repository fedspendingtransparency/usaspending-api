import logging

from django.db.models import Count, Sum, F

from usaspending_api.awards.models import Award, Subaward
from usaspending_api.references.models import LegalEntity, Location, CFDAProgram, Agency
from usaspending_api.etl.helpers import get_or_create_location
from usaspending_api.etl.award_helpers import update_award_subawards
from usaspending_api.etl.broker_etl_helpers import dictfetchall, PhonyCursor

logger = logging.getLogger("console")

# These queries are directly from the broker
D1_FILE_F_QUERY = """
SELECT DISTINCT ON (subcontract_num, fsrs_procurement.contract_number, fsrs_procurement.idv_reference_number) *
FROM   fsrs_procurement
       LEFT OUTER JOIN award_procurement ON fsrs_procurement.contract_number = award_procurement.piid
                                         AND fsrs_procurement.idv_reference_number = award_procurement.parent_award_id
       LEFT OUTER JOIN fsrs_subcontract  ON fsrs_subcontract.parent_id = fsrs_procurement.id
WHERE  award_procurement.submission_id = %s
       AND award_procurement.piid IN %s
"""

D2_FILE_F_QUERY = """
SELECT DISTINCT ON (subaward_num, award_financial_assistance.fain, award_financial_assistance.uri) *
FROM   fsrs_grant
       LEFT OUTER JOIN award_financial_assistance ON fsrs_grant.fain = award_financial_assistance.fain
       LEFT OUTER JOIN fsrs_subgrant ON fsrs_grant.id = fsrs_subgrant.parent_id
WHERE  award_financial_assistance.submission_id = %s
       AND (award_financial_assistance.fain IN %s OR award_financial_assistance.fain IS NULL)
"""


def load_subawards(submission_attributes, db_cursor):
    """
    Loads File F from the broker. db_cursor should be the db_cursor for Broker
    """
    # A list of award id's to update the subaward accounts and totals on
    award_ids_to_update = set()

    # Get a list of PIIDs from this submission
    awards_for_sub = Award.objects.filter(transaction__submission=submission_attributes).distinct()
    piids = list(awards_for_sub.values_list("piid", flat=True))
    fains = list(awards_for_sub.values_list("fain", flat=True))

    # This allows us to handle an empty list in the SQL without changing the query
    piids.append(None)
    fains.append(None)

    # D1 File F
    db_cursor.execute(D1_FILE_F_QUERY, [submission_attributes.broker_submission_id, tuple(piids)])
    d1_f_data = dictfetchall(db_cursor)
    logger.info("Creating D1 F File Entries (Subcontracts): {}".format(len(d1_f_data)))
    d1_create_count = 0
    d1_update_count = 0
    d1_empty_count = 0

    for row in d1_f_data:
        if row['subcontract_num'] is None:
            if row['id'] is not None and row['subcontract_amount'] is not None:
                logger.warn("Subcontract of broker id {} has amount, but no number".format(row["id"]))
                logger.warn("Failing row: {}".format(row))
            else:
                d1_empty_count += 1
            continue

        # Get the agency
        agency = get_valid_awarding_agency(row)

        if not agency:
            logger.warn("Subaward number {} cannot find matching agency with toptier code {} and subtier code {}".format(row['subcontract_num'], row['awarding_agency_code'], row['awarding_sub_tier_agency_c']))
            continue

        # Find the award to attach this sub-contract to
        # We perform this lookup by finding the Award containing a transaction with
        # a matching parent award id, piid, and submission attributes
        award = Award.objects.filter(awarding_agency=agency,
                                     transaction__submission=submission_attributes,
                                     transaction__contract_data__piid=row['piid'],
                                     transaction__contract_data__isnull=False,
                                     transaction__contract_data__parent_award_id=row['parent_award_id']).distinct().order_by("-date_signed").first()

        # We don't have a matching award for this subcontract, log a warning and continue to the next row
        if not award:
            logger.warn("Subcontract number {} cannot find matching award with piid {}, parent_award_id {}; skipping...".format(row['subcontract_num'], row['piid'], row['parent_award_id']))
            continue

        award_ids_to_update.add(award.id)

        # Find the recipient by looking up by duns
        recipient, created = LegalEntity.get_or_create_by_duns(duns=row['duns'])

        if created:
            recipient.parent_recipient_unique_id = row['parent_duns']
            recipient.recipient_name = row['company_name']
            recipient.location = get_or_create_location(row, location_d1_recipient_mapper)
            recipient.save()

        # Get or create POP
        place_of_performance = get_or_create_location(row, pop_mapper)

        d1_f_dict = {
            'award': award,
            'recipient': recipient,
            'submission': submission_attributes,
            'data_source': "DBR",
            'cfda': None,
            'awarding_agency': award.awarding_agency,
            'funding_agency': award.funding_agency,
            'place_of_performance': place_of_performance,
            'subaward_number': row['subcontract_num'],
            'amount': row['subcontract_amount'],
            'description': row['overall_description'],
            'recovery_model_question1': row['recovery_model_q1'],
            'recovery_model_question2': row['recovery_model_q2'],
            'action_date': row['subcontract_date'],
            'award_report_fy_month': row['report_period_mon'],
            'award_report_fy_year': row['report_period_year'],
            'naics': row['naics'],
            'naics_description': row['naics_description'],
        }

        # Create the subaward
        subaward, created = Subaward.objects.update_or_create(subaward_number=row['subcontract_num'],
                                                              award=award,
                                                              defaults=d1_f_dict)
        if created:
            d1_create_count += 1
        else:
            d1_update_count += 1

    # D2 File F
    db_cursor.execute(D2_FILE_F_QUERY, [submission_attributes.broker_submission_id, tuple(fains)])
    d2_f_data = dictfetchall(db_cursor)
    logger.info("Creating D2 F File Entries (Subawards): {}".format(len(d2_f_data)))
    d2_create_count = 0
    d2_update_count = 0
    d2_empty_count = 0

    for row in d2_f_data:
        if row['subaward_num'] is None:
            if row['id'] is not None and row['subaward_amount'] is not None:
                logger.warn("Subcontract of broker id {} has amount, but no number".format(row["id"]))
                logger.warn("Failing row: {}".format(row))
            else:
                d2_empty_count += 1
            continue

        agency = get_valid_awarding_agency(row)

        if not agency:
            logger.warn("Subaward number {} cannot find matching agency with toptier code {} and subtier code {}".format(row['subaward_num'], row['awarding_agency_code'], row['awarding_sub_tier_agency_c']))
            continue

        # Find the award to attach this sub-award to
        # We perform this lookup by finding the Award containing a transaction with
        # a matching fain and submission. If this fails, try submission and uri
        if row['fain'] and len(row['fain']) > 0:
            award = Award.objects.filter(awarding_agency=agency,
                                         transaction__submission=submission_attributes,
                                         transaction__assistance_data__isnull=False,
                                         transaction__assistance_data__fain=row['fain']).distinct().order_by("-date_signed").first()

        # Couldn't find a match on FAIN, try URI if it exists
        if not award and row['uri'] and len(row['uri']) > 0:
            award = Award.objects.filter(awarding_agency=agency,
                                         transaction__submission=submission_attributes,
                                         transaction__assistance_data__isnull=False,
                                         transaction__assistance_data__uri=row['uri']).distinct().first()

        # We don't have a matching award for this subcontract, log a warning and continue to the next row
        if not award:
            logger.warn("Subaward number {} cannot find matching award with fain {}, uri {}; skipping...".format(row['subaward_num'], row['fain'], row['uri']))
            continue

        award_ids_to_update.add(award.id)

        # Find the recipient by looking up by duns
        recipient, created = LegalEntity.get_or_create_by_duns(duns=row['duns'])

        if created:
            recipient_name = row['awardee_name']
            if recipient_name is None:
                recipient_name = row['awardee_or_recipient_legal']
            if recipient_name is None:
                recipient_name = ""

            recipient.recipient_name = recipient_name
            recipient.parent_recipient_unique_id = row['parent_duns']
            recipient.location = get_or_create_location(row, location_d2_recipient_mapper)
            recipient.save()

        # Get or create POP
        place_of_performance = get_or_create_location(row, pop_mapper)

        # Get CFDAProgram
        cfda = CFDAProgram.objects.filter(program_number=row['cfda_number']).first()

        d2_f_dict = {
            'award': award,
            'recipient': recipient,
            'submission': submission_attributes,
            'data_source': "DBR",
            'cfda': cfda,
            'awarding_agency': award.awarding_agency,
            'funding_agency': award.funding_agency,
            'place_of_performance': place_of_performance,
            'subaward_number': row['subaward_num'],
            'amount': row['subaward_amount'],
            'description': row['project_description'],
            'recovery_model_question1': row['compensation_q1'],
            'recovery_model_question2': row['compensation_q2'],
            'action_date': row['subaward_date'],
            'award_report_fy_month': row['report_period_mon'],
            'award_report_fy_year': row['report_period_year'],
            'naics': None,
            'naics_description': None,
        }

        # Create the subaward
        subaward, created = Subaward.objects.update_or_create(subaward_number=row['subaward_num'],
                                                              award=award,
                                                              defaults=d2_f_dict)
        if created:
            d2_create_count += 1
        else:
            d2_update_count += 1

    # Update Award objects with subaward aggregates
    update_award_subawards(tuple(award_ids_to_update))

    logger.info(
        """Submission {}
           Subcontracts created: {}
           Subcontracts updated: {}
           Empty subcontract rows: {}
           Subawards created: {}
           Subawards updated: {}
           Empty subaward rows: {}""".format(submission_attributes.broker_submission_id,
                                             d1_create_count,
                                             d1_update_count,
                                             d1_empty_count,
                                             d2_create_count,
                                             d2_update_count,
                                             d2_empty_count))


def get_valid_awarding_agency(row):
    agency = None

    agency_subtier_code = row['awarding_sub_tier_agency_c']
    agency_toptier_code = row['awarding_agency_code']
    valid_subtier_code = (agency_subtier_code and len(agency_subtier_code) > 0)
    valid_toptier_code = (agency_toptier_code and len(agency_toptier_code) > 0)

    if not valid_toptier_code and not valid_subtier_code:
        return None

    agency = None
    # Get the awarding agency
    if valid_subtier_code and valid_toptier_code:
        agency = Agency.get_by_toptier_subtier(row['awarding_agency_code'],
                                               row['awarding_sub_tier_agency_c'])

    if not agency and valid_subtier_code:
        agency = Agency.get_by_subtier(row['awarding_sub_tier_agency_c'])

    if not agency and valid_toptier_code:
        agency = Agency.get_by_toptier(row['awarding_agency_code'])

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


def location_d2_recipient_mapper(row):
    loc = {
        "location_country_code": row.get("awardee_address_country", ""),
        "city_name": row.get("awardee_address_city", ""),
        "location_zip": row.get("awardee_address_zip", ""),
        "state_code": row.get("awardee_address_state"),
        "address_line1": row.get("awardee_address_street"),
        "congressional_code": row.get("awardee_address_district", None)
    }
    return loc
