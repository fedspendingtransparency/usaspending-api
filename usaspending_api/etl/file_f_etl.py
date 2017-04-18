import logging
from usaspending_api.awards.models import Award, Subaward
from usaspending_api.references.models import LegalEntity, Location, CFDAProgram
from usaspending_api.etl.helpers import get_or_create_location

logger = logging.getLogger("console")

# These queries are directly from the broker
D1_FILE_F_QUERY = """
SELECT *
FROM   fsrs_procurement
       LEFT OUTER JOIN award_procurement ON fsrs_procurement.contract_number = award_procurement.piid
                                         AND fsrs_procurement.idv_reference_number = award_procurement.parent_award_id
       LEFT OUTER JOIN fsrs_subcontract  ON fsrs_subcontract.parent_id = fsrs_procurement.id
WHERE  award_procurement.submission_id = %s
"""

D2_FILE_F_QUERY = """
SELECT *
FROM   fsrs_grant
       LEFT OUTER JOIN award_financial_assistance ON fsrs_grant.fain = award_financial_assistance.fain
       LEFT OUTER JOIN fsrs_subgrant ON fsrs_grant.id = fsrs_subgrant.parent_id
WHERE  award_financial_assistance.submission_id = %s
"""


def load_file_f(submission_attributes, db_cursor):
    """
    Loads File F from the broker. db_cursor should be the db_cursor for Broker
    """

    # D1 File F
    db_cursor.execute(D1_FILE_F_QUERY, [submission_attributes.broker_submission_id])
    d1_f_data = dictfetchall(db_cursor)
    logger.info("Creating D1 F File Entires (Subcontracts): {}".format(len(d1_f_data)))

    for row in d1_f_data:
        # Find the award to attach this sub-contract to
        # We perform this lookup by finding the Award containing a transaction with
        # a matching parent award id, piid, and submission attributes
        award = Award.objects.filter(transaction__submission_attributes=submission_attributes,
                                     transaction__contract_data__piid=row['piid'],
                                     transaction__contract_data__parent_award_id=row['parent_award_id']).first()

        # We don't have a matching award for this subcontract, log a warning and continue to the next row
        if not award:
            logger.warn("Subcontract number {} cannot find matching award with piid {}, parent_award_id {}; skipping...".format(row['subcontract_num'], row['piid'], row['parent_award_id']))
            continue

        # Find the recipient by looking up by duns
        recipient = LegalEntity.objects.filter(recipient_unique_id=row['duns']).first()

        if not recipient:
            recipient = LegalEntity.objects.get_or_create(recipient_unique_id=row['duns']
                                                          parent_recipient_unique_id=row['parent_duns']
                                                          recipient_name=row["company_name"],
                                                          location=get_or_create_location(row, location_d1_recipient_mapper)
                                                          )

        # Get or create POP
        place_of_performance = get_or_create_location(row, pop_mapper)

        d1_f_dict = {
            'award': award,
            'recipient': recipient
            'submission': submission_attributes,
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
        Subaward.objects.create(**d1_f_dict)

    # D2 File F
    db_cursor.execute(D2_FILE_F_QUERY, [submission_attributes.broker_submission_id])
    d2_f_data = dictfetchall(db_cursor)
    logger.info("Creating D2 F File Entires (Subawards): {}".format(len(d2_f_data)))

    for row in d2_f_data:
        # Find the award to attach this sub-award to
        # We perform this lookup by finding the Award containing a transaction with
        # a matching fain and submission. If this fails, try submission and uri
        award = Award.objects.filter(transaction__submission_attributes=submission_attributes,
                                     transaction__assistance_data__fain=row['fain']).first()

        # Couldn't find a match on FAIN, try URI if it exists
        if not award and row['uri'] and len(row['uri']) > 0:
            award = Award.objects.filter(transaction__submission_attributes=submission_attributes,
                                         transaction_assistance_data__uri=row['uri']).first()

        # We don't have a matching award for this subcontract, log a warning and continue to the next row
        if not award:
            logger.warn("Subaward number {} cannot find matching award with fain {}, uri {}; skipping...".format(row['subaward_num'], row['fain'], row['uri']))
            continue

        # Find the recipient by looking up by duns
        recipient = LegalEntity.objects.filter(recipient_unique_id=row['duns']).first()

        if not recipient:
            recipient = LegalEntity.objects.get_or_create(recipient_unique_id=row['duns']
                                                          parent_recipient_unique_id=row['parent_duns']
                                                          recipient_name=row["awardee_name"],
                                                          location=get_or_create_location(row, location_d2_recipient_mapper)
                                                          )

        # Get or create POP
        place_of_performance = get_or_create_location(row, pop_mapper)

        # Get CFDAProgram
        cfda = CFDAProgram.objects.filter(program_number=row['cfda_number']).first()

        d2_f_dict = {
            'award': award,
            'recipient': recipient,
            'submission': submission_attributes,
            'cfda': cfda,
            'awarding_agency': award.awarding_agency,
            'funding_agency': award.funding_agency,
            'place_of_performance': place_of_performance,
            'subaward_number': 'subaward_num',
            'amount': 'subaward_amount',
            'description': 'project_description',
            'recovery_model_question1': 'compensation_q1',
            'recovery_model_question2': 'compensation_q2',
            'action_date': 'subaward_date',
            'award_report_fy_month': 'report_period_mon',
            'award_report_fy_year': 'report_period_year',
            'naics': None,
            'naics_description': None,
        }

        # Create the subaward
        Subaward.objects.create(**d1_f_dict)


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def location_d1_recipient_mapper(row):
    loc = {
        "location_country_code": row.get("company_address_country", ""),
        "city_name": row.get("company_address_city", ""),
        "location_zip": row.get("company_address_zip", "")
        "state_code": row.get("company_address_state"),
        "address_line1": row.get("company_address_street"),
        "congressional_code": row.get("company_address_district", None)
    }
    return loc


def pop_mapper(row):
    loc = {
        "location_country_code": row.get("principle_place_country", ""),
        "city_name": row.get("principle_place_city", ""),
        "location_zip": row.get("principle_place_zip", "")
        "state_code": row.get("principle_place_state"),
        "address_line1": row.get("principle_place_street"),
        "congressional_code": row.get("principle_place_district", None)
    }
    return loc


def location_d2_recipient_mapper(row):
    loc = {
        "location_country_code": row.get("awardee_address_country", ""),
        "city_name": row.get("awardee_address_city", ""),
        "location_zip": row.get("awardee_address_zip", "")
        "state_code": row.get("awardee_address_state"),
        "address_line1": row.get("awardee_address_street"),
        "congressional_code": row.get("awardee_address_district", None)
    }
    return loc
