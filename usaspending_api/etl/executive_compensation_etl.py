import logging

from datetime import datetime, timezone
from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import LegalEntity, LegalEntityOfficers


logger = logging.getLogger("console")


# We join DUNS and exec comp data, but ignore any empty data sets. Expected count from broker is < 5000.
EXEC_COMP_QUERY = """
SELECT DISTINCT ON (e.awardee_or_recipient_uniqu) *
FROM executive_compensation e
INNER JOIN (
    SELECT awardee_or_recipient_uniqu, max(created_at) as MaxDate
    FROM executive_compensation ex
    GROUP BY awardee_or_recipient_uniqu
) ex ON e.awardee_or_recipient_uniqu = ex.awardee_or_recipient_uniqu
    AND e.created_at = ex.MaxDate
WHERE
e.created_at >= %s AND
(TRIM(high_comp_officer1_full_na) != '' OR
TRIM(high_comp_officer2_full_na) != '' OR
TRIM(high_comp_officer3_full_na) != '' OR
TRIM(high_comp_officer4_full_na) != '' OR
TRIM(high_comp_officer5_full_na) != '' OR
TRIM(high_comp_officer1_amount) != '' OR
TRIM(high_comp_officer2_amount) != '' OR
TRIM(high_comp_officer3_amount) != '' OR
TRIM(high_comp_officer4_amount) != '' OR
TRIM(high_comp_officer5_amount) != '')
"""


# Updates all executive compensation data
def load_executive_compensation(db_cursor, date, start_date):
    logger.info("Getting DUNS/Exec Comp data from broker based on the last pull date of %s..." % str(date))

    # Get first page
    db_cursor.execute(EXEC_COMP_QUERY, [date])
    exec_comp_query_dict = dictfetchall(db_cursor)

    total_rows = len(exec_comp_query_dict)
    logger.info('Updating Executive Compensation Data, {} rows coming from the Broker...'.format(total_rows))

    start_time = datetime.now(timezone.utc)

    for index, row in enumerate(exec_comp_query_dict, 1):

        if not (index % 100):
            logger.info('Loading row {} of {} ({})'.format(str(index),
                                                           str(total_rows),
                                                           datetime.now() - start_time))

        leo_update_dict = {
            "officer_1_name": row['high_comp_officer1_full_na'],
            "officer_1_amount": row['high_comp_officer1_amount'],
            "officer_2_name": row['high_comp_officer2_full_na'],
            "officer_2_amount": row['high_comp_officer2_amount'],
            "officer_3_name": row['high_comp_officer3_full_na'],
            "officer_3_amount": row['high_comp_officer3_amount'],
            "officer_4_name": row['high_comp_officer4_full_na'],
            "officer_4_amount": row['high_comp_officer4_amount'],
            "officer_5_name": row['high_comp_officer5_full_na'],
            "officer_5_amount": row['high_comp_officer5_amount'],
        }

        any_data = False
        for attr, value in leo_update_dict.items():
            if value and value != "":
                any_data = True
                break

        if not any_data:
            continue

        duns_number = row['awardee_or_recipient_uniqu']

        # Deal with multiples that we have in our LE table
        legal_entities = LegalEntity.objects.filter(recipient_unique_id=duns_number)
        if not legal_entities.exists():
            logger.info('No record in data store for DUNS {}. Skipping...'.format(duns_number))

        for le in legal_entities:
            leo, _ = LegalEntityOfficers.objects.get_or_create(legal_entity=le)
            for attr, value in leo_update_dict.items():
                if value == "":
                    value = None
                setattr(leo, attr, value)
            leo.save()

    # Update the date for the last time the data load was run
    update_last_load_date("exec_comp", start_date)
