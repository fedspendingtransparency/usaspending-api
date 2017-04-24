import logging

from usaspending_api.references.models import LegalEntity, LegalEntityOfficers
from usaspending_api.etl.broker_etl_helpers import dictfetchall, PhonyCursor

logger = logging.getLogger("console")

# Broker SQL query
FILE_E_QUERY = """
SELECT DISTINCT ON (awardee_or_recipient_uniqu) *
FROM executive_compensation e
INNER JOIN (
    SELECT awardee_or_recipient_uniqu, max(created_at) as MaxDate
    FROM executive_compensation ex
    GROUP BY awardee_or_recipient_uniqu
) ex ON e.awardee_or_recipient_uniqu = ex.awardee_or_recipient_uniqu
     AND e.created_at = ex.MaxDate
WHERE e.awardee_or_recipient_uniqu IN %s
"""


def load_executive_compensation(db_cursor, duns_list=None):
    """
    Loads File E from the broker. db_cursor should be the db_cursor for Broker
    """
    if duns_list is None:
        duns_list = list(set(LegalEntity.objects.all().values_list("recipient_unique_id", flat=True)))

    duns_list = [str(x) for x in duns_list]

    # File E
    db_cursor.execute(FILE_E_QUERY, [tuple(duns_list)])
    e_data = dictfetchall(db_cursor)
    logger.info("Updating Executive Compensation, entries: {}".format(len(e_data)))

    for row in e_data:
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

        leo = LegalEntityOfficers.objects.get(legal_entity__recipient_unique_id=row['awardee_or_recipient_uniqu'])

        for attr, value in leo_update_dict.items():
            if value == "":
                value = None
            setattr(leo, attr, value)
        leo.save()
