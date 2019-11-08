"""
Creates legal entities and adds them to transaction normalized rows and award rows based on transaction_location_data
from `create_locations`
"""

import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("console")
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    def handle(self, *args, **options):

        with connection.cursor() as curs:
            curs.execute(self.CREATE_TRANS_LE_TABLE)
            curs.execute(self.ALTER_TRANS_LE_TABLE)
            curs.execute(self.CREATE_LE_AND_TRANS_LE)
            curs.execute(self.UPDATE_TN)
            curs.execute(self.UPDATE_AWARD)

    CREATE_TRANS_LE_TABLE = """
        CREATE TEMPORARY TABLE trans_to_le (
        transaction_id int,
        legal_entity_id int
        );
    """

    ALTER_TRANS_LE_TABLE = """
        ALTER TABLE trans_to_le ADD CONSTRAINT trans_to_le_uc UNIQUE (transaction_id, legal_entity_id);
    """

    CREATE_LE_AND_TRANS_LE = """
        WITH trans_to_loc AS
        (
        SELECT transaction_id, location_id FROM transaction_to_location
        ),
        existing_legal_entities AS
        (
        SELECT legal_entity_id, location_id FROM legal_entity
        WHERE recipient_unique_id='' AND recipient_name='Multiple Recipients'
        AND legal_entity.location_id= ANY(SELECT DISTINCT location_id from trans_to_loc)
        ),
        new_legal_entities AS
        (
          INSERT INTO legal_entity (business_categories, data_source, business_types_description, create_date,
                update_date, recipient_unique_id, recipient_name, location_id)
            SELECT '{}', 'DBR', 'Unknown Types', NOW(), NOW(), '', 'Multiple Recipients', location_id FROM trans_to_loc
          ON CONFLICT ON CONSTRAINT legal_entity_recipient_unique_id_reci_58a49e8b_uniq DO NOTHING
          RETURNING legal_entity_id, location_id
        ),
        inserted_les AS
        (
        SELECT * FROM existing_legal_entities
        UNION ALL
        SELECT * FROM new_legal_entities
        )
        INSERT INTO trans_to_le (transaction_id, legal_entity_id)
          SELECT trans_to_loc.transaction_id, inserted_les.legal_entity_id FROM trans_to_loc, inserted_les
          WHERE trans_to_loc.location_id = inserted_les.location_id
          ON CONFLICT ON CONSTRAINT trans_to_le_uc DO NOTHING;
    """

    UPDATE_TN = """
        UPDATE transaction_normalized tn
        SET recipient_id = trans_to_le.legal_entity_id
        FROM trans_to_le
        WHERE tn.id = trans_to_le.transaction_id;
    """

    UPDATE_AWARD = """
        UPDATE awards a
        SET recipient_id = trans_to_le.legal_entity_id
        FROM trans_to_le
        WHERE a.latest_transaction_id = trans_to_le.transaction_id;
    """
