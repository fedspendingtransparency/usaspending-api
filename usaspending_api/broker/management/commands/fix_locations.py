"""
Idempotently re-runs location creation / matching code from broker FPDS data
"""

import logging
import time

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("console")
exception_logger = logging.getLogger("exceptions")

BATCH_DOWNLOAD_SIZE = 10000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("--batch", type=int, default=BATCH_DOWNLOAD_SIZE, help="ID range to update per query")

    def handle(self, *args, **options):

        start = time.time()
        with connection.cursor() as curs:
            curs.execute(self.CREATE_STATE_ABBREVS)
            for (descrip, table_name, base_qry) in self.UPDATERS:
                batches = self.find_batches(curs=curs, table=table_name, options=options)
                for (floor, ceiling) in batches:
                    qry = base_qry.format(floor=floor, ceiling=ceiling)
                    curs.execute(qry)
                    elapsed = time.time() - start
                    logger.info("{}: ID {} to {}, {} s".format(descrip, floor, ceiling, elapsed))

    def find_batches(self, curs, table, options):
        batch = options["batch"]
        curs.execute(self.BOUNDARY_FINDER.format(table))
        (lowest, highest) = curs.fetchone()
        floor = (lowest // batch) * batch
        while floor <= highest:
            yield (floor, floor + batch)
            floor += batch

    BOUNDARY_FINDER = """
        SELECT MIN(transaction_id) AS floor, MAX(transaction_id) AS ceiling
        FROM   {}"""

    # Tuples of ( description, controlling table name, query)
    UPDATERS = (
        (
            "Place of performance for FABS",
            "transaction_fabs",
            """
            UPDATE references_location l
            SET    state_name = UPPER(tf.place_of_perform_state_nam),
                state_code = sa.abbrev
            FROM   transaction_normalized tn
            JOIN   transaction_fabs tf ON (tf.transaction_id = tn.id)
            JOIN   state_abbrevs sa ON (UPPER(tf.place_of_perform_state_nam) = sa.name)
            WHERE  tn.place_of_performance_id = l.location_id
            AND    l.location_country_code = 'USA'
            AND    l.state_code IS NULL
            AND    l.state_name IS NULL
            AND    l.place_of_performance_flag
            AND    tf.transaction_id >= {floor}
            AND    tf.transaction_id < {ceiling};
            """,
        ),
        (
            "Recipient for FABS (get state code from name)",
            "transaction_fabs",
            """
        UPDATE references_location l
        SET    state_name = UPPER(tf.legal_entity_state_name),
               state_code = sa.abbrev
        FROM   transaction_normalized tn
        JOIN   transaction_fabs tf ON (tf.transaction_id = tn.id)
        JOIN   legal_entity le ON (tn.recipient_id = le.legal_entity_id)
        JOIN   state_abbrevs sa ON (UPPER(tf.place_of_perform_state_nam) = sa.name)
        WHERE  le.location_id = l.location_id
        AND    tf.legal_entity_state_code IS NULL
        AND    l.location_country_code = 'USA'
        AND    l.state_code IS NULL
        AND    l.state_name IS NULL
        AND    l.recipient_flag
        AND    tf.transaction_id >= {floor}
        AND    tf.transaction_id < {ceiling};
        """,
        ),
        (
            "Recipient for FABS (get state name from code)",
            "transaction_fabs",
            """
        UPDATE references_location l
        SET    state_name = sa.name,
               state_code = UPPER(REPLACE(tf.legal_entity_state_code, '.', ''))
        FROM   transaction_normalized tn
        JOIN   transaction_fabs tf ON (tf.transaction_id = tn.id)
        JOIN   legal_entity le ON (tn.recipient_id = le.legal_entity_id)
        JOIN   state_abbrevs sa ON (UPPER(REPLACE(tf.legal_entity_state_code, '.', '')) = sa.abbrev)
        WHERE  le.location_id = l.location_id
        AND    tf.legal_entity_state_code IS NOT NULL
        AND    l.location_country_code = 'USA'
        AND    l.state_code IS NULL
        AND    l.state_name IS NULL
        AND    l.recipient_flag
        AND    tf.transaction_id >= {floor}
        AND    tf.transaction_id < {ceiling};
        """,
        ),
        (
            "Place of performance for FPDS",
            "transaction_fpds",
            """
        UPDATE references_location l
        SET    state_name = sa.name,
               state_code = UPPER(REPLACE(tf.place_of_performance_state, '.', ''))
        FROM   transaction_normalized tn
        JOIN   transaction_fpds tf ON (tf.transaction_id = tn.id)
        JOIN   state_abbrevs sa ON UPPER(REPLACE(tf.place_of_performance_state, '.', '')) = sa.abbrev
        WHERE  tn.place_of_performance_id = l.location_id
        AND    l.location_country_code = 'USA'
        AND    l.state_code IS NULL
        AND    l.state_name IS NULL
        AND    l.place_of_performance_flag
        AND    tf.transaction_id >= {floor}
        AND    tf.transaction_id < {ceiling};
        """,
        ),
        (
            "Recipient for FPDS (get state name from code)",
            "transaction_fpds",
            """
        UPDATE references_location l
        SET    state_name = sa.name,
               state_code = UPPER(REPLACE(tf.legal_entity_state_code, '.', ''))
        FROM   transaction_normalized tn
        JOIN   transaction_fpds tf ON (tf.transaction_id = tn.id)
        JOIN   legal_entity le ON (tn.recipient_id = le.legal_entity_id)
        JOIN   state_abbrevs sa ON (UPPER(REPLACE(tf.legal_entity_state_code, '.', '')) = sa.abbrev)
        WHERE  le.location_id = l.location_id
        AND    l.location_country_code = 'USA'
        AND    l.state_code IS NULL
        AND    l.state_name IS NULL
        AND    l.recipient_flag
        AND    tf.transaction_id >= {floor}
        AND    tf.transaction_id < {ceiling};
        """,
        ),
    )

    CREATE_STATE_ABBREVS = """
        CREATE TEMPORARY TABLE state_abbrevs (abbrev TEXT PRIMARY KEY, name TEXT NOT NULL);

        INSERT INTO state_abbrevs (abbrev, name) VALUES
        ('AK', 'ALASKA'),
        ('AL', 'ALABAMA'),
        ('AR', 'ARKANSAS'),
        ('AS', 'AMERICAN SAMOA'),
        ('AZ', 'ARIZONA'),
        ('CA', 'CALIFORNIA'),
        ('CO', 'COLORADO'),
        ('CT', 'CONNECTICUT'),
        ('DC', 'DISTRICT OF COLUMBIA'),
        ('DE', 'DELAWARE'),
        ('FL', 'FLORIDA'),
        ('FM', 'FEDERATED STATES OF MICRONESIA'),
        ('GA', 'GEORGIA'),
        ('GU', 'GUAM'),
        ('HI', 'HAWAII'),
        ('IA', 'IOWA'),
        ('ID', 'IDAHO'),
        ('IL', 'ILLINOIS'),
        ('IN', 'INDIANA'),
        ('KS', 'KANSAS'),
        ('KY', 'KENTUCKY'),
        ('LA', 'LOUISIANA'),
        ('MA', 'MASSACHUSETTS'),
        ('MD', 'MARYLAND'),
        ('ME', 'MAINE'),
        ('MH', 'MARSHALL ISLANDS'),
        ('MI', 'MICHIGAN'),
        ('MN', 'MINNESOTA'),
        ('MO', 'MISSOURI'),
        ('MP', 'NORTHERN MARIANA ISLANDS'),
        ('MS', 'MISSISSIPPI'),
        ('MT', 'MONTANA'),
        ('NC', 'NORTH CAROLINA'),
        ('ND', 'NORTH DAKOTA'),
        ('NE', 'NEBRASKA'),
        ('NH', 'NEW HAMPSHIRE'),
        ('NJ', 'NEW JERSEY'),
        ('NM', 'NEW MEXICO'),
        ('NV', 'NEVADA'),
        ('NY', 'NEW YORK'),
        ('OH', 'OHIO'),
        ('OK', 'OKLAHOMA'),
        ('OR', 'OREGON'),
        ('PA', 'PENNSYLVANIA'),
        ('PR', 'PUERTO RICO'),
        ('PW', 'PALAU'),
        ('RI', 'RHODE ISLAND'),
        ('SC', 'SOUTH CAROLINA'),
        ('SD', 'SOUTH DAKOTA'),
        ('TN', 'TENNESSEE'),
        ('TX', 'TEXAS'),
        ('UT', 'UTAH'),
        ('UM', 'U.S. MINOR OUTLYING ISLANDS'),
        ('VA', 'VIRGINIA'),
        ('VI', 'VIRGIN ISLANDS'),
        ('VT', 'VERMONT'),
        ('WA', 'WASHINGTON'),
        ('WI', 'WISCONSIN'),
        ('WV', 'WEST VIRGINIA'),
        ('WY', 'WYOMING');

        CREATE INDEX ON state_abbrevs (name);
        """
