import json
import os
import dj_database_url
from django.db import connection


def dictfetchall(cursor):
    if isinstance(cursor, PhonyCursor):
        return cursor.results
    else:
        "Return all rows from a cursor as a dict"
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


class PhonyCursor:
    """Spoofs the db cursor responses."""

    def __init__(self):
        with open(
                os.path.join(
                    os.path.dirname(__file__),
                    'tests/etl_test_data.json')) as json_data:
            self.db_responses = json.load(json_data)

        self.results = None

    def execute(self, statement, parameters):
        self.results = None
        for key in self.db_responses.keys():
            if "".join(key.split()) == "".join(statement.split(
            )):  # Ignore whitespace in the query
                self.results = self.db_responses[key]


def setup_broker_fdw(broker_submission_id):
    """Prepares the foreign data wrapper used to query the broker db.
    """
    broker_conn = dj_database_url.parse(
        os.environ.get('DATA_BROKER_DATABASE_URL'), conn_max_age=600)

    commands = [
        """CREATE EXTENSION IF NOT EXISTS postgres_fdw""",
        """DROP SERVER IF EXISTS broker_fdw_server CASCADE""",
        """CREATE SERVER broker_fdw_server
           FOREIGN DATA WRAPPER postgres_fdw
           OPTIONS (host '{HOST}', port '{PORT}', dbname '{NAME}')
        """.format(**broker_conn),
        """CREATE USER MAPPING FOR CURRENT_USER
            SERVER broker_fdw_server
            OPTIONS (user '{USER}', password '{PASSWORD}');
        """.format(**broker_conn),
        """DROP SCHEMA IF EXISTS broker CASCADE""",
        """CREATE SCHEMA broker""",
        """IMPORT FOREIGN SCHEMA public
           LIMIT TO (submission, appropriation, award_procurement)
           FROM SERVER broker_fdw_server
           INTO broker """,
        """DROP SCHEMA IF EXISTS local_broker CASCADE""",
        """CREATE SCHEMA local_broker""",
        """CREATE TABLE local_broker.award_procurement AS
           SELECT * FROM broker.award_procurement ap
           WHERE ap.submission_id = %s""",

        # todo: guarantee last result for each view?

        """CREATE OR REPLACE VIEW local_broker.location_by_award_procurement AS
           SELECT p.award_procurement_id,
                  l.location_id
           FROM   local_broker.award_procurement p
           JOIN   references_location l ON (
                    COALESCE(p.legal_entity_country_code, '') = COALESCE(l.location_country_code, '')
                    -- not country name, that is looked up in ref_country_code from code
                AND COALESCE(p.legal_entity_state_code, '') = COALESCE(l.state_code, '')
                AND COALESCE(p.legal_entity_city_name, '') = COALESCE(l.city_name, '')
                AND COALESCE(p.legal_entity_address_line1, '') = COALESCE(l.address_line1, '')
                AND COALESCE(p.legal_entity_address_line2, '') = COALESCE(l.address_line2, '')
                AND COALESCE(p.legal_entity_address_line3, '') = COALESCE(l.address_line3, '')
                AND COALESCE(p.legal_entity_zip4, '') = COALESCE(l.zip4, '')
                AND COALESCE(p.legal_entity_congressional, '') = COALESCE(l.congressional_code, '')
                AND l.recipient_flag)
        """,

        """CREATE OR REPLACE VIEW local_broker.place_of_performance_by_award_procurement AS
           SELECT p.award_procurement_id,
                  l.location_id
           FROM   local_broker.award_procurement p
           JOIN   references_location l ON (
                    COALESCE(p.place_of_perform_country_c, '') = COALESCE(l.location_country_code, '')
                AND COALESCE(p.place_of_performance_state, '') = COALESCE(l.state_code, '')
                AND COALESCE(p.place_of_performance_locat, '') = COALESCE(l.city_name, '')
                AND COALESCE(p.place_of_performance_zip4a, '') = COALESCE(l.zip4, '')  -- TODO - that zip4a to zip4
                AND COALESCE(p.place_of_performance_congr, '') = COALESCE(l.congressional_code, '')
                AND l.place_of_performance_flag)
        """,

# select count(location_id)
# from   broker.place_of_performance_by_award_procurement
# join   broker.award_procurement ap using (award_procurement_id)
# where  ap.submission_id = 3927;
#
#  count
# -------
#      0
# (1 row)
#
# Time: 1486.160 ms


        """CREATE OR REPLACE VIEW local_broker.recipient_by_award_procurement AS
           SELECT p.award_procurement_id,
                  le.legal_entity_id
           FROM   local_broker.award_procurement p
           JOIN   local_broker.location_by_award_procurement lbap ON (p.award_procurement_id = lbap.award_procurement_id)
           JOIN   legal_entity le ON (    p.awardee_or_recipient_uniqu = le.recipient_unique_id  -- # match DUNS
                                      AND p.vendor_doing_as_business_n = le.vendor_doing_as_business_name
                                      AND p.awardee_or_recipient_legal = le.recipient_name
                                      AND lbap.location_id = le.location_id)
        """,

        """CREATE OR REPLACE VIEW local_broker.award_by_award_procurement AS
           SELECT p.award_procurement_id,
                  a.id AS award_id
           FROM   local_broker.award_procurement p
           JOIN   awards a ON (p.piid = a.piid)
           LEFT OUTER JOIN   awards parent_award ON
               (    a.parent_award_id = parent_award.id
                AND p.parent_award_id = parent_award.piid)   -- what if parent award is null?
           LEFT OUTER JOIN local_broker.place_of_performance_by_award_procurement popbap ON
               (p.award_procurement_id = popbap.award_procurement_id )
           JOIN local_broker.recipient_by_award_procurement rbap ON
               (    p.award_procurement_id = rbap.award_procurement_id )
               """,

           ]

    with connection.cursor() as cursor:
        for sql_command in commands:
            params = [broker_submission_id, ] * sql_command.count('%s')
            cursor.execute(sql_command, params)
