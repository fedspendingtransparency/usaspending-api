import json
import os
import dj_database_url
from django.db import connection, connections


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


def setup_broker_fdw():

    with connection.cursor() as cursor:
        with open('usaspending_api/etl/management/setup_broker_fdw.sql') as infile:
            for raw_sql in infile.read().split('\n\n\n'):
                cursor.execute(raw_sql, connections.databases['data_broker'])
