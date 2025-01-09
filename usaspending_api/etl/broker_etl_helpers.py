import json
import logging
import os

from django.conf import settings
from django.db import connection, connections

from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher


logger = logging.getLogger("script")


def dictfetchall(cursor):
    if isinstance(cursor, PhonyCursor):
        if not cursor.results:
            return []
        return cursor.results
    return ordered_dictionary_fetcher(cursor)


class PhonyCursor:
    """Spoofs the db cursor responses."""

    def __init__(self, test_file=None):
        if not test_file:
            test_file = os.path.join(os.path.dirname(__file__), "tests/etl_test_data.json")
        with open(test_file) as json_data:
            self.db_responses = json.load(json_data)

        self.results = None

    def execute(self, statement, parameters=None):
        if parameters:
            statement %= tuple(parameters)

        self.results = None
        for key in self.db_responses.keys():
            if "".join(key.split()) == "".join(statement.split()):  # Ignore whitespace in the query
                self.results = self.db_responses[key]


def setup_broker_fdw():

    with connection.cursor() as cursor:
        with open("usaspending_api/etl/management/setup_broker_fdw.sql") as infile:
            logger.info(connections.databases[settings.DATA_BROKER_DB_ALIAS])
            for raw_sql in infile.read().split("\n\n\n"):
                logger.info("SETUP BROKER FDW: Running SQL => " + str(raw_sql))
                cursor.execute(raw_sql, connections.databases[settings.DATA_BROKER_DB_ALIAS])
