from unittest import SkipTest

import psycopg2

from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string, get_database_dsn_string


def test_can_connect_to_broker():
    """Simple 'integration test' that checks a Broker DB exists to integrate with"""
    with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT now()")
            results = cursor.fetchall()
    assert results is not None
    assert len(str(results[0][0])) > 0


def test_can_connect_to_broker_by_dblink():
    """Simple 'integration test' that checks the USAspending to Broker dblink works

    It will be skipped if the `broker_server` server is not created in the USAspending database-under-test
    """
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select srvname from pg_foreign_server where srvname = 'broker_server'")
            results = cursor.fetchall()
            if not results or not results[0][0] == "broker_server":
                raise SkipTest(
                    "No foreign server named 'broker_server' has been setup on this USAspending database. "
                    "Skipping the test of integration with that server via dblink"
                )
            cursor.execute("SELECT * FROM dblink('broker_server','SELECT now()') AS broker_time(the_now timestamp)")
            results = cursor.fetchall()
    assert results is not None
    assert len(str(results[0][0])) > 0
