import psycopg2

from usaspending_api.common.helpers.sql_helpers import get_broker_dsn_string


def test_can_connect_to_broker():
    """Simple 'integration test' that checks a Broker DB exists to integrate with"""
    with psycopg2.connect(dsn=get_broker_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT now()")
            results = cursor.fetchall()
    assert results is not None
    assert len(str(results[0][0])) > 0
