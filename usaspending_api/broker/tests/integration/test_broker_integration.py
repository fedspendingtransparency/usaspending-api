import pytest

from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS
from django.test import TestCase


class BrokerIntegrationTestCase(TestCase):
    databases = {settings.DEFAULT_DB_ALIAS, settings.DATA_BROKER_DB_ALIAS}
    dummy_table_name = "dummy_broker_table_to_be_rolled_back"

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        # Follow-up of test_broker_transactional_test
        with connections[settings.DATA_BROKER_DB_ALIAS].cursor() as cursor:
            cursor.execute("select * from pg_tables where tablename = '{}'".format(cls.dummy_table_name))
            results = cursor.fetchall()
        assert results is not None
        if len(results) != 0:
            pytest.fail(
                "Test test_broker_transactional_test did not run transactionally. "
                "Creation of table {} in Broker DB was not rolled back and still exists.".format(cls.dummy_table_name)
            )

    @pytest.mark.usefixtures("broker_db_setup")
    def test_can_connect_to_broker(self):
        """Simple 'integration test' that checks a Broker DB exists to integrate with"""
        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:
            cursor.execute("SELECT now()")
            results = cursor.fetchall()
        assert results is not None
        assert len(str(results[0][0])) > 0

    @pytest.mark.usefixtures("broker_db_setup")
    def test_broker_transactional_test(self):
        """Integration test that checks whether Django's default transactional test implementation works against the
        integrated Broker DB too.

        The test creates a dummy table during its execution. If transactional wrapper is working, that table creation
        will be rolled-back after the test completes. This not verified until the ~``tearDownClass`` method runs.

        NOTE: The transaction is only controlled and will only roll-back if you use Django's django.db.connections
        dictionary to get the connection.
        """
        dummy_contents = "dummy_text"

        # Make sure the table and the data get in there
        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:
            cursor.execute("create table {} (contents text)".format(self.dummy_table_name))
            cursor.execute("insert into {} values ('{}')".format(self.dummy_table_name, dummy_contents))
        with connection.cursor() as cursor:
            cursor.execute("select * from pg_tables where tablename = '{}'".format(self.dummy_table_name))
            results = cursor.fetchall()
        assert results is not None
        assert len(str(results[0][0])) > 0
        with connection.cursor() as cursor:
            cursor.execute("select * from {}".format(self.dummy_table_name))
            results = cursor.fetchall()
        assert results is not None
        assert str(results[0][0]) == dummy_contents

    @pytest.mark.usefixtures("broker_db_setup")
    def test_broker_db_fully_setup(self):
        """Simple 'integration test' that checks a Broker DB had its schema setup"""
        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:
            cursor.execute("select * from pg_tables where tablename = 'alembic_version'")
            results = cursor.fetchall()
        assert results is not None
        assert len(results) > 0
        assert len(str(results[0][0])) > 0


def test_can_connect_to_broker_by_dblink(broker_server_dblink_setup, db):
    """Simple 'integration test' that checks the USAspending to Broker dblink works

    It will be skipped if a broker foreign data wrapper is not created in the USAspending database-under-test
    """
    connection = connections[DEFAULT_DB_ALIAS]
    with connection.cursor() as cursor:
        cursor.execute(f"select srvname from pg_foreign_server where srvname = '{settings.DATA_BROKER_DBLINK_NAME}'")
        results = cursor.fetchall()
        if not results or not results[0][0] == settings.DATA_BROKER_DBLINK_NAME:
            pytest.skip(
                f"No foreign server named '{settings.DATA_BROKER_DBLINK_NAME}' has been setup on this "
                "USAspending database.  Skipping the test of integration with that server via dblink"
            )
        cursor.execute(
            f"SELECT * FROM dblink('{settings.DATA_BROKER_DBLINK_NAME}','SELECT now()') "
            "AS broker_time(the_now timestamp)"
        )
        results = cursor.fetchall()
    assert results is not None
    assert len(results) > 0
    assert len(str(results[0][0])) > 0
