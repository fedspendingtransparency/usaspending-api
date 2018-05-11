from django.conf import settings
import subprocess
import pytest
from django.db import connection

ENUM_FILE = ['database_scripts/matviews/functions_and_enums.sql']

TEMP_SQL_FILES = ['../matviews/universal_transaction_matview.sql',
                  '../matviews/universal_award_matview.sql',
                  '../matviews/summary_transaction_view.sql',
                  '../matviews/summary_transaction_month_view.sql',
                  '../matviews/summary_transaction_geo_view.sql',
                  '../matviews/summary_award_view.sql',
                  '../matviews/summary_view_cfda_number.sql',
                  '../matviews/summary_view_naics_codes.sql',
                  '../matviews/summary_view_psc_codes.sql',
                  '../matviews/summary_view.sql']


def pytest_addoption(parser):
    parser.addoption("--local", action="store", default="false")


@pytest.fixture(scope='session')
def local(request):
    return request.config.getoption("--local")


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up another test db, remove everything but the default
    # DATABASE_URL from the list of databases in django settings
    test_db = settings.DATABASES.pop('default', None)
    settings.DATABASES.clear()
    settings.DATABASES['default'] = test_db
    # Also remove any database routers
    settings.DATABASE_ROUTERS.clear()


@pytest.fixture(scope='session')
def django_db_setup(django_db_blocker,
                    django_db_keepdb,
                    request,
                    local):
    #if local == "true":
    with django_db_blocker.unblock():
        with connection.cursor() as c:
                subprocess.call("python database_scripts/matview_generator/matview_sql_generator.py ", shell=True)
                for file in get_sql(TEMP_SQL_FILES):
                    c.execute(file)
    return
    '''else:
        from pytest_django.compat import setup_databases, teardown_databases

        setup_databases_args = {}

        with django_db_blocker.unblock():
            db_cfg = setup_databases(
                verbosity=pytest.config.option.verbose,
                interactive=False,
                **setup_databases_args
            )

        def teardown_database():
            with django_db_blocker.unblock():
                teardown_databases(
                    db_cfg,
                    verbosity=pytest.config.option.verbose,
                )

        if not django_db_keepdb:
            request.addfinalizer(teardown_database)'''


def get_sql(sql_files):
    data = []
    for file in sql_files:
        with open(file, 'r') as myfile:
            data.append(myfile.read())
    return data
