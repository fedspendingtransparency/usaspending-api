from django.conf import settings
import pytest


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up a data_broker test db, remove it
    # from the list of databases in django settings
    settings.DATABASES.pop('data_broker', None)


@pytest.fixture(scope='session')
def django_db_keepdb(request):
    return True
