from django.conf import settings


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up a data_broker test db, remove it
    # from the list of databases in django settings
    settings.DATABASES.pop('data_broker', None)
