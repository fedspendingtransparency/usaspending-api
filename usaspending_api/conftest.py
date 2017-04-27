from django.conf import settings


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up another test db, remove everything but the default
    # DATABASE_URL from the list of databases in django settings
    test_db = settings.DATABASES.pop('default', None)
    settings.DATABASES.clear()
    settings.DATABASES['default'] = test_db
    # Also remove any database routers
    settings.DATABASE_ROUTERS.clear()
