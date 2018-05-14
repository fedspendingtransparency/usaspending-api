from django.conf import settings
import pytest

from usaspending_api.common.helpers import generate_matviews


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up another test db, remove everything but the default
    # DATABASE_URL from the list of databases in django settings
    test_db = settings.DATABASES.pop('default', None)
    settings.DATABASES.clear()
    settings.DATABASES['default'] = test_db
    # Also remove any database routers
    settings.DATABASE_ROUTERS.clear()


def pytest_addoption(parser):
    parser.addoption("--local", action="store", default="true")


@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("--local")


@pytest.fixture(scope='session')
def django_db_setup(django_db_blocker,
                    django_db_keepdb,
                    local,
                    request):

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
        request.addfinalizer(teardown_database)


@pytest.fixture()
def refresh_matviews():
    generate_matviews()
