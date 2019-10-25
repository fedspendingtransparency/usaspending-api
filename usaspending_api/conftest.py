import logging
import os

import docker
import pytest
import tempfile

from django.conf import settings
from django.db import DEFAULT_DB_ALIAS
from django.test import override_settings
from django_mock_queries.query import MockSet
from pathlib import Path
from usaspending_api.common.helpers.generic_helper import generate_matviews
from usaspending_api.common.matview_manager import MATERIALIZED_VIEWS
from usaspending_api.conftest_helpers import (
    TestElasticSearchIndex,
    ensure_transaction_delta_view_exists,
    ensure_broker_server_dblink_exists
)
from usaspending_api.etl.broker_etl_helpers import PhonyCursor


logger = logging.getLogger("console")
VALID_DB_CURSORS = [DEFAULT_DB_ALIAS, "data_broker"]


@pytest.fixture()
def mock_db_cursor(monkeypatch, request):
    db_cursor_dict = request.param

    for cursor in VALID_DB_CURSORS:
        if cursor in db_cursor_dict:
            data_file_key = cursor + "_data_file"

            if data_file_key not in db_cursor_dict:
                # Missing data file for the mocked cursor. Skipping PhonyCursor setup.
                continue

            db_cursor_dict[cursor].cursor.return_value = PhonyCursor(db_cursor_dict[data_file_key])
            del db_cursor_dict[data_file_key]

    monkeypatch.setattr("django.db.connections", db_cursor_dict)


@pytest.fixture()
def mock_psc(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_psc_qs = MockSet()

    monkeypatch.setattr("usaspending_api.references.models.PSC.objects", mock_psc_qs)

    yield mock_psc_qs

    mock_psc_qs.delete()


@pytest.fixture()
def mock_cfda(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_cfda_qs = MockSet()

    monkeypatch.setattr("usaspending_api.references.models.Cfda.objects", mock_cfda_qs)

    yield mock_cfda_qs

    mock_cfda_qs.delete()


@pytest.fixture()
def mock_naics(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_naics_qs = MockSet()
    monkeypatch.setattr("usaspending_api.references.models.NAICS.objects", mock_naics_qs)
    yield mock_naics_qs
    mock_naics_qs.delete()


@pytest.fixture()
def mock_recipients(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_recipient_qs = MockSet()

    monkeypatch.setattr("usaspending_api.references.models.LegalEntity.objects", mock_recipient_qs)

    yield mock_recipient_qs

    mock_recipient_qs.delete()


@pytest.fixture()
def mock_federal_account(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_federal_accounts_qs = MockSet()

    monkeypatch.setattr("usaspending_api.accounts.models.FederalAccount.objects", mock_federal_accounts_qs)

    yield mock_federal_accounts_qs

    mock_federal_accounts_qs.delete()


@pytest.fixture()
def mock_tas(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_tas_qs = MockSet()

    monkeypatch.setattr("usaspending_api.accounts.models.TreasuryAppropriationAccount.objects", mock_tas_qs)

    yield mock_tas_qs

    mock_tas_qs.delete()


@pytest.fixture()
def mock_award(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_award_qs = MockSet()

    monkeypatch.setattr("usaspending_api.awards.models.Award.objects", mock_award_qs)

    yield mock_award_qs

    mock_award_qs.delete()


@pytest.fixture()
def mock_subaward(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_subaward_qs = MockSet()

    monkeypatch.setattr("usaspending_api.awards.models.Subaward.objects", mock_subaward_qs)

    yield mock_subaward_qs

    mock_subaward_qs.delete()


@pytest.fixture()
def mock_financial_account(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_financial_accounts_qs = MockSet()

    monkeypatch.setattr("usaspending_api.awards.models.FinancialAccountsByAwards.objects", mock_financial_accounts_qs)

    yield mock_financial_accounts_qs

    mock_financial_accounts_qs.delete()


@pytest.fixture()
def mock_transaction(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_transaciton_qs = MockSet()

    monkeypatch.setattr("usaspending_api.awards.models.TransactionNormalized.objects", mock_transaciton_qs)

    yield mock_transaciton_qs

    mock_transaciton_qs.delete()


@pytest.fixture()
def mock_agencies(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_agency_qs = MockSet()
    mock_toptier_agency_qs = MockSet()
    mock_subtier_agency_qs = MockSet()

    monkeypatch.setattr("usaspending_api.references.models.Agency.objects", mock_agency_qs)
    monkeypatch.setattr("usaspending_api.references.models.ToptierAgency.objects", mock_toptier_agency_qs)
    monkeypatch.setattr("usaspending_api.references.models.SubtierAgency.objects", mock_subtier_agency_qs)

    mocked_agencies = {
        "agency": mock_agency_qs,
        "toptier_agency": mock_toptier_agency_qs,
        "subtier_agency": mock_subtier_agency_qs,
    }

    yield mocked_agencies

    mock_agency_qs.delete()
    mock_toptier_agency_qs.delete()
    mock_subtier_agency_qs.delete()


@pytest.fixture()
def mock_matviews_qs(monkeypatch):
    """Mocks all matvies to a single mock queryset"""
    mock_qs = MockSet()  # mock queryset
    for k, v in MATERIALIZED_VIEWS.items():
        if k not in ["tas_autocomplete_matview"]:
            monkeypatch.setattr(
                "usaspending_api.awards.models_matviews.{}.objects".format(v["model"].__name__), mock_qs
            )

    yield mock_qs

    mock_qs.delete()


def pytest_addoption(parser):
    parser.addoption("--local", action="store", default="true")


@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("--local")


@pytest.fixture(scope="session")
def django_db_setup(
    request,
    django_test_environment,
    django_db_blocker,
    django_db_use_migrations,
    django_db_keepdb,
    django_db_createdb,
    django_db_modify_db_settings,
):
    """This is an override of the original implementation in the https://github.com/pytest-dev/pytest-django plugin
    from file /pytest-django/fixtures.py.

    Because this "hides" the original implementation, it may get out-of-date as that plugin is upgraded. This override
    takes the implementation from pytest-django Release 3.5.1, and extends it in order to execute materialized views
    as part of database setup.

    If requirements.txt shows a different version than the one this is based on: compare, update, and test.
    More work could be put into trying to patch, replace, or wrap implementation of
    ``django.test.utils.setup_databases``, which is the actual method that needs to be wrapped and extended.
    """
    from pytest_django.compat import setup_databases, teardown_databases
    from pytest_django.fixtures import _disable_native_migrations

    setup_databases_args = {}

    if not django_db_use_migrations:
        _disable_native_migrations()

    if django_db_keepdb and not django_db_createdb:
        setup_databases_args["keepdb"] = True

    with django_db_blocker.unblock():
        db_cfg = setup_databases(
            verbosity=request.config.option.verbose,
            interactive=False,
            **setup_databases_args
        )
        # Matviews and views can't be crated under these conditions, because either their underlying schema objects
        # would not exist, or we want to reuse a DB and not recreate these objects
        skip_views = not django_db_use_migrations or (django_db_keepdb and not django_db_createdb)
        if skip_views:
            logger.warning(
                "Not generating materialized views or other views in this test run because one of: "
                "1) migrations are not running in this session and their underlying schema objects won't exist, or "
                "2) A prior DB is being reused for this test session"
            )
        else:
            generate_matviews()
            ensure_transaction_delta_view_exists()

    def teardown_database():
        with django_db_blocker.unblock():
            try:
                teardown_databases(db_cfg, verbosity=request.config.option.verbose)
            except Exception as exc:
                request.node.warn(
                    pytest.PytestWarning(
                        "Error when trying to teardown test databases: %r" % exc
                    )
                )

    if not django_db_keepdb:
        request.addfinalizer(teardown_database)


@pytest.fixture()
def refresh_matviews():
    generate_matviews()


@pytest.fixture
def elasticsearch_transaction_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    transaction index.  To use, create some mock database data then call
    elasticsearch_transaction_index.update_index to populate Elasticsearch.

    See test_demo_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex()
    with override_settings(TRANSACTIONS_INDEX_ROOT=elastic_search_index.alias_prefix):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture(scope="session")
def broker_db_setup(django_db_setup):
    """Fixture to use during a pytest session if you will run integration tests that requires an actual broker
    database on the other end.

    This will use the Broker test database that Django sets up for the "data_broker" entry in settings.DATABASES,
    which is likely to be named "test_data_broker" by Django.
    """
    broker_docker_image = "dataact-broker-backend:latest"
    broker_src_dir_path_obj = Path(settings.BASE_DIR).resolve().parent / "data-act-broker-backend"
    broker_docker_volume_target = "/data-act/backend"

    docker_client = docker.from_env()

    # Check Docker is running
    try:
        docker_client.ping()
    except Exception:
        logger.error("Error connecting to the Docker daemon. Is the Docker daemon running?")
        pytest.skip(
            "Docker is not running, so a broker database cannot be setup. "
            "Skipping tests of integration with the Broker DB"
        )
        yield None

    # Check required Broker docker image is available
    found = docker_client.images.list(name=broker_docker_image)
    if not found:
        logger.error("Error finding required docker image in images registry: {}".format(broker_docker_image))
        pytest.skip(
            "Could not find the Docker Image named {} used to setup a broker database. "
            "Skipping tests of integration with the Broker DB".format(broker_docker_image)
        )
        yield None

    # Check that the Broker source code is checked out
    if not Path.exists(broker_src_dir_path_obj):
        logger.error("Error finding required broker source code at path: {}".format(broker_src_dir_path_obj))
        pytest.skip(
            "Could not find the Broker source code checked out next to this repo's source code, at {}. "
            "Skipping tests of integration with the Broker DB".format(broker_src_dir_path_obj)
        )
        yield None

    # Run the DB setup script using the Broker docker image.
    if "data_broker" not in settings.DATABASES:
        logger.error("Did not find 'data_broker' database configured in django settings.DATABASES.")
        raise Exception("'data_broker' database not configured in django settings.DATABASES. "
                        "Do you have the environment variable set for this database connection string?")
    broker_test_db_name = settings.DATABASES["data_broker"]["NAME"]
    mounted_src = docker.types.Mount(
        type="bind",
        source=str(broker_src_dir_path_obj),
        target=broker_docker_volume_target
    )
    broker_db_setup_command = "python dataactcore/scripts/setup_all_db.py --dbname {}".format(broker_test_db_name)
    logger.info("Running command `{}` in a container of image {}".format(broker_db_setup_command, broker_docker_image))
    logger.info("Container will access Broker scripts from mounted source dir: {}".format(broker_src_dir_path_obj))
    log_gen = docker_client.containers.run(
        broker_docker_image,
        broker_db_setup_command,
        mounts=[mounted_src],
        stderr=True,
        stream=True
    )
    [logger.info(str(log)) for log in log_gen]  # log container logs from returned streaming log generator
    logger.info("Command ran to completion in container. Broker DB should be setup for tests.")
    yield


@pytest.fixture(scope="session")
def broker_server_dblink_setup(django_db_blocker, broker_db_setup):
    """Fixture to use during a pytest session if you will run integration tests connecting to the broker DB via dblink.


    """
    # TODO: Same thing that .travis.yml does
    # TODO: 1) get broker-db-under-test conn, and CURRENT usaspending db-under-test conn
    # TODO: 2) run the extensions.sql file to ensure extensions are on that usaspending db
    # TODO: 3) replace the env vars in the broker_server.sql script
    # TODO: 4) run the broker_server.sql script
    with django_db_blocker.unblock():
        ensure_broker_server_dblink_exists()


@pytest.fixture(scope="function")
def self_cleaning_broker_db(broker_db_setup, broker_server_dblink_setup):
    """Fixture to use on a pytest integration test that puts data into the integration-testing broker DB, which needs
    too be removed afterward.
    """
    # TODO: docker exec command inside broker image container that removes all table data?
    # TODO: Or ... just connect to that broker db and run SQL to delete all table data?
    # Probably don't want to remove all table data, since the broker setup_all_db command sets up good reference data
    # Is there a way to revert to a point in time of the transaction log?
    # Maybe save off a pg_dump of the data and tables that setup_all_db did, and use that to nuke and pave the DB
    # each time?
    pass


@pytest.fixture
def temp_file_path():
    """
    If you need a temp file for something... anything... but you don't want
    the file to actually exist and you don't want to deal with managing its
    lifetime, use this fixture.
    """
    # This actually creates the temporary file, but once the context manager
    # exits, it will delete the file leaving the filename free for you to use.
    with tempfile.NamedTemporaryFile() as tf:
        path = tf.name

    yield path

    # For convenience.  Don't care if it fails.
    try:
        os.remove(path)
    except Exception:
        pass
