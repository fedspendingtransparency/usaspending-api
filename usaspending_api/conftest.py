import docker
import logging
import os
import pytest
import tempfile

from django.conf import settings
from django.core.management import call_command
from django.db import connections, connection
from django.test import override_settings
from pathlib import Path

from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import (
    ensure_view_exists,
    ensure_business_categories_functions_exist,
)
from usaspending_api.common.sqs.sqs_handler import (
    FAKE_QUEUE_DATA_PATH,
    UNITTEST_FAKE_QUEUE_NAME,
    _FakeUnitTestFileBackedSQSQueue,
)
from usaspending_api.common.helpers.generic_helper import generate_matviews
from usaspending_api.conftest_helpers import (
    TestElasticSearchIndex,
    ensure_broker_server_dblink_exists,
    remove_unittest_queue_data_files,
)


logger = logging.getLogger("console")


def pytest_configure():
    for connection_name in connections:
        host = connections[connection_name].settings_dict.get("HOST")
        if "amazonaws" in host:
            raise RuntimeError(
                "Connection '{}' appears to be pointing to an AWS database: [{}]".format(connection_name, host)
            )


def pytest_addoption(parser):
    parser.addoption("--local", action="store", default="true")


def delete_tables_for_tests():
    """
    Outside of testing, the transaction_search table is created by using a series of chunked matviews that are combined
    into a Django managed table. When unit testing transaction_search is created as a single matview. To prevent a
    naming conflict, the unused Django managed table is deleted while testing.
    """
    with connection.cursor() as cursor:
        try:
            cursor.execute("DROP TABLE IF EXISTS transaction_search;")
        except Exception:
            pass


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
        db_cfg = setup_databases(verbosity=request.config.option.verbose, interactive=False, **setup_databases_args)
        # If migrations are skipped, assume matviews and views are not to be (re)created either
        # Other scenarios (such as reuse or keep DB) may still lead to creation of a non-existent DB, so they must be
        # (re)created under those conditions
        if not django_db_use_migrations:
            logger.warning(
                "Skipping generation of materialized views or other views in this test run because migrations are also "
                "being skipped. "
            )
        else:
            delete_tables_for_tests()
            generate_matviews(materialized_views_as_traditional_views=True)
            ensure_view_exists(settings.ES_TRANSACTIONS_ETL_VIEW_NAME)
            ensure_view_exists(settings.ES_AWARDS_ETL_VIEW_NAME)
            ensure_business_categories_functions_exist()
            call_command("load_broker_static_data")

    def teardown_database():
        with django_db_blocker.unblock():
            try:
                teardown_databases(db_cfg, verbosity=request.config.option.verbose)
            except Exception as exc:
                request.node.warn(pytest.PytestWarning("Error when trying to teardown test databases: %r" % exc))

    if not django_db_keepdb:
        request.addfinalizer(teardown_database)


@pytest.fixture
def elasticsearch_transaction_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    transaction index.  To use, create some mock database data then call
    elasticsearch_transaction_index.update_index to populate Elasticsearch.

    See test_demo_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("transaction")
    with override_settings(ES_TRANSACTIONS_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture
def elasticsearch_award_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    award index.  To use, create some mock database data then call
    elasticsearch_award_index.update_index to populate Elasticsearch.

    See test_award_index_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("award")
    with override_settings(ES_AWARDS_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture
def elasticsearch_account_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    account index.  To use, create some mock database data then call
    elasticsearch_account_index.update_index to populate Elasticsearch.

    See test_account_index_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("covid19-faba")
    with override_settings(ES_COVID19_FABA_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture(scope="session")
def broker_db_setup(django_db_setup, django_db_use_migrations):
    """Fixture to use during a pytest session if you will run integration tests that requires an actual broker
    database on the other end.

    This will use the Broker test database that Django sets up for the "data_broker" entry in settings.DATABASES,
    which is likely to be named "test_data_broker" by Django.
    """
    if not django_db_use_migrations:
        logger.info("broker_db_setup: Skipping execution of broker DB migrations because --nomigrations flag was used.")
        return

    broker_config_env_envvar = "integrationtest"
    broker_docker_image = "dataact-broker-backend:latest"
    broker_src_dir_path_obj = settings.REPO_DIR.parent / "data-act-broker-backend"
    broker_src_target = "/data-act/backend"
    broker_config_dir = "dataactcore"
    broker_config_copy_target = "/tmp/" + broker_config_dir + "/"
    broker_config_mask_target = broker_src_target + "/" + broker_config_dir + "/"
    broker_example_default_config_file = "config_example.yml"
    broker_example_env_config_file = "local_config_example.yml"
    broker_example_env_secrets_file = "local_secrets_example.yml"
    broker_integrationtest_default_config_file = "config.yml"
    broker_integrationtest_config_file = f"{broker_config_env_envvar}_config.yml"
    broker_integrationtest_secrets_file = f"{broker_config_env_envvar}_secrets.yml"

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

    # Check required Broker docker image is available
    found = docker_client.images.list(name=broker_docker_image)
    if not found:
        logger.error("Error finding required docker image in images registry: {}".format(broker_docker_image))
        pytest.skip(
            "Could not find the Docker Image named {} used to setup a broker database. "
            "Skipping tests of integration with the Broker DB".format(broker_docker_image)
        )

    # Check that the Broker source code is checked out
    if not Path.exists(broker_src_dir_path_obj):
        logger.error("Error finding required broker source code at path: {}".format(broker_src_dir_path_obj))
        pytest.skip(
            "Could not find the Broker source code checked out next to this repo's source code, at {}. "
            "Skipping tests of integration with the Broker DB".format(broker_src_dir_path_obj)
        )

    # Run the DB setup script using the Broker docker image.
    if "data_broker" not in settings.DATABASES:
        logger.error("Did not find 'data_broker' database configured in django settings.DATABASES.")
        raise Exception(
            "'data_broker' database not configured in django settings.DATABASES. "
            "Do you have the environment variable set for this database connection string?"
        )
    broker_test_db_name = settings.DATABASES["data_broker"]["NAME"]

    # Using a combination of mounts to copy the broker source code into the container, and create a modifiable copy
    # of the broker config dir as a tmpfs, so those modifications are not seen in the host source, but only from
    # within the container
    # NOTE: tmpfs layered over the src bind mount makes the config dir empty, so another bind mount is used as the
    # source to copy the config back into the tmpfs mount
    mounted_src = docker.types.Mount(type="bind", source=str(broker_src_dir_path_obj), target=broker_src_target)
    mounted_broker_config_copy = docker.types.Mount(
        type="bind",
        source=str(broker_src_dir_path_obj / broker_config_dir),
        target=broker_config_copy_target,
        read_only=True,
    )
    mounted_broker_config = docker.types.Mount(type="tmpfs", source=None, target=broker_config_mask_target)

    broker_config_file_cmds = rf"""                                                               \
        cp -a {broker_config_copy_target}* {broker_src_target}/{broker_config_dir}/;              \
        cp {broker_src_target}/{broker_config_dir}/{broker_example_default_config_file}           \
           {broker_src_target}/{broker_config_dir}/{broker_integrationtest_default_config_file};  \
        cp {broker_src_target}/{broker_config_dir}/{broker_example_env_config_file}               \
           {broker_src_target}/{broker_config_dir}/{broker_integrationtest_config_file};          \
        cp {broker_src_target}/{broker_config_dir}/{broker_example_env_secrets_file}              \
           {broker_src_target}/{broker_config_dir}/{broker_integrationtest_secrets_file};         \
    """

    # Setup Broker config files to work with the same DB configured via the Broker DB URL env var
    # This will ensure that when the broker script is run, it uses the same test broker DB
    broker_db_config_cmds = rf"""                                                                 \
        sed -i.bak -E "s/host:.*$/host: {settings.DATABASES["data_broker"]["HOST"]}/"             \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_config_file};         \
        sed -i.bak -E "s/port:.*$/port: {settings.DATABASES["data_broker"]["PORT"]}/"             \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_config_file};         \
        sed -i.bak -E "s/username:.*$/username: {settings.DATABASES["data_broker"]["USER"]}/"     \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_secrets_file};        \
        sed -i.bak -E "s/password:.*$/password: {settings.DATABASES["data_broker"]["PASSWORD"]}/" \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_secrets_file};        \
    """

    # Python script in broker-code that will run migrations and other db setup in the broker test DB
    broker_db_setup_cmd = rf"""                                                                   \
        python {broker_src_target}/{broker_config_dir}/scripts/setup_all_db.py --dbname {broker_test_db_name};
    """

    broker_container_command = "sh -cex '" + broker_config_file_cmds + broker_db_config_cmds + broker_db_setup_cmd + "'"
    logger.info(f"Running following command in a container of image {broker_docker_image}: {broker_container_command}")
    logger.info(f"Container will access Broker scripts from mounted source dir: {broker_src_dir_path_obj}")

    # NOTE: use of network_mode="host" applies ONLY when on Linux
    # It allows docker to resolve network addresses (like "localhost") as if running from the docker host
    log_gen = docker_client.containers.run(
        broker_docker_image,
        broker_container_command,
        remove=True,
        network_mode="host",
        environment={"env": broker_config_env_envvar},
        mounts=[mounted_src, mounted_broker_config_copy, mounted_broker_config],
        stderr=True,
        stream=True,
    )
    [logger.info(str(log)) for log in log_gen]  # log container logs from returned streaming log generator
    logger.info("Command ran to completion in container. Broker DB should be setup for tests.")


@pytest.fixture(scope="session")
def broker_server_dblink_setup(django_db_blocker, broker_db_setup):
    """Fixture to use during a pytest session if you will run integration tests connecting to the broker DB via dblink."""
    with django_db_blocker.unblock():
        ensure_broker_server_dblink_exists()


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


@pytest.fixture(scope="session")
def unittest_fake_sqs_queue_instance():
    fake_unittest_q = _FakeUnitTestFileBackedSQSQueue.instance()
    yield fake_unittest_q


@pytest.fixture(scope="session")
def local_queue_dir(unittest_fake_sqs_queue_instance):
    FAKE_QUEUE_DATA_PATH.mkdir(parents=True, exist_ok=True)
    yield
    # Clean up any files created on disk
    remove_unittest_queue_data_files(unittest_fake_sqs_queue_instance)


@pytest.fixture()
def fake_sqs_queue(local_queue_dir, unittest_fake_sqs_queue_instance):
    q = unittest_fake_sqs_queue_instance

    # Check that it's the unit test queue before purging
    assert q.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME
    q.purge()
    q.reset_instance_state()
    yield
    q.purge()
    q.reset_instance_state()
