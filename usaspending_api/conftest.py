import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import List

import docker
import pytest
from django.conf import settings
from django.db import connections
from django.test import override_settings
from model_bakery import baker
from pytest_django.fixtures import _set_suffix_to_test_databases
from pytest_django.lazy_django import skip_if_no_django
from xdist.plugin import (
    get_xdist_worker_id,
    is_xdist_worker,
    pytest_xdist_auto_num_workers,
)

from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import (
    ensure_business_categories_functions_exist,
    ensure_view_exists,
)
from usaspending_api.common.helpers.generic_helper import generate_matviews
from usaspending_api.common.helpers.sql_helpers import (
    build_dsn_string,
    execute_sql_simple,
)
from usaspending_api.common.sqs.sqs_handler import (
    UNITTEST_FAKE_QUEUE_NAME,
    _FakeUnitTestFileBackedSQSQueue,
)
from usaspending_api.config import CONFIG

# Compose other supporting conftest_*.py files
from usaspending_api.conftest_helpers import (
    TestElasticSearchIndex,
    ensure_broker_server_dblink_exists,
    is_pytest_xdist_master_process,
    is_safe_for_xdist_setup_or_teardown,
    remove_unittest_queue_data_files,
    transform_xdist_worker_id_to_django_test_db_id,
)

# Compose ALL fixtures from conftest_spark
from usaspending_api.tests.conftest_spark import *  # noqa
from usaspending_api.tests.integration.test_setup_of_test_dbs import TEST_DB_SETUP_TEST_NAME

logger = logging.getLogger("console")

# Baker Settings
# Since baker doesn't support SearchVectorField, we'll pass in a function to return a string in the meantime
baker.generators.add("django.contrib.postgres.search.SearchVectorField", lambda: "VECTORFIELD")
baker.generators.add("usaspending_api.common.custom_django_fields.NumericField", lambda: 0.00)
baker.generators.add("usaspending_api.common.custom_django_fields.BooleanFieldWithDefault", lambda: False)


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: List[pytest.Item]) -> None:
    """A global built-in fixture to pytest that is called at collection, providing a hook to modify collected items.

    In this case, used to add specific marks on tests to allow running groups/sub-groups of tests. These marks added
    here need to be declared for pytest in pyproject.toml

    Args:
        session: pytest test session object, holding details of the invoked pytest run
        config: pytest Config object, holding config details of the inovked pytest run
        items: List of tests of type ``pytest.Item`` that were collected for this pytest session
    """
    for item in items:
        if (
            # For fixtures that setup or interact with a DB
            any(
                fx in ["db", "broker_db_setup", "transactional_db", "django_db_reset_sequences"]
                for fx in getattr(item, "fixturenames", ())
            )
            # For tests marked with this annotation
            or "django_db" in [m.name for m in item.own_markers]  # for tests marked with this annotation
            # For e.g. Django TransactionTestCase
            or (item.cls and hasattr(item.cls, "databases") and len(item.cls.databases) > 0)
        ):
            item.add_marker("database")

        if any("elasticsearch" in fx and "index" in fx for fx in getattr(item, "fixturenames", ())):
            item.add_marker("elasticsearch")

        # Mark all tests using the spark fixture as "spark".
        # Can be selected with -m spark or deselected with -m (not spark)
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker("spark")


def pytest_sessionfinish(session, exitstatus):
    """A global built-in fixture to pytest that is called when all tests in a session complete.
    For parallel execution with xdist, this function is called once in each worker,
    strictly before it is called on the master process.
    NOTE: If you need to log/print, do so to sys.__stderr__ (sys.stderr is captured at this point)
    """
    worker_id = get_xdist_worker_id(session)
    if is_safe_for_xdist_setup_or_teardown(session, worker_id):
        if is_pytest_xdist_master_process(session):
            print("\nRunning pytest_sessionfinish while exiting the xdist 'master' process", file=sys.__stderr__)
        else:
            print(
                "\nRunning pytest_sessionfinish in single process execution (no xdist parallel test sessions)",
                file=sys.__stderr__,
            )
        # Add cleanup below
        pass
    else:
        print(
            f"\nRunning pytest_sessionfinish while exiting the xdist worker process with id = {worker_id}",
            file=sys.__stderr__,
        )
        # Possible per-worker or worker-specific cleanup below. Rare and not recommended to do this.
        pass


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
    The transaction_search/award_search tables are created from data coming from various based tables.
    When unit testing these tables, we can easily account for that logic using a single view.
    To prevent a naming conflict, the unused Django managed table is deleted while testing.
    """
    try:
        tables = []
        for table in tables:
            execute_sql_simple(f"DROP TABLE IF EXISTS {table} CASCADE;")
    except Exception:
        pass


def add_view_protection():
    """
    When unit testing transaction_search is created as a single view. Views can't be deleted from, so a custom rule
    is added to transaction_search to prevent sql errors when deletes are attempted
    """
    try:
        execute_sql_simple("CREATE RULE ts_del_protect AS ON DELETE TO transaction_search DO INSTEAD NOTHING;")
    except Exception:
        pass


@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("--local")


def is_test_db_setup_trigger(request: pytest.FixtureRequest) -> bool:
    """
    Return True if this is a single invocation of the test with name defined in the `TEST_DB_SETUP_TEST_NAME`
    constant, which is invoked independently to pre-establish test databases
    """
    return (
        len(request.node.items) == 1
        and request.node.items[0].originalname == TEST_DB_SETUP_TEST_NAME
        and request.config.option.reuse_db
    )


@pytest.fixture(scope="session")
def django_db_setup(
    request,
    django_test_environment: None,
    django_db_blocker,
    django_db_use_migrations: bool,
    django_db_keepdb: bool,
    django_db_createdb: bool,
    django_db_modify_db_settings: None,  # will call the overridden (below) version of this fixture
):
    """This is an override of the original implementation in the https://github.com/pytest-dev/pytest-django plugin
    from file /pytest-django/fixtures.py.

    Because this "hides" the original implementation, it may get out-of-date as that plugin is upgraded. This override
    takes the implementation from pytest-django Release 4.2.0, and extends it in order to execute materialized views
    as part of database setup.

    If requirements.txt shows a different version than the one this is based on: compare, update, and test.
    More work could be put into trying to patch, replace, or wrap implementation of
    ``django.test.utils.setup_databases``, which is the actual method that needs to be wrapped and extended.
    """
    from django.test.utils import TimeKeeper, setup_databases, teardown_databases
    from pytest_django.fixtures import _disable_migrations

    setup_databases_args = {}

    if not django_db_use_migrations:
        _disable_migrations()

    if django_db_keepdb and not django_db_createdb:
        setup_databases_args["keepdb"] = True

    # NEW (OVERRIDES) of default Django behavior
    setup_databases_args["time_keeper"] = TimeKeeper()
    if is_test_db_setup_trigger(request):
        # This should only be triggered when calling the TEST_DB_SETUP_TEST_NAME unit test all by itself, which is an
        # indicator to execute this fixture in a way where it sets up and leaves around multiple test databases.
        # By handing a parallel arg > 0 value to setup_databases() we use the Django unittest approach to creating
        # parallel copies of the test database rather than the pytest-django approach. The Django approach is faster
        # because it clones the first test db (CREATE DATABASE ... WITH TEMPLATE [for Postgres]) rather than running
        # migrations scripts for each copy to build it up.
        if is_xdist_worker(request):  # running in xdist with workers
            parallel_workers = request.config.workerinput["workercount"]
        elif request.config.option.numprocesses is not None:
            try:
                parallel_workers = int(request.config.option.numprocesses)
            except ValueError:
                parallel_workers = pytest_xdist_auto_num_workers(request.config) or 0
        else:
            parallel_workers = 0
        setup_databases_args["parallel"] = parallel_workers

    with django_db_blocker.unblock():
        db_cfg = setup_databases(verbosity=request.config.option.verbose, interactive=False, **setup_databases_args)
        # If migrations are skipped, assume matviews and views are not to be (re)created either
        # Only inspecting whether the migration option was turned off, since --reuse-db might still cause a DB to be
        # created if there's nothing there to reuse.
        if not django_db_use_migrations:
            logger.warning(
                "Skipping generation of materialized views or other views in this test run because migrations are also "
                "being skipped. "
            )
        else:
            delete_tables_for_tests()
            # If using parallel test runners through pytest-xdist, pass the unique worker ID to handle matview SQL
            # files distinctly for each one
            generate_matviews(
                materialized_views_as_traditional_views=True,
                parallel_worker_id=getattr(request.config, "workerinput", {}).get("workerid"),
            )
            ensure_view_exists(settings.ES_TRANSACTIONS_ETL_VIEW_NAME)
            ensure_view_exists(settings.ES_AWARDS_ETL_VIEW_NAME)
            add_view_protection()
            ensure_business_categories_functions_exist()

            # This is necessary for any script/code run in a test that bases its database connection off the postgres
            # config. This resolves the issue by temporarily mocking the DATABASE_URL to accurately point to the test
            # database.
            old_usas_db_url = CONFIG.DATABASE_URL
            old_usas_ps_db = CONFIG.USASPENDING_DB_NAME

            test_usas_db_name = settings.DATABASES[settings.DEFAULT_DB_ALIAS].get("NAME")
            if test_usas_db_name is None:
                raise ValueError(f"DB 'NAME' for DB alias {settings.DEFAULT_DB_ALIAS} came back as None. Check config.")
            if "test" not in test_usas_db_name:
                raise ValueError(
                    f"DB 'NAME' for DB alias {settings.DEFAULT_DB_ALIAS} does not contain 'test' when expected to."
                )
            CONFIG.USASPENDING_DB_NAME = test_usas_db_name
            CONFIG.DATABASE_URL = build_dsn_string(
                {**settings.DATABASES[settings.DEFAULT_DB_ALIAS], **{"NAME": test_usas_db_name}}
            )

            old_broker_db_url = CONFIG.DATA_BROKER_DATABASE_URL
            old_broker_ps_db = CONFIG.BROKER_DB_NAME

            if settings.DATA_BROKER_DB_ALIAS in settings.DATABASES:
                test_broker_db = settings.DATABASES[settings.DATA_BROKER_DB_ALIAS].get("NAME")
                if test_broker_db is None:
                    raise ValueError(
                        f"DB 'NAME' for DB alias {settings.DATA_BROKER_DB_ALIAS} came back as None. Check config."
                    )
                if "test" not in test_broker_db:
                    raise ValueError(
                        f"DB 'NAME' for DB alias {settings.DATA_BROKER_DB_ALIAS} does not contain 'test' when expected to."
                    )
                CONFIG.BROKER_DB_NAME = test_broker_db
                CONFIG.DATA_BROKER_DATABASE_URL = build_dsn_string(
                    {**settings.DATABASES[settings.DATA_BROKER_DB_ALIAS], **{"NAME": test_broker_db}}
                )

    # This will be added to the finalizer which will be run when the newly made test database is being torn down
    def reset_postgres_dsn():
        CONFIG.DATABASE_URL = old_usas_db_url
        CONFIG.USASPENDING_DB_NAME = old_usas_ps_db

        CONFIG.DATA_BROKER_DATABASE_URL = old_broker_db_url
        CONFIG.BROKER_DB_NAME = old_broker_ps_db

    request.addfinalizer(reset_postgres_dsn)

    def teardown_database():
        with django_db_blocker.unblock():
            try:
                teardown_databases(db_cfg, verbosity=request.config.option.verbose)
            except Exception as exc:
                request.node.warn(pytest.PytestWarning("Error when trying to teardown test databases: %r" % exc))

    if not django_db_keepdb:
        request.addfinalizer(teardown_database)


@pytest.fixture(scope="session")
def django_db_modify_db_settings_xdist_suffix(request):
    """Overriding definition from pytest-django to fuse pytest-xdist with Django test DB setup
    See original code:
    https://github.com/pytest-dev/pytest-django/blob/bd2ae62968aaf97c6efc7e02ff77ba6160865435/pytest_django/fixtures.py#L46
    """
    skip_if_no_django()

    # If running in pytest-xdist, and on a worker process,
    # but part of a regular test session and NOT the test_db_setup prep (don't want a suffix on the first cloned DB)
    if is_xdist_worker(request) and not is_test_db_setup_trigger(request):
        worker_id = get_xdist_worker_id(request)
        suffix = transform_xdist_worker_id_to_django_test_db_id(worker_id)
        _set_suffix_to_test_databases(suffix=suffix)


@pytest.fixture
def elasticsearch_transaction_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    transaction index.  To use, create some mock database data then call
    elasticsearch_transaction_index.update_index to populate Elasticsearch.

    See test_demo_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("transaction")
    with override_settings(
        ES_TRANSACTIONS_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix,
        ES_TRANSACTIONS_WRITE_ALIAS=elastic_search_index.etl_config["write_alias"],
    ):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture
def fake_csv_local_path(tmp_path):
    """A fixture that will mock the settings.CSV_LOCAL_PATH to a temporary directory created by the tmp_path_factory
    fixture, and return the path to that temporary directory.

    Good for use in unit test that download to the local CSV_LOCAL_PATH directory, but you don't want data in that
    directory to create conflicts from test to test or for concurrently running tests.
    """
    tmp_csv_local_path = str(tmp_path) + "/"
    with override_settings(CSV_LOCAL_PATH=tmp_csv_local_path):
        yield tmp_csv_local_path


@pytest.fixture
def elasticsearch_award_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    award index.  To use, create some mock database data then call
    elasticsearch_award_index.update_index to populate Elasticsearch.

    See test_award_index_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("award")
    with override_settings(
        ES_AWARDS_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix,
        ES_AWARDS_WRITE_ALIAS=elastic_search_index.etl_config["write_alias"],
    ):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture
def elasticsearch_subaward_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    subaward index.  To use, create some mock database data then call
    elasticsearch_subaward_index.update_index to populate Elasticsearch.
    """
    elastic_search_index = TestElasticSearchIndex("subaward")
    with override_settings(
        ES_SUBAWARD_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix,
        ES_SUBAWARD_WRITE_ALIAS=elastic_search_index.etl_config["write_alias"],
    ):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture
def elasticsearch_recipient_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    recipient index.  To use, create some mock database data then call
    elasticsearch_recipient_index.update_index to populate Elasticsearch.

    See test_demo_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("recipient")
    with override_settings(
        ES_RECIPIENTS_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix,
        ES_RECIPIENTS_WRITE_ALIAS=elastic_search_index.etl_config["write_alias"],
    ):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture
def elasticsearch_location_index(db):
    """
    Add this fixture to your test if you intend to use the Elasticsearch
    location index.  To use, create some mock database data then call
    elasticsearch_location_index.update_index to populate Elasticsearch.

    See test_demo_elasticsearch_tests.py for sample usage.
    """
    elastic_search_index = TestElasticSearchIndex("location")
    with override_settings(
        ES_LOCATIONS_QUERY_ALIAS_PREFIX=elastic_search_index.alias_prefix,
        ES_LOCATIONS_WRITE_ALIAS=elastic_search_index.etl_config["write_alias"],
    ):
        yield elastic_search_index
        elastic_search_index.delete_index()


@pytest.fixture(scope="session")
def broker_db_setup(django_db_setup, django_db_use_migrations, worker_id):
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
    broker_src_dir_name = "data-act-broker-backend"
    broker_src_dir_path_obj = (
        Path(os.environ.get("DATA_BROKER_SRC_PATH"))  # will be set if running in test container
        if os.environ.get("DATA_BROKER_SRC_PATH")
        else settings.REPO_DIR.parent / broker_src_dir_name
    )
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

    # Check that a Connection to the Broker DB has been configured
    if settings.DATA_BROKER_DB_ALIAS not in settings.DATABASES:
        logger.error("Error finding 'data_broker' database configured in django settings.DATABASES.")
        pytest.skip(
            "'data_broker' database not configured in django settings.DATABASES. "
            "Do you have the environment variable set for this database connection string? "
            "Skipping test of integration with the Broker DB"
        )

    docker_client = docker.from_env()

    # Check Docker is running
    try:
        docker_client.ping()
    except Exception:
        logger.error("Error connecting to the Docker daemon. Is the Docker daemon running?")
        pytest.skip(
            "Docker is not running, so a broker database cannot be setup. "
            "NOTE: If tests are invoked from within a Docker container, that test-container will need to have the "
            "host's docker socket mounted when run, with: -v /var/run/docker.sock:/var/run/docker.sock"
            "Skipping test of integration with the Broker DB"
        )

    # Check required Broker docker image is available
    found = docker_client.images.list(name=broker_docker_image)
    if not found:
        logger.error("Error finding required docker image in images registry: {}".format(broker_docker_image))
        pytest.skip(
            "Could not find the Docker Image named {} used to setup a broker database. "
            "Skipping test of integration with the Broker DB".format(broker_docker_image)
        )

    # ==== Run the DB setup script using the Broker docker image. ====

    broker_test_db_name = settings.DATABASES[settings.DATA_BROKER_DB_ALIAS]["NAME"]

    # Using a mounts to copy the broker source code into the container, and create a modifiable copy
    # of the broker config dir in order to execute broker DB setup code using that config
    # One of two strategies is used here, depending on whether Tests are running within a docker container or on a
    # developer's host machine (not a container)
    docker_run_mounts = []
    mounted_src = docker.types.Mount(
        type="bind",
        source=str(broker_src_dir_path_obj),
        target=broker_src_target,
        read_only=True,  # ensure provided broker source is not messed with during tests
    )
    docker_run_mounts.append(mounted_src)

    # The broker config dir that will be interrogated when executing DB init scripts in the broker container is
    # overlayed with a docker tmpfs, so the config modifications are not seen in the host source, but only from
    # within the running broker container
    # NOTE: tmpfs layered over the src bind mount makes the config dir empty, so another bind mount is used as the
    # source to copy the config back into the tmpfs mount

    # Temporary mount of surrogate config files
    mounted_broker_config_copy = docker.types.Mount(
        type="bind",
        source=str(broker_src_dir_path_obj / broker_config_dir),
        target=broker_config_copy_target,
        read_only=True,
    )

    # Throw-away/container-only tmpfs config-layer
    mounted_broker_config = docker.types.Mount(type="tmpfs", source=None, target=broker_config_mask_target)

    docker_run_mounts.append(mounted_broker_config_copy)
    docker_run_mounts.append(mounted_broker_config)

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
        sed -i.bak -E "s/host:.*$/host: {settings.DATABASES[settings.DATA_BROKER_DB_ALIAS]["HOST"]}/"             \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_config_file};         \
        sed -i.bak -E "s/port:.*$/port: {settings.DATABASES[settings.DATA_BROKER_DB_ALIAS]["PORT"]}/"             \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_config_file};         \
        sed -i.bak -E "s/username:.*$/username: {settings.DATABASES[settings.DATA_BROKER_DB_ALIAS]["USER"]}/"     \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_secrets_file};        \
        sed -i.bak -E "s/password:.*$/password: {settings.DATABASES[settings.DATA_BROKER_DB_ALIAS]["PASSWORD"]}/" \
            {broker_src_target}/{broker_config_dir}/{broker_integrationtest_secrets_file};        \
    """

    # Python script in broker-code that will run migrations and other db setup in the broker test DB
    broker_db_setup_cmd = rf"""                                                                   \
        python {broker_src_target}/{broker_config_dir}/scripts/setup/setup_all_db.py --dbname {broker_test_db_name};
    """

    broker_container_command = "sh -cex '" + broker_config_file_cmds + broker_db_config_cmds + broker_db_setup_cmd + "'"
    logger.info(f"Running following command in a container of image {broker_docker_image}: {broker_container_command}")
    logger.info(f"Container will access Broker scripts from mounted source dir: {broker_src_dir_path_obj}")
    logger.info("Configured Mounts: ")
    [logger.info(f"\tMOUNT: {str(m)}") for m in docker_run_mounts]

    container_name_suffix = (
        ""
        if not transform_xdist_worker_id_to_django_test_db_id(worker_id)
        else f"_{transform_xdist_worker_id_to_django_test_db_id(worker_id)}"
    )

    docker_run_args = {
        "name": "data-act-broker-init-test-db" + container_name_suffix,
        "command": broker_container_command,
        "remove": True,
        # network_mode = "host",
        # network=f"{os.path.basename(os.environ.get('PROJECT_SRC_PATH') or os.getcwd())}_default",
        "environment": {"env": broker_config_env_envvar},
        "mounts": docker_run_mounts,
        "stderr": True,
        "stream": True,
    }
    if os.environ.get("PROJECT_SRC_PATH") is None:
        # NOTE: use of network_mode="host" applies ONLY when on Linux (i.e. ignored elsewhere)
        # It allows docker to resolve network addresses (like "localhost") as if running from the docker host
        docker_run_args["network_mode"] = "host"
    else:
        # The PROJECT_SRC_PATH env is defined in the docker-compose.yaml when running tests inside a container.
        # In order for the test container to see the Broker containers created by this command we add it to the same
        # network; this assumes that the network is not set in the docker-compose.yaml and is using the default.
        docker_run_args["network"] = f"{os.path.basename(os.environ['PROJECT_SRC_PATH'])}_default"

    log_gen = docker_client.containers.run(broker_docker_image, **docker_run_args)
    [logger.info(str(log)) for log in log_gen]  # log container logs from returned streaming log generator
    logger.info("Command ran to completion in container. Broker DB should be setup for tests.")


@pytest.fixture(scope="session")
def broker_server_dblink_setup(django_db_blocker, broker_db_setup):
    """Fixture to use during a pytest session if you will run integration
    tests connecting to the broker DB via dblink.
    """
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


@pytest.fixture()
def fake_sqs_queue():
    q = _FakeUnitTestFileBackedSQSQueue.instance()

    # Check that it's the unit test queue before purging
    assert q.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME
    q.purge()
    q.reset_instance_state()
    yield q
    q.purge()
    q.reset_instance_state()
    remove_unittest_queue_data_files(q)
