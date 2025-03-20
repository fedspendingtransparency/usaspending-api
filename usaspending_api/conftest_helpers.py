import os
from builtins import Exception
from datetime import datetime, timezone
from pathlib import Path
from string import Template

from django.conf import settings
from django.core.management import call_command
from django.core.serializers.json import DjangoJSONEncoder, json
from django.db import connection
from elasticsearch import Elasticsearch
from pytest import Session

from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher
from usaspending_api.common.helpers.text_helpers import generate_random_string
from usaspending_api.common.sqs.sqs_handler import (
    UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME,
    UNITTEST_FAKE_QUEUE_NAME,
    _FakeStatelessLoggingSQSDeadLetterQueue,
    _FakeUnitTestFileBackedSQSQueue,
)
from usaspending_api.etl.elasticsearch_loader_helpers import (
    TaskSpec,
    create_award_type_aliases,
    execute_sql_statement,
    transform_award_data,
    transform_location_data,
    transform_subaward_data,
    transform_transaction_data,
)
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import create_load_alias


def is_pytest_xdist_parallel_sessions() -> bool:
    """Return True if the current tests executing are running in a pytest-xdist parallel test session,
    even if only 1 worker (1 master, and 1 worker) are configured"""
    worker_count = int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", 0))
    return worker_count > 0


def is_pytest_xdist_master_process(session: Session):
    """Return True if running in pytest-xdist parallel test sessions and the session given is from the 'master'
    process, and not a session of one of the spawned workers.

    This is useful if needing to orchestration cross-session (cross-worker) setup and teardown of fixtures that
    should only run once, which can be done from the master process.
    See: https://pytest-xdist.readthedocs.io/en/stable/how-to.html#making-session-scoped-fixtures-execute-only-once
    And: https://github.com/pytest-dev/pytest-xdist/issues/271#issuecomment-826396320
    """
    workerinput = getattr(session.config, "workerinput", None)
    return is_pytest_xdist_parallel_sessions() and workerinput is None


def transform_xdist_worker_id_to_django_test_db_id(worker_id: str):
    """Align pytest-xdist global worker IDs instead to suffixes Django unittest uses for their test DBs
    e.g. 'gw0' -> '1', 'gw1' -> '2', ...
    If it's the master process, return empty string"""
    if not worker_id or worker_id == "master":
        return ""
    return str(int(worker_id.replace("gw", "")) + 1)


def is_safe_for_xdist_setup_or_teardown(session: Session, worker_id: str):
    """Use this in any session fixture whose setup must run EXACTLY ONCE for ALL workers
    if there are multiple parallel workers (e.g. via a pytest-xdist run with -n X or --numprocesses=X),
    and then only perform that exactly-once setup logic when this evalutes to True.
    For teardown, to ensure this cleanup runs after all workers complete their session, add cleanup logic to the pytest
    ``pytest_sessionfinish(session, exitstatus)`` fixture defined in this module, and guard it with this

    Example:
        Setup
        >>> @pytest.fixture(scope="session")
        >>> def do_some_setup_for_all_tests(session, worker_id):
        >>>     if not is_safe_for_xdist_setup_or_teardown(session, worker_id):
        >>>         yield
        >>>     else:
        >>>         # ... do setup here exactly once

        TearDown:
        >>> def pytest_sessionfinish(session, exitstatus):
        >>>     worker_id = get_xdist_worker_id(session)
        >>>     if is_safe_for_xdist_setup_or_teardown(session, worker_id):
        >>>         # ... do teardown here
    """
    return is_pytest_xdist_master_process(session) or worker_id == "master" or not worker_id


class TestElasticSearchIndex:
    def __init__(self, index_type):
        """
        We will be prefixing all aliases with the index name to ensure
        uniquity, otherwise, we may end up with aliases representing more
        than one index which may throw off our search results.
        """
        self.index_type = index_type
        self.index_name = self._generate_index_name()
        self.alias_prefix = self.index_name
        self.client = Elasticsearch([settings.ES_HOSTNAME], timeout=settings.ES_TIMEOUT)
        self.etl_config = {
            "load_type": self.index_type,
            "index_name": self.index_name,
            "query_alias_prefix": self.alias_prefix,
            "verbose": False,
            "verbosity": 0,
            "write_alias": self.index_name + "-load-alias",
            "process_deletes": True,
        }

        match self.index_type:
            case "award":
                es_id_field = "award_id"
            case "subaward":
                es_id_field = "broker_subaward_id"
            case "transaction":
                es_id_field = "transaction_id"
            case "recipient" | "location":
                es_id_field = "id"
            case _:
                raise Exception(f"No value for the `_id` field in Elasticsearch has been set for {self.index_type}")

        self.worker = TaskSpec(
            base_table=None,
            base_table_id=None,
            execute_sql_func=execute_sql_statement,
            field_for_es_id=es_id_field,
            index=self.index_name,
            is_incremental=None,
            name=f"{self.index_type} test worker",
            partition_number=None,
            primary_key=es_id_field,
            sql=None,
            transform_func=None,
            view=None,
            slices="auto",
        )

    def delete_index(self):
        self.client.indices.delete(self.index_name, ignore_unavailable=True)

    def update_index(self, load_index: bool = True, **options):
        """
        To ensure a fresh Elasticsearch index, delete the old one, update the
        materialized views, re-create the Elasticsearch index, create aliases
        for the index, and add contents.
        """
        self.delete_index()
        call_command("es_configure", "--template-only", f"--load-type={self.index_type}")
        self.client.indices.create(index=self.index_name)
        create_award_type_aliases(self.client, self.etl_config)
        create_load_alias(self.client, self.etl_config)
        self.etl_config["max_query_size"] = self._get_max_query_size()
        if load_index:
            self._add_contents(**options)

    def _get_max_query_size(self):
        upper_name = ""
        if self.index_type == "award":
            upper_name = "AWARDS"
        elif self.index_type == "subaward":
            upper_name = "SUBAWARD"
        elif self.index_type == "transaction":
            upper_name = "TRANSACTIONS"
        elif self.index_type == "recipient":
            upper_name = "RECIPIENTS"
        elif self.index_type == "location":
            upper_name = "LOCATIONS"
        return getattr(settings, f"ES_{upper_name}_MAX_RESULT_WINDOW")

    def _add_contents(self, **options):
        """
        Get all of the transactions presented in the view and stuff them into the Elasticsearch index.
        The view is only needed to load the transactions into Elasticsearch so it is dropped after each use.
        """
        if self.index_type == "award":
            view_sql_file = f"{settings.ES_AWARDS_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_AWARDS_ETL_VIEW_NAME
            es_id = f"{self.index_type}_id"
        elif self.index_type == "subaward":
            view_sql_file = f"{settings.ES_SUBAWARD_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_SUBAWARD_ETL_VIEW_NAME
            es_id = "broker_subaward_id"
        elif self.index_type == "transaction":
            view_sql_file = f"{settings.ES_TRANSACTIONS_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
            es_id = f"{self.index_type}_id"
        elif self.index_type == "recipient":
            view_sql_file = f"{settings.ES_RECIPIENTS_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_RECIPIENTS_ETL_VIEW_NAME
            es_id = "id"
        elif self.index_type == "location":
            view_sql_file = f"{settings.ES_LOCATIONS_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_LOCATIONS_ETL_VIEW_NAME
            es_id = "id"
        else:
            raise Exception("Invalid index type")

        view_sql = open(str(settings.APP_DIR / "database_scripts" / "etl" / view_sql_file), "r").read()
        with connection.cursor() as cursor:
            cursor.execute(view_sql)
            cursor.execute(f"SELECT * FROM {view_name};")
            records = ordered_dictionary_fetcher(cursor)
            cursor.execute(f"DROP VIEW {view_name};")
            if self.index_type == "award":
                records = transform_award_data(self.worker, records)
            elif self.index_type == "transaction":
                records = transform_transaction_data(self.worker, records)
            elif self.index_type == "subaward":
                records = transform_subaward_data(self.worker, records)
            elif self.index_type == "location":
                records = transform_location_data(self.worker, records)

        for record in records:
            # Special cases where we convert array of JSON to an array of strings to avoid nested types
            routing_key = options.get("routing", settings.ES_ROUTING_FIELD)
            routing_value = record.get(routing_key)

            if "_id" in record:
                es_id_value = record.pop("_id")
            else:
                es_id_value = record.get(es_id)

            self.client.index(
                index=self.index_name,
                body=json.dumps(record, cls=DjangoJSONEncoder),
                id=es_id_value,
                routing=routing_value,
            )
        # Force newly added documents to become searchable.
        self.client.indices.refresh(self.index_name)

    def _generate_index_name(self):
        required_suffix = ""
        if self.index_type == "award":
            required_suffix = "-" + settings.ES_AWARDS_NAME_SUFFIX
        elif self.index_type == "transaction":
            required_suffix = "-" + settings.ES_TRANSACTIONS_NAME_SUFFIX
        elif self.index_type == "subaward":
            required_suffix = "-" + settings.ES_SUBAWARD_NAME_SUFFIX
        elif self.index_type == "recipient":
            required_suffix = "-" + settings.ES_RECIPIENTS_NAME_SUFFIX
        elif self.index_type == "location":
            required_suffix = "-" + settings.ES_LOCATIONS_NAME_SUFFIX
        return (
            f"test-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M-%S-%f')}"
            f"-{generate_random_string()}"
            f"{required_suffix}"
        )


def ensure_broker_server_dblink_exists():
    """Ensure that all the database extensions exist, and the broker database is setup as a foreign data server

    This leverages SQL script files and connection strings in ``settings.DATABASES`` in order to setup these database
    objects. The connection strings for BOTH the USAspending and the Broker databases are needed,
    as the postgres ``SERVER`` setup needs to reference tokens in both connection strings.

    NOTE: An alternative way to run this through bash scripting is to first ``EXPORT`` the referenced environment
    variables, then run::

        psql --username ${USASPENDING_DB_USER} --dbname ${USASPENDING_DB_NAME} --echo-all --set ON_ERROR_STOP=on --set VERBOSITY=verbose --file usaspending_api/database_scripts/extensions/extensions.sql
        eval "cat <<< \"$(<usaspending_api/database_scripts/servers/broker_server.sql)\"" > usaspending_api/database_scripts/servers/broker_server.sql
        psql --username ${USASPENDING_DB_USER} --dbname ${USASPENDING_DB_NAME} --echo-all --set ON_ERROR_STOP=on --set VERBOSITY=verbose --file usaspending_api/database_scripts/servers/broker_server.sql

    """
    # Gather tokens from database connection strings
    if settings.DEFAULT_DB_ALIAS not in settings.DATABASES:
        raise Exception(f"'{settings.DEFAULT_DATABASE_ALIAS}' database not configured in django settings.DATABASES")
    if settings.DATA_BROKER_DB_ALIAS not in settings.DATABASES:
        raise Exception(f"'{settings.DATA_BROKER_DB_ALIAS}' database not configured in django settings.DATABASES")
    db_conn_tokens_dict = {
        **{"USASPENDING_DB_" + k: v for k, v in settings.DATABASES[settings.DEFAULT_DB_ALIAS].items()},
        **{"BROKER_DB_" + k: v for k, v in settings.DATABASES[settings.DATA_BROKER_DB_ALIAS].items()},
    }

    extensions_script_path = str(settings.APP_DIR / "database_scripts" / "extensions" / "extensions.sql")
    broker_server_script_path = str(settings.APP_DIR / "database_scripts" / "servers" / "broker_server.sql")

    with open(extensions_script_path) as f1, open(broker_server_script_path) as f2:
        extensions_script = f1.read()
        broker_server_script = f2.read()

    with connection.cursor() as cursor:
        # Ensure required extensions added to USAspending db
        cursor.execute(extensions_script)

        # Ensure foreign server setup to point to the broker DB for dblink to work
        broker_server_script_template = Template(broker_server_script)

        cursor.execute(broker_server_script_template.substitute(**db_conn_tokens_dict))


def get_unittest_fake_sqs_queue(*args, **kwargs):
    """Mocks sqs_handler.get_sqs_queue to instead return a fake queue used for unit testing"""
    if "queue_name" in kwargs and kwargs["queue_name"] == UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME:
        return _FakeStatelessLoggingSQSDeadLetterQueue()
    else:
        return _FakeUnitTestFileBackedSQSQueue.instance()


def remove_unittest_queue_data_files(queue_to_tear_down):
    """Delete any local files created to persist or access file-backed queue data from
    ``_FakeUnittestFileBackedSQSQueue``
    """
    q = queue_to_tear_down

    # Check that it's the unit test queue before removing
    assert q.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME
    queue_data_file = q._QUEUE_DATA_FILE
    lock_file_path = Path(queue_data_file + ".lock")
    if lock_file_path.exists():
        lock_file_path.unlink()
    queue_data_file_path = Path(queue_data_file)
    if queue_data_file_path.exists():
        queue_data_file_path.unlink()
