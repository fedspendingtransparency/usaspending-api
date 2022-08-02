from builtins import Exception
from datetime import datetime, timezone
from django.conf import settings
from django.core.management import call_command
from django.core.serializers.json import json, DjangoJSONEncoder
from django.db import connection, DEFAULT_DB_ALIAS
from elasticsearch import Elasticsearch
from pathlib import Path
from string import Template

from usaspending_api.etl.elasticsearch_loader_helpers.index_config import create_load_alias
from usaspending_api.common.sqs.sqs_handler import (
    UNITTEST_FAKE_QUEUE_NAME,
    _FakeUnitTestFileBackedSQSQueue,
    _FakeStatelessLoggingSQSDeadLetterQueue,
    UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME,
)
from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher
from usaspending_api.common.helpers.text_helpers import generate_random_string
from usaspending_api.etl.elasticsearch_loader_helpers import (
    create_award_type_aliases,
    execute_sql_statement,
    TaskSpec,
    transform_award_data,
    transform_covid19_faba_data,
    transform_transaction_data,
)


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
        self.worker = TaskSpec(
            base_table=None,
            base_table_id=None,
            execute_sql_func=execute_sql_statement,
            field_for_es_id="award_id" if self.index_type == "award" else "transaction_id",
            index=self.index_name,
            is_incremental=None,
            name=f"{self.index_type} test worker",
            partition_number=None,
            primary_key="award_id" if self.index_type == "award" else "transaction_id",
            sql=None,
            transform_func=None,
            view=None,
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
        elif self.index_type == "covid19-faba":
            upper_name = "COVID19_FABA"
        elif self.index_type == "transaction":
            upper_name = "TRANSACTIONS"
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
        elif self.index_type == "covid19-faba":
            view_sql_file = f"{settings.ES_COVID19_FABA_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_COVID19_FABA_ETL_VIEW_NAME
            es_id = "financial_account_distinct_award_key"
        elif self.index_type == "transaction":
            view_sql_file = f"{settings.ES_TRANSACTIONS_ETL_VIEW_NAME}.sql"
            view_name = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
            es_id = f"{self.index_type}_id"
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
            elif self.index_type == "covid19-faba":
                records = transform_covid19_faba_data(
                    TaskSpec(
                        name="worker",
                        index=self.index_name,
                        sql=view_sql_file,
                        view=view_name,
                        base_table="financial_accounts_by_awards",
                        base_table_id="financial_accounts_by_awards_id",
                        field_for_es_id="financial_account_distinct_award_key",
                        primary_key="award_id",
                        partition_number=1,
                        is_incremental=False,
                    ),
                    records,
                )

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
        elif self.index_type == "covid19-faba":
            required_suffix = "-" + settings.ES_COVID19_FABA_NAME_SUFFIX
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
    if DEFAULT_DB_ALIAS not in settings.DATABASES:
        raise Exception("'{}' database not configured in django settings.DATABASES".format(DEFAULT_DB_ALIAS))
    if "data_broker" not in settings.DATABASES:
        raise Exception("'data_broker' database not configured in django settings.DATABASES")
    db_conn_tokens_dict = {
        **{"USASPENDING_DB_" + k: v for k, v in settings.DATABASES[DEFAULT_DB_ALIAS].items()},
        **{"BROKER_DB_" + k: v for k, v in settings.DATABASES["data_broker"].items()},
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

        if settings.DATABASES["data_broker"]["HOST"] in ("localhost", "120.0.0.1", "0.0.0.0"):
            db_conn_tokens_dict['BROKER_DB_HOST'] = "host.docker.internal"

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
