import json

from datetime import datetime, timezone
from typing import Optional, List
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, DEFAULT_DB_ALIAS
from elasticsearch import Elasticsearch
from pathlib import Path
from string import Template

from usaspending_api.common.sqs.sqs_handler import (
    UNITTEST_FAKE_QUEUE_NAME,
    _FakeUnitTestFileBackedSQSQueue,
    _FakeStatelessLoggingSQSDeadLetterQueue,
    UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME,
)
from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher
from usaspending_api.common.helpers.text_helpers import generate_random_string
from usaspending_api.etl.elasticsearch_loader_helpers import create_aliases
from usaspending_api.etl.management.commands.es_configure import retrieve_index_template


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
        self.template = retrieve_index_template("{}_template".format(self.index_type[:-1]))
        self.mappings = json.loads(self.template)["mappings"]
        self.etl_config = {
            "index_name": self.index_name,
            "query_alias_prefix": self.alias_prefix,
            "verbose": False,
            "write_alias": self.index_name + "-alias",
        }

    def delete_index(self):
        self.client.indices.delete(self.index_name, ignore_unavailable=True)

    def update_index(self, **options):
        """
        To ensure a fresh Elasticsearch index, delete the old one, update the
        materialized views, re-create the Elasticsearch index, create aliases
        for the index, and add contents.
        """
        self.delete_index()
        self.client.indices.create(index=self.index_name, body=self.template)
        create_aliases(self.client, self.etl_config)
        self._add_contents(**options)

    def _add_contents(self, **options):
        """
        Get all of the transactions presented in the view and stuff them into the Elasticsearch index.
        The view is only needed to load the transactions into Elasticsearch so it is dropped after each use.
        """
        view_sql_file = "award_delta_view.sql" if self.index_type == "awards" else "transaction_delta_view.sql"
        view_sql = open(str(settings.APP_DIR / "database_scripts" / "etl" / view_sql_file), "r").read()
        with connection.cursor() as cursor:
            cursor.execute(view_sql)
            if self.index_type == "transactions":
                view_name = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
            else:
                view_name = settings.ES_AWARDS_ETL_VIEW_NAME
            cursor.execute(f"SELECT * FROM {view_name};")
            transactions = ordered_dictionary_fetcher(cursor)
            cursor.execute(f"DROP VIEW {view_name};")

        for transaction in transactions:
            # Special cases where we convert array of JSON to an array of strings to avoid nested types
            routing_key = options.get("routing", settings.ES_ROUTING_FIELD)
            routing_value = transaction.get(routing_key)
            if self.index_type == "transactions":
                transaction["federal_accounts"] = self.convert_json_arrays_to_list(transaction["federal_accounts"])
            self.client.index(
                index=self.index_name,
                body=json.dumps(transaction, cls=DjangoJSONEncoder),
                id=transaction["{}_id".format(self.index_type[:-1])],
                routing=routing_value,
            )
        # Force newly added documents to become searchable.
        self.client.indices.refresh(self.index_name)

    @classmethod
    def _generate_index_name(cls):
        return "test-{}-{}".format(
            datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f"), generate_random_string()
        )

    @staticmethod
    def convert_json_arrays_to_list(json_array: Optional[List[dict]]) -> Optional[List[str]]:
        if json_array is None:
            return None
        result = []
        for j in json_array:
            for key, value in j.items():
                j[key] = "" if value is None else str(j[key])
            result.append(json.dumps(j, sort_keys=True))
        return result


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

    # Check that it's the unit test queue before removings
    assert q.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME
    queue_data_file = q._QUEUE_DATA_FILE
    lock_file_path = Path(queue_data_file + ".lock")
    if lock_file_path.exists():
        lock_file_path.unlink()
    queue_data_file_path = Path(queue_data_file)
    if queue_data_file_path.exists():
        queue_data_file_path.unlink()
