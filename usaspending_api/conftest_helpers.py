import json

from datetime import datetime, timezone
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, DEFAULT_DB_ALIAS
from elasticsearch import Elasticsearch
from pathlib import Path
from string import Template

from usaspending_api.bulk_download.sqs_handler import FAKE_QUEUE
from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher
from usaspending_api.common.helpers.text_helpers import generate_random_string
from usaspending_api.etl.es_etl_helpers import create_aliases
from usaspending_api.etl.management.commands.es_configure import retrieve_transaction_index_template


class TestElasticSearchIndex:
    def __init__(self):
        """
        We will be prefixing all aliases with the index name to ensure
        uniquity, otherwise, we may end up with aliases representing more
        than one index which may throw off our search results.
        """
        self.index_name = self._generate_index_name()
        self.alias_prefix = self.index_name
        self.client = Elasticsearch([settings.ES_HOSTNAME], timeout=settings.ES_TIMEOUT)
        self.template = retrieve_transaction_index_template()
        self.mappings = json.loads(self.template)["mappings"]
        self.doc_type = str(list(self.mappings.keys())[0])

    def delete_index(self):
        self.client.indices.delete(self.index_name, ignore_unavailable=True)

    def update_index(self):
        """
        To ensure a fresh Elasticsearch index, delete the old one, update the
        materialized views, re-create the Elasticsearch index, create aliases
        for the index, and add contents.
        """
        self.delete_index()
        self._refresh_materialized_views()
        self.client.indices.create(self.index_name, self.template)
        create_aliases(self.client, self.index_name, True)
        self._add_contents()

    def _add_contents(self):
        """
        Get all of the transactions presented in the view and
        stuff them into the Elasticsearch index.
        """
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM {}".format(settings.ES_TRANSACTIONS_ETL_VIEW_NAME))
            transactions = ordered_dictionary_fetcher(cursor)

        for transaction in transactions:
            self.client.index(
                self.index_name,
                self.doc_type,
                json.dumps(transaction, cls=DjangoJSONEncoder),
                transaction["transaction_id"],
            )

        # Force newly added documents to become searchable.
        self.client.indices.refresh(self.index_name)

    @staticmethod
    def _refresh_materialized_views():
        """
        This materialized view is used by the es etl view, so
        we will need to refresh it in order for the view to see
        changes to the underlying tables.
        """
        with connection.cursor() as cursor:
            cursor.execute("REFRESH MATERIALIZED VIEW universal_transaction_matview;")

    @classmethod
    def _generate_index_name(cls):
        return "test-{}-{}".format(
            datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f"), generate_random_string()
        )


def ensure_broker_server_dblink_exists():
    """Ensure that all the database extensions exist, and the the broker database is setup as a foreign data server

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


def remove_local_queue_data_files():
    """Delete any local files created to persist or access file-backed queue data from ``FakeFileBackedSQSQueue``"""
    queue_data_file = FAKE_QUEUE._QUEUE_DATA_FILE
    lock_file_path = Path(queue_data_file + ".lock")
    if lock_file_path.exists():
        lock_file_path.unlink()
    queue_data_file_path = Path(queue_data_file)
    if queue_data_file_path.exists():
        queue_data_file_path.unlink()
