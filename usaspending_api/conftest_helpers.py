import json
import os
import string

from datetime import datetime, timezone
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from elasticsearch import Elasticsearch
from random import choice
from usaspending_api.common.helpers.sql_helpers import fetchall_to_ordered_dictionary
from usaspending_api.etl.es_etl_helpers import create_aliases
from usaspending_api.etl.management.commands.es_rapidloader import mapping_data_for_processing


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
        self.mapping, self.doc_type, _ = mapping_data_for_processing()

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
        self.client.indices.create(self.index_name, self.mapping)
        create_aliases(self.client, self.index_name, True)
        self._add_contents()

    def _add_contents(self):
        """
        Get all of the transactions presented by transaction_delta_view and
        stuff them into the Elasticsearch index.
        """
        with connection.cursor() as cursor:
            cursor.execute("select * from transaction_delta_view")
            transactions = fetchall_to_ordered_dictionary(cursor)

        for transaction in transactions:
            self.client.index(
                self.index_name,
                self.doc_type,
                json.dumps(transaction, cls=DjangoJSONEncoder),
                transaction["transaction_id"])

        # Force newly added documents to become searchable.
        self.client.indices.refresh(self.index_name)

    @staticmethod
    def _refresh_materialized_views():
        """
        These two materialized views are used by transaction_delta_view.sql, so
        we will need to refresh them in order for transaction_delta_view to see
        changes to their underlying tables.
        """
        with connection.cursor() as cursor:
            cursor.execute(
                "refresh materialized view universal_award_matview; "
                "refresh materialized view universal_transaction_matview;"
            )

    @staticmethod
    def _generate_random_string(size=6, chars=string.ascii_lowercase + string.digits):
        return "".join(choice(chars) for _ in range(size))

    @classmethod
    def _generate_index_name(cls):
        return "test-{}-{}".format(
            datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f"),
            cls._generate_random_string()
        )


def ensure_transaction_delta_view_exists():
    """
    The transaction_delta_view is used to populate the Elasticsearch index.
    This function will just ensure the view exists in the database.
    """
    transaction_delta_view_path = os.path.join(
        settings.BASE_DIR,
        "usaspending_api/database_scripts/etl/transaction_delta_view.sql"
    )
    with open(transaction_delta_view_path) as f:
        transaction_delta_view = f.read()
    with connection.cursor() as cursor:
        cursor.execute(transaction_delta_view)
