import random

from django.db import DEFAULT_DB_ALIAS

from usaspending_api.references.models import FilterHash
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.references.models import Agency
from usaspending_api.references.models import ToptierAgency
from usaspending_api.references.models import SubtierAgency


class BaseRouter:
    """
    The USAspending API is *mostly* a readonly application.  This router is used to balance loads
    between multiple databases defined by the environment variables in settings.py, and handle
    the models that are *not* readonly appropriately.  Also prevents model access/migrations to Broker.
    """

    primary_database = DEFAULT_DB_ALIAS
    read_replicas = ["db_r1"]
    secondary_database = "db_secondary"
    migrateable_databases = [primary_database, secondary_database]

    primary_models = [FilterHash, DownloadJob]
    secondary_models = [Agency, ToptierAgency, SubtierAgency]

    def __init__(self):
        self.usaspending_databases = [self.primary_database] + self.read_replicas

    def db_for_read(self, model, **hints):
        """

        """
        if model in self.secondary_models:
            return self.secondary_database

        """
        FilterHash and DownloadJob are writable tables so always read from source (default) to
        mitigate replication lag.  Otherwise, choose a connection randomly.
        """
        if model in self.primary_models:
            return self.primary_database
        return random.choice(self.usaspending_databases)

    def db_for_write(self, model, **hints):
        if model in self.primary_models:
            return self.primary_database
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """ Relations are currently only allowed in USAspending.  Cross database relations are not allowed. """
        return obj1._state.db in self.usaspending_databases and obj2._state.db == obj1._state.db

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """ Migrations should only run in USAspending against the writable database. """
        return db in self.migrateable_databases


class DefaultOnlyRouter(BaseRouter):
    """ For when only the default connection is used.  Prevents model access/migrations to Broker. """

    read_replicas = []
