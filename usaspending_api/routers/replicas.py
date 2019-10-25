import random

from django.db import DEFAULT_DB_ALIAS
from usaspending_api.references.models import FilterHash
from usaspending_api.download.models import DownloadJob


class ReadReplicaRouter:
    """
    The USAspending API is *mostly* a readonly application.  This router is used to balance loads
    between multiple databases defined by the environment variables in settings.py, and handle
    the models that are *not* readonly appropriately.
    """

    usaspending_db_list = (DEFAULT_DB_ALIAS, "db_r1")

    def db_for_read(self, model, **hints):
        """
        FilterHash and DownloadJob are writable tables so always read from source (default) to
        mitigate replication lag.  Otherwise, choose a connection randomly.
        """
        if model in [FilterHash, DownloadJob]:
            return DEFAULT_DB_ALIAS
        return random.choice(self.usaspending_db_list)

    def db_for_write(self, model, **hints):
        """ Write to source (default) db only because replicas are read only. """
        return DEFAULT_DB_ALIAS

    def allow_relation(self, obj1, obj2, **hints):
        """ Relations are currently only allowed in USAspending. """
        return obj1._state.db in self.usaspending_db_list and obj2._state.db in self.usaspending_db_list

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """ Migrations should only run in USAspending, never in Broker. """
        return db in self.usaspending_db_list


class DefaultOnlyRouter(ReadReplicaRouter):
    """ For when only the default connection is used.  Prevents migrations to Broker. """

    usaspending_db_list = (DEFAULT_DB_ALIAS,)
