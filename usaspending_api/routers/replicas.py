import random

from django.db import DEFAULT_DB_ALIAS
from usaspending_api.references.models import FilterHash
from usaspending_api.download.models import DownloadJob

"""
The USAspending API is a *mostly* readonly application. This
router is used to balance loads between multiple databases
defined by the environment variables in settings.py, and
handle the models that are *not* readonly appropriately.

It splits requests among two databases but you can add more.
"""


class ReadReplicaRouter:

    usaspending_db_list = (DEFAULT_DB_ALIAS, "db_r1")
    broker_db_list = ("data_broker",)

    def db_for_read(self, model, **hints):
        """ For reads, choose a connection randomly.  FilterHash and DownloadJob are writable tables so always
            read from source (default) to mitigation replication lag issues. """
        if model in [FilterHash, DownloadJob]:
            return DEFAULT_DB_ALIAS
        return random.choice(self.usaspending_db_list)

    def db_for_write(self, model, **hints):
        """ Write to source (default) db only because replicas are read only. """
        return DEFAULT_DB_ALIAS

    def allow_relation(self, obj1, obj2, **hints):
        """ Relations are allowed between similar databases so USAspending to USAspending or Broker to Broker. """
        return (obj1._state.db in self.usaspending_db_list and obj2._state.db in self.usaspending_db_list) or (
            obj1._state.db in self.broker_db_list and obj2._state.db in self.broker_db_list
        )

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """ We do not allow migrations against Broker. """
        return db not in self.broker_db_list


class DefaultOnlyRouter(ReadReplicaRouter):
    usaspending_db_list = (DEFAULT_DB_ALIAS,)
