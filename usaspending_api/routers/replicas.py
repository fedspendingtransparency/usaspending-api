import random

from usaspending_api.references.models import FilterHash
from usaspending_api.download.models import DownloadJob

"""
The USAspending API is a *mostly* readonly application. This
router is used to balance loads between multiple databases
defined by the environment variables in settings.py, and
handle the models that are *not* readonly appropriately.

It splits requests among two databases but you can add more.
"""


class ReadReplicaRouter(object):
    def db_for_read(self, model, **hints):
        # these are the only models we write to; to deal with replication lag just get them from the source db
        if model in [FilterHash, DownloadJob]:
            return "db_source"
        return random.choice(["db_source", "db_r1"])

    def db_for_write(self, model, **hints):
        # write to source db only (bc read replicas)
        return "db_source"

    def allow_relation(self, obj1, obj2, **hints):
        db_list = ("db_source", "db_r1")
        if obj1._state.db in db_list and obj2._state.db in db_list:
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return True
