from django.db import DEFAULT_DB_ALIAS
from django.db.models import Model

from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.llm.models.db_models import AIModel, Message, Prompts, Session, ToolUse
from usaspending_api.references.models import FilterHash


class ReadReplicaRouter:
    """
    The USAspending API is *mostly* a readonly application.  This router is used to balance loads
    between multiple databases defined by the environment variables in settings.py, and handle
    the models that are *not* readonly appropriately.  Also prevents model access/migrations to Broker.
    """

    writable_database = DEFAULT_DB_ALIAS
    read_replicas = ["db_r1"]

    def __init__(self):
        self.usaspending_databases = [self.writable_database] + self.read_replicas

    def db_for_read(self, model: Model, **hints) -> str:
        """
        FilterHash, DownloadJob, and the LLM app models are writable tables so always read from source (default) to
        mitigate replication lag.  Otherwise, choose a connection randomly.
        """
        if model in [FilterHash, DownloadJob, AIModel, Message, Prompts, Session, ToolUse]:
            return self.writable_database

        if len(self.read_replicas) > 0:
            return self.read_replicas[0]
        else:
            return self.writable_database

    def db_for_write(self, model: Model, **hints) -> str:
        return self.writable_database

    def allow_relation(self, obj1: Model, obj2: Model, **hints) -> bool:
        """Relations are currently only allowed in USAspending.  Cross database relations are not allowed."""
        return obj1._state.db in self.usaspending_databases and obj2._state.db == obj1._state.db

    def allow_migrate(self, db: str, app_label: str, model_name: str | None = None, **hints) -> bool:
        """Migrations should only run in USAspending against the writable database."""
        return db == self.writable_database


class DefaultOnlyRouter(ReadReplicaRouter):
    """For when only the default connection is used.  Prevents model access/migrations to Broker."""

    read_replicas = []
