import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator

from databricks.sdk import WorkspaceClient
from django.core.management import call_command

from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.common.spark.configs import LOCAL_EXTENDED_EXTRA_CONF

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _AbstractStrategy(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def handle_start(
        self, job_name: str, command_name: str, command_options: list[str], **kwargs
    ) -> dict[str, str] | None:
        pass


class DatabricksStrategy(_AbstractStrategy):

    _client: WorkspaceClient = None

    @property
    def name(self) -> str:
        return "DATABRICKS"

    @property
    def client(self) -> WorkspaceClient:
        if not self._client:
            self._client = WorkspaceClient()
        return self._client

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict[str, str]:
        job_id = self.get_job_id(job_name)
        try:
            job_run = self.client.jobs.run_now(job_id, python_params=[command_name, *command_options])
        except Exception:
            logger.exception(f'Failed to run job "{job_name}" with ID "{job_id}"')
            raise
        return {"job_id": job_id, "run_id": job_run.bind()["run_id"]}

    def get_job_id(self, job_name: str) -> int:
        run_list = list(self.client.jobs.list(name=job_name))
        if len(run_list) == 0:
            raise ValueError(f"No job found with name: {job_name}")
        if len(run_list) > 1:
            raise ValueError(f"More than one job found that match name: {job_name}")
        return run_list[0].job_id


class EmrServerlessStrategy(_AbstractStrategy):

    @property
    def name(self) -> str:
        return "EMR_SERVERLESS"

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict[str, str]:
        # TODO: This will be implemented as we migrate, but added as a placeholder for now
        pass


class LocalStrategy(_AbstractStrategy):

    @property
    def name(self) -> str:
        return "LOCAL"

    @staticmethod
    @contextmanager
    def _get_spark_session() -> Generator["SparkSession", None, None]:
        extra_conf = {
            **LOCAL_EXTENDED_EXTRA_CONF,
            # Overwrite to allow more memory given this will process more data than test cases
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        }
        spark = get_active_spark_session()
        spark_created_for_job = False
        if not spark:
            spark_created_for_job = True
            spark = configure_spark_session(**extra_conf, spark_context=spark, enable_hive_support=True)

        yield spark

        if spark_created_for_job:
            spark.stop()

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> None:
        try:
            with self._get_spark_session():
                call_command(command_name, *command_options)
        except Exception:
            logger.exception(f"Failed on command: {command_name} {' '.join(command_options)}")
            raise


class SparkJobs:
    def __init__(self, strategy: _AbstractStrategy):
        self._strategy = strategy

    @property
    def strategy(self) -> _AbstractStrategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: _AbstractStrategy) -> None:
        self._strategy = strategy

    def start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict[str, str] | None:
        logger.info(f'Starting {job_name} on {self.strategy.name}: "{command_name} {" ".join(command_options)}"')
        run_details = self.strategy.handle_start(job_name, command_name, command_options, **kwargs)
        if run_details is None:
            success_msg = "Job completed successfully"
        else:
            success_msg = f"Job run successfully started; {run_details}"
        logger.info(success_msg)
        return run_details
