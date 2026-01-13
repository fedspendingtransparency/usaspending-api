import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Generator

import boto3
from botocore.client import BaseClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config as DatabricksConfig
from databricks.sdk.service.jobs import BaseJob, RunLifeCycleState
from django.conf import settings
from django.core.management import call_command
from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from usaspending_api.config import CONFIG

from usaspending_api.common.spark.configs import LOCAL_EXTENDED_EXTRA_CONF, OPTIONAL_SPARK_HIVE_JAR, SPARK_SESSION_JARS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _AbstractStrategy(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict | None:
        pass


class DatabricksStrategy(_AbstractStrategy):
    _client: WorkspaceClient = None

    @property
    def name(self) -> str:
        return "DATABRICKS"

    @property
    def client(self) -> WorkspaceClient:
        if not self._client:
            self._client = WorkspaceClient(config=DatabricksConfig(retry_timeout_seconds=120))
        return self._client

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict:
        job = None
        try:
            job = self._get_job(job_name)
            job_run_as_wait = self.client.jobs.run_now(job.job_id, python_params=[command_name, *command_options])
            job_run_id = job_run_as_wait.bind()["run_id"]
            self._wait_for_run_to_start(job, job_run_id)
        except TimeoutError:
            logger.exception(f"Connection timed out while starting a run of {command_name}")
            raise
        except Exception:
            msg = f"Failed to run job '{job_name}'"
            if job is not None:
                msg += f" with ID '{job.job_id}'"
            logger.exception(msg)
            raise

        return {"job_id": job.job_id, "run_id": job_run_id}

    def _get_job(self, job_name: str) -> BaseJob:
        job_list = list(self.client.jobs.list(name=job_name))
        if len(job_list) == 0:
            raise ValueError(f"No job found with name: {job_name}")
        if len(job_list) > 1:
            raise ValueError(f"More than one job found that match name: {job_name}")
        return job_list[0]

    def _wait_for_run_to_start(self, job: BaseJob, job_run_id: int) -> None:
        max_wait_time = 2 * 60
        expected_job_statuses = [RunLifeCycleState.SKIPPED, RunLifeCycleState.RUNNING]

        # Initial wait to give the job time to start or be marked as skipped
        time.sleep(5)

        job_status = None
        job_run = self.client.jobs.get_run(job_run_id)

        while time.time() - (job_run.start_time / 1_000) < max_wait_time and job_status is None:
            job_status = job_run.state.life_cycle_state
            if job_status not in expected_job_statuses:
                # Databricks jobs have a start-up time associated that will always run longer than the short wait time
                # associated with this loop. As a result we only check for three statuses to ensure that the job has
                # been picked up for processing.
                job_status = None
            if job_status is None:
                time.sleep(15)
                # Fetch the job_run for next iteration
                job_run = self.client.jobs.get_run(job_run_id)

        if job_status is None:
            msg = (
                f"Job with ID of '{job_run.job_id}' and run with ID of '{job_run_id}' failed to reach one of the"
                f" expected statuses in {max_wait_time} seconds; status is '{job_run.state.life_cycle_state.value}'."
                f" Expected one of the following: {', '.join([status.value for status in expected_job_statuses])}."
            )
            logger.error(msg)
            raise RuntimeError(msg)

        if job_status == RunLifeCycleState.SKIPPED:
            concurrency_limit = job.settings.max_concurrent_runs
            msg = f"Job with ID of `{job_run.job_id}' is currently at its concurrency limit of {concurrency_limit}."
            logger.error(msg)
            raise RuntimeError(msg)


class EmrServerlessStrategy(_AbstractStrategy):
    _client: BaseClient = None

    @property
    def name(self) -> str:
        return "EMR_SERVERLESS"

    @property
    def client(self) -> BaseClient:
        if not self._client:
            self._client = boto3.client("emr-serverless", settings.USASPENDING_AWS_REGION)
        return self._client

    def _get_application_id(self, application_name: str) -> str:
        paginator = self.client.get_paginator("list_applications")
        matched_applications = []
        for list_applications_response in paginator.paginate():
            temp_applications = list_applications_response.get("applications", [])
            matched_applications.extend(
                [application for application in temp_applications if application["name"] == application_name]
            )

        match len(matched_applications):
            case 1:
                application_id = matched_applications[0]["id"]
            case 0:
                raise ValueError(f"No EMR Serverless application found with name '{application_name}'")
            case _:
                arns_to_log = [application["arn"] for application in matched_applications]
                raise ValueError(
                    f"More than 1 EMR Serverless application found with name '{application_name}': {arns_to_log}"
                )

        return application_id

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict:
        application_id = kwargs.get("application_id")
        application_name = kwargs.get("application_name")
        execution_role_arn = kwargs.get("execution_role_arn")

        if not execution_role_arn:
            raise ValueError(f"Execution role ARN is required to start an EMR Serverless job")
        elif not application_name and not application_id:
            raise ValueError(f"Application Name or ID is required to start an EMR Serverless job")
        elif application_name and not application_id:
            application_id = self._get_application_id(application_name)

        response = self.client.start_job_run(
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            name=job_name,
            mode="BATCH",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{CONFIG.SPARK_S3_BUCKET}/master/manage.py",
                    "entryPointArguments": [command_name, *command_options],
                }
            },
            # TODO: Requires updating to EMR 7
            # retryPolicy={"maxAttempts": 2},
        )
        return response


class LocalStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "LOCAL"

    @staticmethod
    @contextmanager
    def _get_spark_session() -> Generator["SparkSession", None, None]:
        from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session

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

    @staticmethod
    def _run_in_container(job_name: str, command_name: str, command_options: list[str]) -> str:
        import docker

        client = docker.from_env()
        try:
            template_container = client.containers.get("spark-submit")
        except docker.errors.NotFound:
            logger.exception(
                "The 'spark-submit' container was not found. Please create this container first via the supported"
                " spark-submit docker compose workflow."
            )
            raise
        image_name = template_container.attrs["Config"]["Image"]
        image_exists = client.images.list(name=image_name)
        if not image_exists:
            msg = (
                f"The '{image_name}' image was not found. Please create this image first via the supported"
                " spark docker compose workflows."
            )
            logger.error(msg)
            raise RuntimeError(msg)
        volumes = template_container.attrs["Mounts"]
        environment_variables = template_container.attrs["Config"]["Env"]
        network = template_container.attrs["HostConfig"]["NetworkMode"]
        options_as_string = " ".join(command_options)
        container_name = f"spark-submit_{job_name}_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        required_jars = ",".join([*SPARK_SESSION_JARS, OPTIONAL_SPARK_HIVE_JAR])
        client.containers.run(
            image_name,
            name=container_name,
            network=network,
            command=(
                'spark-submit --driver-memory "2g"'
                f" --packages {required_jars}"
                f" /project/manage.py {command_name} {options_as_string}"
            ),
            environment=[
                f"COMPONENT_NAME={command_name} {options_as_string}",
                *environment_variables,
            ],
            volumes=[f"{volume['Source']}:{volume['Destination']}" for volume in volumes],
        )
        return container_name

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict | None:
        run_as_container = kwargs.get("run_as_container", False)
        run_details = None
        try:
            if run_as_container:
                container_name = self._run_in_container(job_name, command_name, command_options)
                run_details = {"container_name": container_name}
            else:
                with self._get_spark_session():
                    call_command(command_name, *command_options)
        except Exception:
            logger.exception(f"Failed on command: {command_name} {' '.join(command_options)}")
            raise
        return run_details


class DuckDBStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "DUCKDB"

    @staticmethod
    @contextmanager
    def _get_spark_session() -> Generator["SparkSession", None, None]:
        spark = DuckDBSparkSession.builder.getOrCreate()
        yield spark
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

    def start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict | None:
        logger.info(f'Starting {job_name} on {self.strategy.name}: "{command_name} {" ".join(command_options)}"')
        run_details = self.strategy.handle_start(job_name, command_name, command_options, **kwargs)

        if run_details is None:
            msg = "Job completed successfully"
        else:
            msg = f"Job run successfully started; {run_details}"

        logger.info(msg)

        return run_details
