import time
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import RunLifeCycleState

from usaspending_api.common.spark.jobs import DatabricksStrategy, EmrServerlessStrategy, LocalStrategy, SparkJobs


@patch("usaspending_api.common.spark.jobs.EmrServerlessStrategy.handle_start")
@patch("usaspending_api.common.spark.jobs.DatabricksStrategy.handle_start")
@patch("usaspending_api.common.spark.jobs.LocalStrategy.handle_start")
def test_set_strategy(local_strategy_start, databricks_strategy_start, emr_serverless_strategy_start):
    spark_job = SparkJobs(LocalStrategy())
    assert spark_job.strategy.name == "LOCAL"
    spark_job.start("test", "test", ["test"])
    assert local_strategy_start.call_count == 1

    spark_job.strategy = DatabricksStrategy()
    assert spark_job.strategy.name == "DATABRICKS"
    spark_job.start("test", "test", ["test"])
    assert databricks_strategy_start.call_count == 1

    spark_job.strategy = EmrServerlessStrategy()
    assert spark_job.strategy.name == "EMR_SERVERLESS"
    spark_job.start("test", "test", ["test"])
    assert emr_serverless_strategy_start.call_count == 1


@patch("usaspending_api.common.spark.jobs.DatabricksStrategy.client")
def test_databricks_strategy_handle_start(databricks_strategy_client):
    mock_job = MagicMock()
    mock_job.job_id = 1

    mock_job_run_wait = MagicMock()
    mock_job_run_wait.bind = MagicMock(return_value={"run_id": 10})

    mock_job_run = MagicMock()
    mock_job_run.job_id = 1
    mock_job_run.start_time = time.time() * 1_000
    mock_job_run.state.life_cycle_state = RunLifeCycleState.RUNNING

    mock_jobs = MagicMock()
    mock_jobs.list = MagicMock(return_value=[mock_job])
    mock_jobs.run_now = MagicMock(return_value=mock_job_run_wait)
    mock_jobs.get_run = MagicMock(return_value=mock_job_run)

    databricks_strategy_client.jobs = mock_jobs

    strategy = DatabricksStrategy()
    assert strategy._get_job("test_job_name") == mock_job

    spark_job = SparkJobs(DatabricksStrategy())
    assert spark_job.start(job_name="", command_name="", command_options=[""]) == {"job_id": 1, "run_id": 10}


@patch("usaspending_api.common.spark.jobs.EmrServerlessStrategy.client")
def test_emr_serverless_strategy_handle_start(emr_serverless_strategy_client):
    mock_application = MagicMock()
    mock_application.application_id = 1
    mock_application.name = "application_1"

    mock_application_paginator = MagicMock()
    mock_application_paginator.paginate = MagicMock(
        return_value=[{"applications": [{"id": mock_application.application_id, "name": mock_application.name}]}]
    )

    emr_serverless_strategy_client.get_paginator = MagicMock(return_value=mock_application_paginator)
    emr_serverless_strategy_client.start_job_run = MagicMock()

    strategy = EmrServerlessStrategy()
    assert strategy._get_application_id("application_1") == 1

    emr_serverless_strategy_client.reset_mock()

    spark_job = SparkJobs(strategy)
    spark_job.start(
        job_name="",
        command_name="",
        command_options=[""],
        application_id="Some ID",
        application_name="application_1",
        execution_role_arn="Some ARN",
    )
    assert emr_serverless_strategy_client.get_paginator.call_count == 0
    assert emr_serverless_strategy_client.start_job_run.call_count == 1

    emr_serverless_strategy_client.reset_mock()

    spark_job = SparkJobs(strategy)
    spark_job.start(
        job_name="",
        command_name="",
        command_options=[""],
        application_id=None,
        application_name="application_1",
        execution_role_arn="Some ARN",
    )
    assert emr_serverless_strategy_client.get_paginator.call_count == 1
    assert emr_serverless_strategy_client.start_job_run.call_count == 1
