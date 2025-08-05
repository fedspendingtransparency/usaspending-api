from unittest.mock import MagicMock, patch

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
    mock_job_run = MagicMock(return_value=mock_job_run_wait)

    mock_jobs = MagicMock()
    mock_jobs.list = MagicMock(return_value=[mock_job])
    mock_jobs.run_now = mock_job_run
    databricks_strategy_client.jobs = mock_jobs

    strategy = DatabricksStrategy()
    assert strategy.get_job_id("test_job_name") == 1

    spark_job = SparkJobs(DatabricksStrategy())
    assert spark_job.start(job_name="", command_name="", command_options=[""]) == {"job_id": 1, "run_id": 10}
