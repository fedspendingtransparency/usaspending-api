"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.
"""
import sys
from common.helpers.spark_helpers import configure_spark_session
from config import CONFIG


def test_spark_app_run():
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.
    """
    spark = configure_spark_session(app_name=CONFIG.COMPONENT_NAME)  # type: SparkSession
    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    print(versions)
