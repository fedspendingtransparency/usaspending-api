"""Make sure the spark context can be accessed and print versions

    Running Locally:
        (1) Review config values in usaspending_api/app_config/local.py and override any as needed in
            a .env file
        (2) Run:
            Run with spark-submit container as "Driver" (preferred):
            make docker-compose-run args="--rm -e COMPONENT_NAME='Spark Version Check' spark-submit \
                /project/usaspending_api/etl/spark/prototypes/spark_version_check.py"
            Run with desktop dev env as "Driver":
            SPARK_LOCAL_IP=127.0.0.1 spark-submit \
                usaspending_api/etl/spark/prototypes/spark_version_check.py
"""
import os
import sys

from pyspark.sql import SparkSession
from usaspending_api.config import CONFIG

print("==== [ENVIRONMENT] ====")
[print(f"{k}={v}") for k, v in os.environ.items()]
print("==== [SYS.PATH] ====")
[print(i) for i in sys.path]

from usaspending_api.common.helpers.spark_helpers import configure_spark_session  # noqa


def main():
    spark = configure_spark_session(app_name=CONFIG.COMPONENT_NAME)  # type: SparkSession
    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    print(versions)


if __name__ == "__main__":
    main()
