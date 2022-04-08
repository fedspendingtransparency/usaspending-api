"""Make sure the spark context can be accessed and print versions

    Running Locally:
        (1) Review config values in usaspending_api/app_config/local.py and override any as needed in
            a .env file
        (2) Run:
            python3 usaspending_api/etl/spark/prototypes/boto3_write_to_s3.py
"""
import sys
import os

from pyspark.sql import SparkSession

print("==== [ENVIRONMENT] ====")
[print(f"{k}={v}") for k, v in os.environ.items()]
print("==== [SYS.PATH] ====")
[print(i) for i in sys.path]


from usaspending_api.common.helpers.spark_helpers import configure_spark_session  # noqa


def main():
    spark = configure_spark_session()  # type: SparkSession
    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    print(versions)


if __name__ == "__main__":
    main()
