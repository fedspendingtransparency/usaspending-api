from contextlib import contextmanager
from typing import Generator

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)


@contextmanager
def prepare_spark() -> Generator[SparkSession, None, None]:
    extra_conf = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
        "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
        "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
    }

    # Create the Spark Session
    spark = get_active_spark_session()
    spark_created_by_command = False
    if not spark:
        spark_created_by_command = True
        spark = configure_spark_session(**extra_conf, spark_context=spark)

    # Create UDFs for Business Categories
    spark.udf.register(
        name="get_business_categories_fabs", f=get_business_categories_fabs, returnType=ArrayType(StringType())
    )
    spark.udf.register(
        name="get_business_categories_fpds", f=get_business_categories_fpds, returnType=ArrayType(StringType())
    )

    yield spark

    if spark_created_by_command:
        spark.stop()
