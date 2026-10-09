from contextlib import contextmanager
from typing import Any, Generator

from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from pyspark.sql import Column, SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.download.delta_downloads.filters.account_filters import (
    AccountDownloadFilters,
)
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


def collect_concat(
    col_name: str | Column | DuckDBSparkColumn,
    spark: SparkSession | DuckDBSparkSession,
    concat_str: str = "; ",
    alias: str | None = None,
) -> Column | DuckDBSparkColumn:
    """Aggregates columns into a string of values seperated by some delimiter"""

    if isinstance(spark, DuckDBSparkSession):
        from duckdb.experimental.spark.sql import functions as sf

        if alias is None and isinstance(col_name, str):
            alias = col_name
        elif alias is None and not isinstance(col_name, str):
            # DuckDB doesn't have a "._jc" property like PySpark does so we need a string for the alias
            raise TypeError(f"`col_name` must be a string for DuckDB, but got {type(col_name)}")

        # collect_set() is not implemented in DuckDB's Spark API, but the `list_distinct` SQL method should work
        return sf.concat_ws(
            concat_str,
            sf.sort_array(sf.call_function("list_distinct", sf.call_function("array_agg", col_name))),
        ).alias(alias)
    else:
        from pyspark.sql import functions as sf

        if alias is None:
            alias = col_name if isinstance(col_name, str) else str(col_name._jc)

        return sf.concat_ws(concat_str, sf.sort_array(sf.collect_set(col_name))).alias(alias)


def filter_submission_and_sum(
    col_name: str,
    filters: AccountDownloadFilters,
    spark: SparkSession | DuckDBSparkSession,
) -> Column:
    if isinstance(spark, DuckDBSparkSession):
        from duckdb.experimental.spark.sql import functions as sf
    else:
        from pyspark.sql import functions as sf

    filter_column = (
        sf.when(
            sf.col("submission_id").isin(
                get_submission_ids_for_periods(
                    filters.reporting_fiscal_year,
                    filters.reporting_fiscal_quarter,
                    filters.reporting_fiscal_period,
                )
            ),
            sf.col(col_name),
        )
        .otherwise(None)
        .alias(col_name)
    )
    return sf.sum(filter_column).alias(col_name)


@contextmanager
def prepare_spark(
    udf_kwarg_list: list[dict[str, Any]] | None = None,
    create_temp_views: bool = False,
    create_temp_broker_views: bool = False,
    extra_table_names: list[str] | None = None,
) -> Generator[SparkSession, None, None]:
    if not create_temp_views and (create_temp_broker_views or extra_table_names):
        raise ValueError(
            "'create_temp_views' must be True in order to use 'create_temp_broker_views' or 'extra_table_names'"
        )

    extra_conf = {**DEFAULT_EXTRA_CONF}
    spark = get_active_spark_session()
    spark_created_by_command = False
    if not spark:
        spark_created_by_command = True
        spark = configure_spark_session(**extra_conf, spark_context=spark)  # type: SparkSession

    udf_kwarg_list = udf_kwarg_list or []
    for udf_kwarg in udf_kwarg_list:
        spark.udf.register(**udf_kwarg)

    if create_temp_views:
        create_ref_temp_views(spark, create_broker_views=create_temp_broker_views, extra_table_names=extra_table_names)

    yield spark

    if spark_created_by_command:
        spark.stop()
