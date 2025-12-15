from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from pyspark.sql import Column, SparkSession

from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
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
        return sf.concat_ws(concat_str, sf.sort_array(sf.call_function("list_distinct", col_name))).alias(alias)
    else:
        from pyspark.sql import functions as sf

        if alias is None:
            alias = col_name if isinstance(col_name, str) else str(col_name._jc)

        return sf.concat_ws(concat_str, sf.sort_array(sf.collect_set(col_name))).alias(alias)


def filter_submission_and_sum(
    col_name: str, filters: AccountDownloadFilters, spark: SparkSession | DuckDBSparkSession
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
