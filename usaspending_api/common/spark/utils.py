from pyspark.sql import functions as sf, Column

from usaspending_api.submissions.helpers import get_submission_ids_for_periods
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters


def collect_concat(col_name: str | Column, concat_str: str = "; ", alias: str | None = None) -> Column:
    """Aggregates columns into a string of values seperated by some delimiter"""
    if alias is None:
        alias = col_name if isinstance(col_name, str) else str(col_name._jc)
    return sf.concat_ws(concat_str, sf.sort_array(sf.collect_set(col_name))).alias(alias)


def filter_submission_and_sum(col_name: str, filters: AccountDownloadFilters) -> Column:
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
