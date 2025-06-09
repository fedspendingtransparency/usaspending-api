from dataclasses import dataclass
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf, Column

from usaspending_api.submissions.helpers import get_submission_ids_for_periods
from usaspending_api.download.management.commands.delta_downloads.award_financial.columns import (
    groupby_cols,
    select_cols,
)


@dataclass
class AccountDownloadFilter:
    year: int
    month: int | None = None
    quarter: int | None = None
    agency: int | None = None
    federal_account_id: int | None = None
    def_codes: list[str] | None = None

    def __post_init__(self):
        if self.month is None and self.quarter is None:
            raise ValueError("Must define month or quarter.")
        elif self.month is not None and self.quarter is not None:
            raise ValueError("Month and quarter are mutually exclusive.")


class AccountDownloadDataFrameBuilder:

    def __init__(
        self,
        spark: SparkSession,
        account_download_filter: AccountDownloadFilter,
    ):
        self.reporting_fiscal_year = account_download_filter.year
        self.reporting_fiscal_quarter = account_download_filter.quarter
        self.reporting_fiscal_period = account_download_filter.month
        self.agency = account_download_filter.agency
        self.federal_account_id = account_download_filter.federal_account_id
        self.def_codes = account_download_filter.def_codes
        self.df = spark.table("rpt.account_download")
        self.groupby_cols = groupby_cols
        self.select_cols = select_cols

    def filter_to_latest_submissions_for_agencies(self, col_name: str, otherwise: Any = None) -> Column:
        """Filter to the latest submission regardless of whether the agency submitted on a monthly or quarterly basis"""
        return (
            sf.when(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.reporting_fiscal_year, self.reporting_fiscal_quarter, self.reporting_fiscal_period
                    )
                ),
                sf.col(col_name),
            )
            .otherwise(otherwise)
            .alias(col_name)
        )

    @property
    def combined_filters(self) -> Column:

        @dataclass
        class Condition:
            name: str
            condition: Column
            apply: bool

        conditions = [
            Condition(name="year", condition=sf.col("reporting_fiscal_year") == self.reporting_fiscal_year, apply=True),
            Condition(
                name="quarter or month",
                condition=(
                    (sf.col("reporting_fiscal_period") <= self.reporting_fiscal_period) & ~sf.col("quarter_format_flag")
                )
                | (
                    (sf.col("reporting_fiscal_quarter") <= self.reporting_fiscal_quarter)
                    & sf.col("quarter_format_flag")
                ),
                apply=True,
            ),
            Condition(name="agency", condition=sf.col("agency_code") == self.agency, apply=bool(self.agency)),
            Condition(
                name="federal account",
                condition=sf.col("federal_account_id") == self.federal_account_id,
                apply=bool(self.federal_account_id),
            ),
            Condition(
                name="def_codes",
                condition=sf.col("disaster_emergency_fund_code").isin(self.def_codes),
                apply=bool(self.def_codes),
            ),
        ]
        return reduce(
            lambda x, y: x & y,
            [condition.condition for condition in conditions if condition.apply],
        )

    @staticmethod
    def collect_concat(col_name: str, concat_str: str = "; ") -> Column:
        return sf.concat_ws(concat_str, sf.collect_set(col_name)).alias(col_name)

    @property
    def source_df(self) -> DataFrame:
        return (
            self.df.filter(self.combined_filters)
            .groupBy(self.groupby_cols)
            .agg(
                *[
                    self.collect_concat(col)
                    for col in ["reporting_agency_name", "budget_function", "budget_subfunction"]
                ],
                sf.sum("transaction_obligated_amount").alias("transaction_obligated_amount"),
                *[
                    sf.sum(self.filter_to_latest_submissions_for_agencies(col)).alias(col)
                    for col in [
                        "gross_outlay_amount_FYB_to_period_end",
                        "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
                        "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
                    ]
                ],
                sf.max(sf.col("last_modified_date")).alias("last_modified_date"),
            )
            .filter(
                (sf.col("gross_outlay_amount_FYB_to_period_end") != 0)
                | (sf.col("USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig") != 0)
                | (sf.col("USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig") != 0)
                | (sf.col("transaction_obligated_amount") != 0)
            )
            .select(self.select_cols)
        )
