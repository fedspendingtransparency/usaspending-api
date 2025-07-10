from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf, Column

from usaspending_api.download.management.commands.delta_downloads.award_financial.filters import AccountDownloadFilter
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class AbstractAccountDownloadDataFrameBuilder(ABC):

    def __init__(
        self,
        spark: SparkSession,
        account_download_filter: AccountDownloadFilter,
        table_name: str = "rpt.account_download",
    ):
        self.reporting_fiscal_year = account_download_filter.fy
        self.reporting_fiscal_quarter = account_download_filter.quarter or account_download_filter.period // 3
        self.reporting_fiscal_period = account_download_filter.period or account_download_filter.quarter * 3
        self.agency = account_download_filter.agency
        self.federal_account_id = account_download_filter.federal_account
        self.budget_function = account_download_filter.budget_function
        self.budget_subfunction = account_download_filter.budget_subfunction
        self.def_codes = account_download_filter.def_codes
        self.df: DataFrame = spark.table(table_name)

    @property
    def dynamic_filters(self) -> Column:

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
            Condition(
                name="agency", condition=sf.col("funding_toptier_agency_id") == self.agency, apply=bool(self.agency)
            ),
            Condition(
                name="federal account",
                condition=sf.col("federal_account_id") == self.federal_account_id,
                apply=bool(self.federal_account_id),
            ),
            Condition(
                name="budget function",
                condition=sf.col("budget_function_code") == self.budget_function,
                apply=bool(self.budget_function),
            ),
            Condition(
                name="budget subfunction",
                condition=sf.col("budget_subfunction_code") == self.budget_subfunction,
                apply=bool(self.budget_subfunction),
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

    @property
    def non_zero_filters(self) -> Column:
        return (
            (sf.col("gross_outlay_amount_FYB_to_period_end") != 0)
            | (sf.col("USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig") != 0)
            | (sf.col("USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig") != 0)
            | (sf.col("transaction_obligated_amount") != 0)
        )

    @staticmethod
    def collect_concat(col_name: str, concat_str: str = "; ") -> Column:
        return sf.concat_ws(concat_str, sf.sort_array(sf.collect_set(col_name))).alias(col_name)

    @property
    @abstractmethod
    def source_df(self) -> DataFrame: ...


class FederalAccountDownloadDataFrameBuilder(AbstractAccountDownloadDataFrameBuilder):

    def __init__(
        self,
        spark: SparkSession,
        account_download_filter: AccountDownloadFilter,
        table_name: str = "rpt.account_download",
    ):
        super().__init__(spark, account_download_filter, table_name)
        self.agg_cols = {
            "reporting_agency_name": self.collect_concat,
            "budget_function": self.collect_concat,
            "budget_subfunction": self.collect_concat,
            "transaction_obligated_amount": lambda col: sf.sum(col).alias(col),
            "gross_outlay_amount_FYB_to_period_end": self.filter_and_sum,
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": self.filter_and_sum,
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": self.filter_and_sum,
            "last_modified_date": lambda col: sf.max(col).alias(col),
        }
        self.select_cols = (
            [sf.col("federal_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["federal_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + ["last_modified_date"]
        )
        filter_cols = [
            "submission_id",
            "federal_account_id",
            "funding_toptier_agency_id",
            "budget_function_code",
            "budget_subfunction_code",
            "reporting_fiscal_period",
            "reporting_fiscal_quarter",
            "reporting_fiscal_year",
            "quarter_format_flag",
        ]
        self.groupby_cols = [col for col in self.df.columns if col not in list(self.agg_cols) + filter_cols]

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

    def filter_and_sum(self, col_name: str) -> Column:
        return sf.sum(self.filter_to_latest_submissions_for_agencies(col_name)).alias(col_name)

    @property
    def source_df(self) -> DataFrame:
        return (
            self.df.filter(self.dynamic_filters)
            .groupBy(self.groupby_cols)
            .agg(*[agg_func(col) for col, agg_func in self.agg_cols.items()])
            .filter(self.non_zero_filters)
            .select(self.select_cols)
        )


class TreasuryAccountDownloadDataFrameBuilder(AbstractAccountDownloadDataFrameBuilder):

    @property
    def source_df(self) -> DataFrame:
        select_cols = (
            [sf.col("treasury_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["treasury_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + ["last_modified_date"]
        )
        return self.df.filter(self.dynamic_filters & self.non_zero_filters).select(select_cols)
