from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf, Column

from usaspending_api.download.management.commands.delta_downloads.filters import AccountDownloadFilter
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class AbstractAccountDownloadDataFrameBuilder(ABC):

    def __init__(
        self,
        spark: SparkSession,
        account_download_filter: AccountDownloadFilter,
        table_name: str = "rpt.account_download",
    ):
        self.spark = spark
        self.reporting_fiscal_year = account_download_filter.fy
        self.submission_types = account_download_filter.submission_types
        self.reporting_fiscal_quarter = account_download_filter.quarter or account_download_filter.period // 3
        self.reporting_fiscal_period = account_download_filter.period or account_download_filter.quarter * 3
        self.agency = account_download_filter.agency
        self.federal_account_id = account_download_filter.federal_account
        self.budget_function = account_download_filter.budget_function
        self.budget_subfunction = account_download_filter.budget_subfunction
        self.def_codes = account_download_filter.def_codes
        self.award_financial_df: DataFrame = spark.table(table_name)

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
    def collect_concat(col_name: str, concat_str: str = "; ", alias: str | None = None) -> Column:
        if alias is None:
            alias = col_name
        return sf.concat_ws(concat_str, sf.sort_array(sf.collect_set(col_name))).alias(alias)

    @property
    def fy_quarter_period(self) -> Column:
        return sf.when(
            sf.col("quarter_format_flag"),
            sf.concat(sf.lit("FY"), sf.col("reporting_fiscal_year"), sf.lit("Q"), sf.col("reporting_fiscal_quarter")),
        ).otherwise(
            sf.concat(
                sf.lit("FY"),
                sf.col("reporting_fiscal_year"),
                sf.lit("P"),
                sf.lpad(sf.col("reporting_fiscal_period"), 2, "0"),
            )
        )

    @property
    def _account_balances_df(self) -> DataFrame:
        aab = self.spark.table("global_temp.appropriation_account_balances")
        sa = self.spark.table("global_temp.submission_attributes")
        taa = self.spark.table("global_temp.treasury_appropriation_account")
        cgac_aid = self.spark.table("global_temp.cgac")
        cgac_ata = self.spark.table("global_temp.cgac")
        fa = self.spark.table("global_temp.federal_account")
        ta = self.spark.table("global_temp.toptier_agency")

        return (
            aab.join(sa, on="submission_id", how="inner")
            .join(taa, on="treasury_account_identifier", how="leftouter")
            .join(
                cgac_aid.withColumnRenamed("agency_name", "agency_identifier_name"),
                on=(taa.agency_id == cgac_aid.cgac_code),
                how="leftouter",
            )
            .join(
                cgac_ata.withColumnRenamed("agency_name", "allocation_transfer_agency_identifier_name"),
                on=(taa.allocation_transfer_agency_id == cgac_ata.cgac_code),
                how="leftouter",
            )
            .join(fa, on=taa.federal_account_id == fa.id, how="leftouter")
            .join(ta, on=fa.parent_toptier_agency_id == ta.toptier_agency_id, how="leftouter")
            .filter(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.reporting_fiscal_year, self.reporting_fiscal_quarter, self.reporting_fiscal_period
                    )
                )
            )
            .filter(self.dynamic_filters)
            .withColumn("submission_period", self.fy_quarter_period)
        )

    @property
    @abstractmethod
    def account_balances(self) -> DataFrame: ...

    @property
    @abstractmethod
    def object_class_program_activity(self) -> DataFrame: ...

    @property
    @abstractmethod
    def award_financial(self) -> DataFrame: ...

    @property
    def source_dfs(self) -> list[DataFrame]:
        return [getattr(self, submission_type) for submission_type in self.submission_types]


class FederalAccountDownloadDataFrameBuilder(AbstractAccountDownloadDataFrameBuilder):

    @property
    def award_financial_agg_cols(self) -> dict[str, Column]:
        return {
            "reporting_agency_name": self.collect_concat,
            "budget_function": self.collect_concat,
            "budget_subfunction": self.collect_concat,
            "transaction_obligated_amount": lambda col: sf.sum(col).alias(col),
            "gross_outlay_amount_FYB_to_period_end": self.filter_and_sum,
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": self.filter_and_sum,
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": self.filter_and_sum,
            "last_modified_date": lambda col: sf.max(col).alias(col),
        }

    @property
    def award_financial_select_cols(self) -> list[Any]:
        return (
            [sf.col("federal_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["federal_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + ["last_modified_date"]
        )

    @property
    def award_financial_filter_cols(self) -> list[str]:
        return [
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

    @property
    def award_financial_groupby_cols(self) -> list[Any]:
        return [
            col
            for col in self.award_financial_df.columns
            if col not in list(self.award_financial_agg_cols) + self.award_financial_filter_cols
        ]

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
    def account_balances_agg_cols(self) -> list[Column]:
        return [
            self.collect_concat("submission_attributes.reporting_agency_name", alias="reporting_agency_name"),
            self.collect_concat("agency_identifier_name"),
            self.collect_concat("budget_function_title", alias="budget_function"),
            self.collect_concat("budget_subfunction_title", alias="budget_subfunction"),
            sf.sum(sf.col("budget_authority_unobligated_balance_brought_forward_fyb")).alias(
                "budget_authority_unobligated_balance_brought_forward"
            ),
            sf.sum(sf.col("adjustments_to_unobligated_balance_brought_forward_cpe")).alias(
                "adjustments_to_unobligated_balance_brought_forward_cpe"
            ),
            sf.sum(sf.col("budget_authority_appropriated_amount_cpe")).alias("budget_authority_appropriated_amount"),
            sf.sum(sf.col("borrowing_authority_amount_total_cpe")).alias("borrowing_authority_amount"),
            sf.sum(sf.col("contract_authority_amount_total_cpe")).alias("contract_authority_amount"),
            sf.sum(sf.col("spending_authority_from_offsetting_collections_amount_cpe")).alias(
                "spending_authority_from_offsetting_collections_amount"
            ),
            sf.sum(sf.col("other_budgetary_resources_amount_cpe")).alias("total_other_budgetary_resources_amount"),
            sf.sum(sf.col("total_budgetary_resources_amount_cpe")).alias("total_budgetary_resources"),
            sf.sum(sf.col("obligations_incurred_total_by_tas_cpe")).alias("obligations_incurred"),
            sf.sum(sf.col("deobligations_recoveries_refunds_by_tas_cpe")).alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            sf.sum(sf.col("unobligated_balance_cpe")).alias("unobligated_balance"),
            sf.sum(
                sf.when(
                    (
                        (
                            sf.col("quarter_format_flag")
                            & (sf.col("reporting_fiscal_quarter") == self.reporting_fiscal_quarter)
                        )
                        | (
                            ~sf.col("quarter_format_flag")
                            & (sf.col("reporting_fiscal_period") == self.reporting_fiscal_period)
                        )
                    )
                    & (sf.col("reporting_fiscal_year") == self.reporting_fiscal_year),
                    sf.col("gross_outlay_amount_by_tas_cpe"),
                ).otherwise(0)
            ).alias("gross_outlay_amount"),
            sf.sum(sf.col("status_of_budgetary_resources_total_cpe")).alias("status_of_budgetary_resources_total"),
            sf.max("published_date").alias("last_modified_date"),
        ]

    @property
    def account_balances_select_cols(self) -> list[Column]:
        return [
            sf.col("name").alias("owning_agency_name"),
            sf.col("reporting_agency_name"),
            sf.col("submission_period"),
            sf.col("federal_account_code").alias("federal_account_symbol"),
            sf.col("account_title").alias("federal_account_name"),
            sf.col("agency_identifier_name"),
            sf.col("budget_function"),
            sf.col("budget_subfunction"),
            sf.col("budget_authority_unobligated_balance_brought_forward"),
            sf.col("adjustments_to_unobligated_balance_brought_forward_cpe"),
            sf.col("budget_authority_appropriated_amount"),
            sf.col("borrowing_authority_amount"),
            sf.col("contract_authority_amount"),
            sf.col("spending_authority_from_offsetting_collections_amount"),
            sf.col("total_other_budgetary_resources_amount"),
            sf.col("total_budgetary_resources"),
            sf.col("obligations_incurred"),
            sf.col("deobligations_or_recoveries_or_refunds_from_prior_year"),
            sf.col("unobligated_balance"),
            sf.col("gross_outlay_amount"),
            sf.col("status_of_budgetary_resources_total"),
            sf.col("last_modified_date"),
        ]

    @property
    def account_balances(self) -> DataFrame:
        return (
            self._account_balances_df.groupby(
                "federal_account_code", "name", "federal_account.account_title", "submission_period"
            )
            .agg(*self.account_balances_agg_cols)
            .select(*self.account_balances_select_cols)
        )

    @property
    def object_class_program_activity(self) -> DataFrame:
        return None

    @property
    def award_financial(self) -> DataFrame:
        return (
            self.award_financial_df.filter(self.dynamic_filters)
            .groupBy(self.award_financial_groupby_cols)
            .agg(*[agg_func(col) for col, agg_func in self.award_financial_agg_cols.items()])
            # drop original agg columns from the dataframe to avoid ambiguous column names
            .drop(*[sf.col(f"account_download.{col}") for col in self.award_financial_agg_cols])
            .filter(self.non_zero_filters)
            .select(self.award_financial_select_cols)
        )


class TreasuryAccountDownloadDataFrameBuilder(AbstractAccountDownloadDataFrameBuilder):

    @property
    def account_balances_groupby_cols(self) -> list[Column]:
        return [
            sf.col(col)
            for col in [
                "appropriation_account_balances.data_source",
                "appropriation_account_balances.appropriation_account_balances_id",
                "appropriation_account_balances.budget_authority_unobligated_balance_brought_forward_fyb",
                "appropriation_account_balances.adjustments_to_unobligated_balance_brought_forward_cpe",
                "appropriation_account_balances.budget_authority_appropriated_amount_cpe",
                "appropriation_account_balances.borrowing_authority_amount_total_cpe",
                "appropriation_account_balances.contract_authority_amount_total_cpe",
                "appropriation_account_balances.spending_authority_from_offsetting_collections_amount_cpe",
                "appropriation_account_balances.other_budgetary_resources_amount_cpe",
                "appropriation_account_balances.total_budgetary_resources_amount_cpe",
                "appropriation_account_balances.gross_outlay_amount_by_tas_cpe",
                "appropriation_account_balances.deobligations_recoveries_refunds_by_tas_cpe",
                "appropriation_account_balances.unobligated_balance_cpe",
                "appropriation_account_balances.status_of_budgetary_resources_total_cpe",
                "appropriation_account_balances.obligations_incurred_total_by_tas_cpe",
                "appropriation_account_balances.drv_appropriation_availability_period_start_date",
                "appropriation_account_balances.drv_appropriation_availability_period_end_date",
                "appropriation_account_balances.drv_appropriation_account_expired_status",
                "appropriation_account_balances.drv_obligations_unpaid_amount",
                "appropriation_account_balances.drv_other_obligated_amount",
                "appropriation_account_balances.reporting_period_start",
                "appropriation_account_balances.reporting_period_end",
                "appropriation_account_balances.last_modified_date",
                "appropriation_account_balances.certified_date",
                "appropriation_account_balances.create_date",
                "appropriation_account_balances.update_date",
                "appropriation_account_balances.final_of_fy",
                "appropriation_account_balances.submission_id",
                "appropriation_account_balances.treasury_account_identifier",
                "agency_identifier_name",
                "allocation_transfer_agency_identifier_name",
                "submission_period",
                "toptier_agency.name",
                "submission_attributes.reporting_agency_name",
                "treasury_appropriation_account.allocation_transfer_agency_id",
                "treasury_appropriation_account.agency_id",
                "treasury_appropriation_account.beginning_period_of_availability",
                "treasury_appropriation_account.ending_period_of_availability",
                "treasury_appropriation_account.availability_type_code",
                "treasury_appropriation_account.main_account_code",
                "treasury_appropriation_account.sub_account_code",
                "treasury_appropriation_account.tas_rendering_label",
                "treasury_appropriation_account.account_title",
                "treasury_appropriation_account.budget_function_title",
                "treasury_appropriation_account.budget_subfunction_title",
                "federal_account.federal_account_code",
                "federal_account.account_title",
            ]
        ]

    @property
    def account_balances_agg_cols(self) -> list[Column]:
        return [sf.max("published_date").alias("max_last_modified_date")]

    @property
    def account_balances_select_cols(self) -> list[Column]:
        return [
            sf.col("toptier_agency.name").alias("owning_agency_name"),
            sf.col("submission_attributes.reporting_agency_name").alias("reporting_agency_name"),
            sf.col("submission_period"),
            sf.col("treasury_appropriation_account.allocation_transfer_agency_id").alias(
                "allocation_transfer_agency_identifier_code"
            ),
            sf.col("treasury_appropriation_account.agency_id").alias("agency_identifier_code"),
            sf.col("treasury_appropriation_account.beginning_period_of_availability").alias(
                "beginning_period_of_availability"
            ),
            sf.col("treasury_appropriation_account.ending_period_of_availability").alias(
                "ending_period_of_availability"
            ),
            sf.col("treasury_appropriation_account.availability_type_code").alias("availability_type_code"),
            sf.col("treasury_appropriation_account.main_account_code").alias("main_account_code"),
            sf.col("treasury_appropriation_account.sub_account_code").alias("sub_account_code"),
            sf.col("treasury_appropriation_account.tas_rendering_label").alias("treasury_account_symbol"),
            sf.col("treasury_appropriation_account.account_title").alias("treasury_account_name"),
            sf.col("agency_identifier_name"),
            sf.col("allocation_transfer_agency_identifier_name"),
            sf.col("treasury_appropriation_account.budget_function_title").alias("budget_function"),
            sf.col("treasury_appropriation_account.budget_subfunction_title").alias("budget_subfunction"),
            sf.col("federal_account.federal_account_code").alias("federal_account_symbol"),
            sf.col("federal_account.account_title").alias("federal_account_name"),
            sf.col("appropriation_account_balances.budget_authority_unobligated_balance_brought_forward_fyb").alias(
                "budget_authority_unobligated_balance_brought_forward"
            ),
            sf.col("appropriation_account_balances.adjustments_to_unobligated_balance_brought_forward_cpe").alias(
                "adjustments_to_unobligated_balance_brought_forward_cpe"
            ),
            sf.col("appropriation_account_balances.budget_authority_appropriated_amount_cpe").alias(
                "budget_authority_appropriated_amount"
            ),
            sf.col("appropriation_account_balances.borrowing_authority_amount_total_cpe").alias(
                "borrowing_authority_amount"
            ),
            sf.col("appropriation_account_balances.contract_authority_amount_total_cpe").alias(
                "contract_authority_amount"
            ),
            sf.col("appropriation_account_balances.spending_authority_from_offsetting_collections_amount_cpe").alias(
                "spending_authority_from_offsetting_collections_amount"
            ),
            sf.col("appropriation_account_balances.other_budgetary_resources_amount_cpe").alias(
                "total_other_budgetary_resources_amount"
            ),
            sf.col("appropriation_account_balances.total_budgetary_resources_amount_cpe").alias(
                "total_budgetary_resources"
            ),
            sf.col("appropriation_account_balances.obligations_incurred_total_by_tas_cpe").alias(
                "obligations_incurred"
            ),
            sf.col("appropriation_account_balances.deobligations_recoveries_refunds_by_tas_cpe").alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            sf.col("appropriation_account_balances.unobligated_balance_cpe").alias("unobligated_balance"),
            sf.col("appropriation_account_balances.gross_outlay_amount_by_tas_cpe").alias("gross_outlay_amount"),
            sf.col("appropriation_account_balances.status_of_budgetary_resources_total_cpe").alias(
                "status_of_budgetary_resources_total"
            ),
            sf.col("max_last_modified_date").alias("last_modified_date"),
        ]

    @property
    def account_balances(self) -> DataFrame:
        return (
            self._account_balances_df.groupby(*self.account_balances_groupby_cols)
            .agg(*self.account_balances_agg_cols)
            .select(*self.account_balances_select_cols)
        )

    @property
    def object_class_program_activity(self) -> DataFrame:
        return None

    @property
    def award_financial(self) -> DataFrame:
        select_cols = (
            [sf.col("treasury_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["treasury_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + ["last_modified_date"]
        )
        return self.award_financial_df.filter(self.dynamic_filters & self.non_zero_filters).select(select_cols)
