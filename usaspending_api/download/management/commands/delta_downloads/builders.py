from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf

from usaspending_api.download.management.commands.delta_downloads.filters import AccountDownloadFilter
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class AbstractAccountDownloadDataFrameBuilder(ABC):
    def __init__(
        self,
        spark: SparkSession,
        account_download_filter: AccountDownloadFilter,
        account_balances_table: str = "rpt.account_balances_download",
        award_financial_table: str = "rpt.account_download",
        object_class_program_activity_download_table: str = "rpt.object_class_program_activity_download",
    ):
        # Resolve Filters
        self.reporting_fiscal_year = account_download_filter.fy
        self.submission_types = account_download_filter.submission_types
        self.reporting_fiscal_quarter = account_download_filter.quarter or account_download_filter.period // 3
        self.reporting_fiscal_period = account_download_filter.period or account_download_filter.quarter * 3
        self.agency = account_download_filter.agency
        self.federal_account_id = account_download_filter.federal_account
        self.budget_function = account_download_filter.budget_function
        self.budget_subfunction = account_download_filter.budget_subfunction
        self.def_codes = account_download_filter.def_codes

        # Base Dataframes
        self._award_financial_df: DataFrame = spark.table(award_financial_table)
        self._object_class_program_activity_df: DataFrame = spark.table(object_class_program_activity_download_table)
        self._account_balances_df: DataFrame = spark.table(account_balances_table)

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
    def award_financial_groupby_cols(self) -> list[str]:
        return [
            "federal_owning_agency_name",
            "federal_account_symbol",
            "federal_account_name",
            "agency_identifier_name",
            "program_activity_code",
            "program_activity_name",
            "object_class_code",
            "object_class_name",
            "direct_or_reimbursable_funding_source",
            "disaster_emergency_fund_code",
            "disaster_emergency_fund_name",
            "award_unique_key",
            "award_id_piid",
            "parent_award_id_piid",
            "award_id_fain",
            "award_id_uri",
            "award_base_action_date",
            "award_latest_action_date",
            "period_of_performance_start_date",
            "period_of_performance_current_end_date",
            "ordering_period_end_date",
            "idv_type_code",
            "idv_type",
            "prime_award_base_transaction_description",
            "awarding_agency_code",
            "awarding_agency_name",
            "awarding_subagency_code",
            "awarding_subagency_name",
            "awarding_office_code",
            "awarding_office_name",
            "funding_agency_code",
            "funding_agency_name",
            "funding_sub_agency_code",
            "funding_sub_agency_name",
            "funding_office_code",
            "funding_office_name",
            "recipient_uei",
            "recipient_duns",
            "recipient_name",
            "recipient_name_raw",
            "recipient_parent_uei",
            "recipient_parent_duns",
            "recipient_parent_name",
            "recipient_parent_name_raw",
            "recipient_country",
            "recipient_state",
            "recipient_county",
            "recipient_city",
            "primary_place_of_performance_country",
            "primary_place_of_performance_state",
            "primary_place_of_performance_county",
            "primary_place_of_performance_zip_code",
            "cfda_number",
            "cfda_title",
            "product_or_service_code",
            "product_or_service_code_description",
            "naics_code",
            "naics_description",
            "national_interest_action_code",
            "national_interest_action",
            "submission_period",
            "award_type_code",
            "award_type",
            "recipient_zip_code",
            "award_base_action_date_fiscal_year",
            "award_latest_action_date_fiscal_year",
            "usaspending_permalink",
            "prime_award_summary_recipient_cd_original",
            "prime_award_summary_recipient_cd_current",
            "prime_award_summary_place_of_performance_cd_original",
            "prime_award_summary_place_of_performance_cd_current",
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
            self.collect_concat("reporting_agency_name"),
            self.collect_concat("agency_identifier_name"),
            self.collect_concat("budget_function"),
            self.collect_concat("budget_subfunction"),
            sf.sum(sf.col("budget_authority_unobligated_balance_brought_forward")).alias(
                "budget_authority_unobligated_balance_brought_forward"
            ),
            sf.sum(sf.col("adjustments_to_unobligated_balance_brought_forward_cpe")).alias(
                "adjustments_to_unobligated_balance_brought_forward_cpe"
            ),
            sf.sum(sf.col("budget_authority_appropriated_amount")).alias("budget_authority_appropriated_amount"),
            sf.sum(sf.col("borrowing_authority_amount")).alias("borrowing_authority_amount"),
            sf.sum(sf.col("contract_authority_amount")).alias("contract_authority_amount"),
            sf.sum(sf.col("spending_authority_from_offsetting_collections_amount")).alias(
                "spending_authority_from_offsetting_collections_amount"
            ),
            sf.sum(sf.col("total_other_budgetary_resources_amount")).alias("total_other_budgetary_resources_amount"),
            sf.sum(sf.col("total_budgetary_resources")).alias("total_budgetary_resources"),
            sf.sum(sf.col("obligations_incurred")).alias("obligations_incurred"),
            sf.sum(sf.col("deobligations_or_recoveries_or_refunds_from_prior_year")).alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            sf.sum(sf.col("unobligated_balance")).alias("unobligated_balance"),
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
                    sf.col("gross_outlay_amount"),
                ).otherwise(0)
            ).alias("gross_outlay_amount"),
            sf.sum(sf.col("status_of_budgetary_resources_total")).alias("status_of_budgetary_resources_total"),
            sf.max("last_modified_date").alias("last_modified_date"),
        ]

    @property
    def account_balances_select_cols(self) -> list[Column]:
        return [
            sf.col("owning_agency_name"),
            sf.col("reporting_agency_name"),
            sf.col("submission_period"),
            sf.col("federal_account_symbol"),
            sf.col("federal_account_name"),
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
            self._account_balances_df.filter(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.reporting_fiscal_year, self.reporting_fiscal_quarter, self.reporting_fiscal_period
                    )
                )
            )
            .filter(self.dynamic_filters)
            .groupby("federal_account_symbol", "owning_agency_name", "federal_account_name", "submission_period")
            .agg(*self.account_balances_agg_cols)
            .select(*self.account_balances_select_cols)
        )

    @property
    def object_class_program_activity_groupby_cols(self) -> list[str]:
        return [
            "owning_agency_name",
            "agency_identifier_name",
            "federal_account_symbol",
            "federal_account_name",
            "program_activity_code",
            "program_activity_name",
            "object_class_code",
            "object_class_name",
            "direct_or_reimbursable_funding_source",
            "disaster_emergency_fund_code",
            "disaster_emergency_fund_name",
            "submission_period",
        ]

    @property
    def object_class_program_activity_agg_cols(self) -> dict[str, Column]:
        return {
            "reporting_agency_name": self.collect_concat,
            "budget_function_title": lambda col: self.collect_concat(col_name=col, alias="budget_function"),
            "budget_subfunction_title": lambda col: self.collect_concat(col_name=col, alias="budget_subfunction"),
            "obligations_incurred": lambda col: sf.sum(col).alias(col),
            "obligations_undelivered_orders_unpaid_total": lambda col: sf.sum(col).alias(col),
            "obligations_undelivered_orders_unpaid_total_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL480100_undelivered_orders_obligations_unpaid": lambda col: sf.sum(col).alias(col),
            "USSGL480100_undelivered_orders_obligations_unpaid_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid": lambda col: sf.sum(col).alias(col),
            "obligations_delivered_orders_unpaid_total": lambda col: sf.sum(col).alias(col),
            "obligations_delivered_orders_unpaid_total_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL490100_delivered_orders_obligations_unpaid": lambda col: sf.sum(col).alias(col),
            "USSGL490100_delivered_orders_obligations_unpaid_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid": lambda col: sf.sum(col).alias(col),
            "gross_outlay_amount_FYB_to_period_end": self.filter_and_sum,
            "gross_outlay_amount_FYB": lambda col: sf.sum(col).alias(col),
            "gross_outlays_undelivered_orders_prepaid_total": lambda col: sf.sum(col).alias(col),
            "gross_outlays_undelivered_orders_prepaid_total_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL480200_undelivered_orders_obligations_prepaid_advanced": lambda col: sf.sum(col).alias(col),
            "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid": lambda col: sf.sum(col).alias(col),
            "gross_outlays_delivered_orders_paid_total": lambda col: sf.sum(col).alias(col),
            "gross_outlays_delivered_orders_paid_total_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL490200_delivered_orders_obligations_paid": lambda col: sf.sum(col).alias(col),
            "USSGL490800_authority_outlayed_not_yet_disbursed": lambda col: sf.sum(col).alias(col),
            "USSGL490800_authority_outlayed_not_yet_disbursed_FYB": lambda col: sf.sum(col).alias(col),
            "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid": lambda col: sf.sum(col).alias(col),
            "deobligations_or_recoveries_or_refunds_from_prior_year": lambda col: sf.sum(col).alias(col),
            "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig": lambda col: sf.sum(col).alias(col),
            "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig": lambda col: sf.sum(col).alias(col),
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": self.filter_and_sum,
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": self.filter_and_sum,
            "USSGL483100_undelivered_orders_obligations_transferred_unpaid": lambda col: sf.sum(col).alias(col),
            "USSGL493100_delivered_orders_obligations_transferred_unpaid": lambda col: sf.sum(col).alias(col),
            "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced": lambda col: sf.sum(col).alias(col),
            "last_modified_date": lambda col: sf.max(col).alias(col),
        }

    @property
    def object_class_program_activity_select_cols(self) -> list[Any]:
        return [
            col
            for col in query_paths["object_class_program_activity"]["federal_account"].keys()
            if not col.startswith("last_modified_date")
        ] + ["last_modified_date"]

    @property
    def object_class_program_activity(self) -> DataFrame:
        return (
            self._object_class_program_activity_df.filter(self.dynamic_filters)
            .filter(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.reporting_fiscal_year, self.reporting_fiscal_quarter, self.reporting_fiscal_period
                    )
                )
            )
            .groupBy(self.object_class_program_activity_groupby_cols)
            .agg(*[agg_func(col) for col, agg_func in self.object_class_program_activity_agg_cols.items()])
            .drop(
                *[
                    sf.col(f"object_class_program_activity_download.{col}")
                    for col in self.object_class_program_activity_agg_cols
                ]
            )
            .select(self.object_class_program_activity_select_cols)
        )

    @property
    def award_financial(self) -> DataFrame:
        return (
            self._award_financial_df.filter(self.dynamic_filters)
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
            sf.col("data_source"),
            sf.col("appropriation_account_balances_id"),
            sf.col("budget_authority_unobligated_balance_brought_forward"),
            sf.col("adjustments_to_unobligated_balance_brought_forward_cpe"),
            sf.col("budget_authority_appropriated_amount"),
            sf.col("borrowing_authority_amount"),
            sf.col("contract_authority_amount"),
            sf.col("spending_authority_from_offsetting_collections_amount"),
            sf.col("total_other_budgetary_resources_amount"),
            sf.col("total_budgetary_resources"),
            sf.col("gross_outlay_amount"),
            sf.col("deobligations_or_recoveries_or_refunds_from_prior_year"),
            sf.col("unobligated_balance"),
            sf.col("status_of_budgetary_resources_total"),
            sf.col("obligations_incurred"),
            sf.col("drv_appropriation_availability_period_start_date"),
            sf.col("drv_appropriation_availability_period_end_date"),
            sf.col("drv_appropriation_account_expired_status"),
            sf.col("drv_obligations_unpaid_amount"),
            sf.col("drv_other_obligated_amount"),
            sf.col("reporting_period_start"),
            sf.col("reporting_period_end"),
            sf.col("appropriation_account_last_modified"),
            sf.col("certified_date"),
            sf.col("create_date"),
            sf.col("update_date"),
            sf.col("final_of_fy"),
            sf.col("submission_id"),
            sf.col("treasury_account_identifier"),
            sf.col("owning_agency_name"),
            sf.col("reporting_agency_name"),
            sf.col("allocation_transfer_agency_identifier_code"),
            sf.col("agency_identifier_code"),
            sf.col("beginning_period_of_availability"),
            sf.col("ending_period_of_availability"),
            sf.col("availability_type_code"),
            sf.col("main_account_code"),
            sf.col("sub_account_code"),
            sf.col("treasury_account_symbol"),
            sf.col("treasury_account_name"),
            sf.col("budget_function"),
            sf.col("budget_subfunction"),
            sf.col("federal_account_symbol"),
            sf.col("federal_account_name"),
            sf.col("agency_identifier_name"),
            sf.col("allocation_transfer_agency_identifier_name"),
            sf.col("submission_period"),
        ]

    @property
    def account_balances_agg_cols(self) -> list[Column]:
        return [sf.max("last_modified_date").alias("max_last_modified_date")]

    @property
    def account_balances_select_cols(self) -> list[Column]:
        return [
            sf.col("owning_agency_name"),
            sf.col("reporting_agency_name"),
            sf.col("submission_period"),
            sf.col("allocation_transfer_agency_identifier_code"),
            sf.col("agency_identifier_code"),
            sf.col("beginning_period_of_availability"),
            sf.col("ending_period_of_availability"),
            sf.col("availability_type_code"),
            sf.col("main_account_code"),
            sf.col("sub_account_code"),
            sf.col("treasury_account_symbol"),
            sf.col("treasury_account_name"),
            sf.col("agency_identifier_name"),
            sf.col("allocation_transfer_agency_identifier_name"),
            sf.col("budget_function"),
            sf.col("budget_subfunction"),
            sf.col("federal_account_symbol"),
            sf.col("federal_account_name"),
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
            sf.col("max_last_modified_date").alias("last_modified_date"),
        ]

    @property
    def account_balances(self) -> DataFrame:
        return (
            self._account_balances_df.filter(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.reporting_fiscal_year, self.reporting_fiscal_quarter, self.reporting_fiscal_period
                    )
                )
            )
            .filter(self.dynamic_filters)
            .groupby(*self.account_balances_groupby_cols)
            .agg(*self.account_balances_agg_cols)
            .select(*self.account_balances_select_cols)
        )

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
        return self._award_financial_df.filter(self.dynamic_filters & self.non_zero_filters).select(select_cols)

    @property
    def object_class_program_activity_groupby_cols(self) -> list[str]:
        return [
            "owning_agency_name",
            "sub_account_code",
            "beginning_period_of_availability",
            "reporting_agency_name",
            "ending_period_of_availability",
            "direct_or_reimbursable_funding_source",
            "allocation_transfer_agency_identifier_code",
            "availability_type_code",
            "federal_account_name",
            "treasury_account_symbol",
            "agency_identifier_code",
            "budget_subfunction_title",
            "object_class_name",
            "program_activity_code",
            "disaster_emergency_fund_name",
            "submission_period",
            "treasury_account_name",
            "main_account_code",
            "federal_account_symbol",
            "budget_function_title",
            "object_class_code",
            "program_activity_name",
            "data_source",
            "financial_accounts_by_program_activity_object_class_id",
            "program_activity_id",
            "object_class_id",
            "prior_year_adjustment",
            "disaster_emergency_fund_code",
            "USSGL480100_undelivered_orders_obligations_unpaid_FYB",
            "USSGL480100_undelivered_orders_obligations_unpaid",
            "USSGL480110_rein_undel_ord_CPE",
            "USSGL483100_undelivered_orders_obligations_transferred_unpaid",
            "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid",
            "USSGL490100_delivered_orders_obligations_unpaid_FYB",
            "USSGL490100_delivered_orders_obligations_unpaid",
            "USSGL490110_rein_deliv_ord_CPE",
            "USSGL493100_delivered_orders_obligations_transferred_unpaid",
            "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid",
            "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB",
            "USSGL480200_undelivered_orders_obligations_prepaid_advanced",
            "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced",
            "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid",
            "USSGL490200_delivered_orders_obligations_paid",
            "USSGL490800_authority_outlayed_not_yet_disbursed_FYB",
            "USSGL490800_authority_outlayed_not_yet_disbursed",
            "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid",
            "obligations_undelivered_orders_unpaid_total_FYB",
            "obligations_undelivered_orders_unpaid_total",
            "obligations_delivered_orders_unpaid_total",
            "obligations_delivered_orders_unpaid_total_FYB",
            "gross_outlays_undelivered_orders_prepaid_total_FYB",
            "gross_outlays_undelivered_orders_prepaid_total",
            "gross_outlays_delivered_orders_paid_total_FYB",
            "gross_outlays_delivered_orders_paid_total",
            "gross_outlay_amount_FYB",
            "gross_outlay_amount_FYB_to_period_end",
            "obligations_incurred",
            "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig",
            "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig",
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
            "deobligations_or_recoveries_or_refunds_from_prior_year",
            "drv_obligations_incurred_by_program_object_class",
            "drv_obligations_undelivered_orders_unpaid",
            "reporting_period_start",
            "reporting_period_end",
            "last_modified_date",
            "certified_date",
            "create_date",
            "update_date",
            "submission_id",
            "treasury_account_id",
            "agency_identifier_name",
            "allocation_transfer_agency_identifier_name",
        ]

    @property
    def object_class_program_activity_agg_cols(self) -> dict[str, Column]:
        return {
            "last_modified_date": lambda col: sf.max(col).alias(col),
        }

    @property
    def object_class_program_activity_select_cols(self) -> list[Any]:
        return [
            col
            for col in query_paths["object_class_program_activity"]["treasury_account"].keys()
            if not col.startswith("last_modified_date") and col not in ["budget_function", "budget_subfunction"]
        ] + [
            "last_modified_date",
            sf.col("budget_function_title").alias("budget_function"),
            sf.col("budget_subfunction_title").alias("budget_subfunction"),
        ]

    @property
    def object_class_program_activity(self) -> DataFrame:
        return (
            self._object_class_program_activity_df.filter(self.dynamic_filters)
            .filter(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.reporting_fiscal_year, self.reporting_fiscal_quarter, self.reporting_fiscal_period
                    )
                )
            )
            .groupBy(self.object_class_program_activity_groupby_cols)
            .agg(*[agg_func(col) for col, agg_func in self.object_class_program_activity_agg_cols.items()])
            .drop(
                *[
                    sf.col(f"object_class_program_activity_download.{col}")
                    for col in self.object_class_program_activity_agg_cols
                ]
            )
            .select(self.object_class_program_activity_select_cols)
        )
