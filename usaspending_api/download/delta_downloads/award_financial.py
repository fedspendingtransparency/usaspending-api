from pyspark.sql import functions as sf, Column, DataFrame, SparkSession

from usaspending_api.common.spark.utils import collect_concat
from usaspending_api.download.delta_downloads.abstract_downloads.account_download import (
    AbstractAccountDownload,
    AccountLevel,
    SubmissionType,
)
from usaspending_api.download.delta_downloads.abstract_factories.account_download_factory import (
    AbstractAccountDownloadFactory,
)
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class AwardFinancialMixin:
    """Shared code between concrete implementations of the AbstractAccountDownload"""

    spark: SparkSession

    filters: AccountDownloadFilters
    dynamic_filters: Column

    @property
    def download_table(self) -> DataFrame:
        return self.spark.table("rpt.award_financial_download")

    @property
    def non_zero_filters(self) -> Column:
        return (
            (sf.col("gross_outlay_amount_FYB_to_period_end") != 0)
            | (sf.col("USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig") != 0)
            | (sf.col("USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig") != 0)
            | (sf.col("transaction_obligated_amount") != 0)
        )


class FederalAccountDownload(AwardFinancialMixin, AbstractAccountDownload):

    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.FEDERAL_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.AWARD_FINANCIAL

    @property
    def group_by_cols(self) -> list[str]:
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

    @property
    def agg_cols(self) -> dict[str, callable]:
        return {
            "reporting_agency_name": collect_concat,
            "budget_function": collect_concat,
            "budget_subfunction": collect_concat,
            "transaction_obligated_amount": lambda col: sf.sum(col).alias(col),
            "gross_outlay_amount_FYB_to_period_end": self.filter_submission_and_sum,
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": self.filter_submission_and_sum,
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": self.filter_submission_and_sum,
            "last_modified_date": lambda col: sf.max(col).alias(col),
        }

    @property
    def select_cols(self) -> list[Column]:
        # TODO: As we move more to dataframes we should replace the "query_paths" implementation
        return (
            [sf.col("federal_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["federal_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + ["last_modified_date"]
        )

    def _build_dataframe(self) -> DataFrame:
        return (
            self.download_table.groupBy(self.group_by_cols)
            .agg(*[agg_func(col) for col, agg_func in self.agg_cols.items()])
            # drop original agg columns from the dataframe to avoid ambiguous column names
            .drop(*[sf.col(f"award_financial_download.{col}") for col in self.agg_cols])
            .filter(self.non_zero_filters)
            .select(self.select_cols)
        )

    def filter_submission_and_sum(self, col_name: str) -> Column:
        filter_column = (
            sf.when(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.filters.reporting_fiscal_year,
                        self.filters.reporting_fiscal_quarter,
                        self.filters.reporting_fiscal_period,
                    )
                ),
                sf.col(col_name),
            )
            .otherwise(None)
            .alias(col_name)
        )
        return sf.sum(filter_column).alias(col_name)


class TreasuryAccountDownload(AwardFinancialMixin, AbstractAccountDownload):

    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.TREASURY_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.AWARD_FINANCIAL

    def _build_dataframe(self) -> DataFrame:
        select_cols = (
            [sf.col("treasury_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["treasury_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + ["last_modified_date"]
        )
        return self.download_table.filter(self.dynamic_filters & self.non_zero_filters).select(select_cols)


class AwardFinancialDownloadFactory(AbstractAccountDownloadFactory):

    def create_federal_account_download(self) -> FederalAccountDownload:
        return FederalAccountDownload(self.spark, self.filters, self.dynamic_filters)

    def create_treasury_account_download(self) -> TreasuryAccountDownload:
        return TreasuryAccountDownload(self.spark, self.filters, self.dynamic_filters)
