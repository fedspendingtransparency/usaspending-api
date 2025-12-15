from datetime import datetime


from pyspark.sql import functions as sf, Column, DataFrame, SparkSession
from usaspending_api.config import CONFIG

from usaspending_api.common.spark.utils import collect_concat, filter_submission_and_sum
from usaspending_api.download.delta_downloads.abstract_downloads.account_download import (
    AbstractAccountDownload,
    AccountLevel,
    SubmissionType,
)
from usaspending_api.download.delta_downloads.abstract_factories.account_download_factory import (
    AbstractAccountDownloadFactory,
)
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.download.download_utils import construct_data_date_range, obtain_filename_prefix_from_agency_id
from usaspending_api.download.v2.download_column_historical_lookups import query_paths


class AwardFinancialMixin:
    """Shared code between concrete implementations of the AbstractAccountDownload"""

    spark: SparkSession

    filters: AccountDownloadFilters
    dynamic_filters: Column

    @property
    def download_table(self) -> DataFrame:
        # TODO: This should be reverted back after Spark downloads are migrated to EMR
        # return self.spark.table("rpt.award_financial_download")
        return self.spark.read.format("delta").load(
            f"s3a://{CONFIG.SPARK_S3_BUCKET}/{CONFIG.DELTA_LAKE_S3_PATH}/rpt/award_financial_download"
        )

    @property
    def non_zero_filters(self) -> Column:
        return (
            (sf.col("gross_outlay_amount_FYB_to_period_end") != 0)
            | (sf.col("USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig") != 0)
            | (sf.col("USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig") != 0)
            | (sf.col("transaction_obligated_amount") != 0)
        )

    @property
    def award_categories(self) -> dict[str, Column]:
        return {
            "Assistance": sf.col("is_fpds" == True),
            "Contracts": (sf.isnotnull(sf.col("is_fpds")) & (sf.col("is_fpds") == False)),
            "Unlinked": sf.isnull(sf.col("is_fpds")),
        }


class FederalAccountDownload(AwardFinancialMixin, AbstractAccountDownload):

    def _build_file_names(self) -> list[str]:
        date_range = construct_data_date_range(self.filters.dict())
        agency = obtain_filename_prefix_from_agency_id(self.filters.agency)
        level = self.account_level.abbreviation
        title = self.submission_type.title
        timestamp = datetime.strftime(self.start_time, "%Y-%m-%d_H%HM%MS%S")
        return [
            f"{date_range}_{agency}_{level}_{award_category}_{title}_{timestamp}"
            for award_category in self.award_categories
        ]

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
            "reporting_agency_name": lambda col: collect_concat(col, spark=self.spark),
            "budget_function": lambda col: collect_concat(col, spark=self.spark),
            "budget_subfunction": lambda col: collect_concat(col, spark=self.spark),
            "transaction_obligated_amount": lambda col: sf.sum(col).alias(col),
            "gross_outlay_amount_FYB_to_period_end": lambda col: filter_submission_and_sum(
                col, self.filters, spark=self.spark
            ),
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": lambda col: filter_submission_and_sum(
                col, self.filters, spark=self.spark
            ),
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": lambda col: filter_submission_and_sum(
                col, self.filters, spark=self.spark
            ),
            "last_modified_date": lambda col: sf.max(sf.date_format(col, "yyyy-MM-dd")).alias(col),
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

    def _build_dataframes(self) -> list[DataFrame]:
        # TODO: Should handle the aggregate columns via a new name instead of relying on drops. If the Delta tables are
        #       referenced by their location then the ability to use the table identifier is lost as it doesn't
        #       appear to use the metastore for the Delta tables.
        combined_download = (
            self.download_table.filter(self.dynamic_filters)
            .groupBy(self.group_by_cols)
            .agg(*[agg_func(col) for col, agg_func in self.agg_cols.items()])
            # drop original agg columns from the dataframe to avoid ambiguous column names
            .drop(*[sf.col(f"award_financial_download.{col}") for col in self.agg_cols])
            .filter(self.non_zero_filters)
        )
        return [
            combined_download.filter(award_category_filter).select(self.select_cols)
            for award_category_filter in self.award_categories.values()
        ]


class TreasuryAccountDownload(AwardFinancialMixin, AbstractAccountDownload):

    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.TREASURY_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.AWARD_FINANCIAL

    def _build_dataframes(self) -> list[DataFrame]:
        select_cols = (
            [sf.col("treasury_owning_agency_name").alias("owning_agency_name")]
            + [
                col
                for col in query_paths["award_financial"]["treasury_account"].keys()
                if col != "owning_agency_name" and not col.startswith("last_modified_date")
            ]
            + [sf.date_format("last_modified_date", "yyyy-MM-dd").alias("last_modified_date")]
        )
        combined_download = self.download_table.filter(self.dynamic_filters & self.non_zero_filters)
        return [
            combined_download.filter(award_category_filter).select(select_cols)
            for award_category_filter in self.award_categories.values()
        ]


class AwardFinancialDownloadFactory(AbstractAccountDownloadFactory):

    def create_federal_account_download(self) -> FederalAccountDownload:
        return FederalAccountDownload(self.spark, self.filters, self.dynamic_filters)

    def create_treasury_account_download(self) -> TreasuryAccountDownload:
        return TreasuryAccountDownload(self.spark, self.filters, self.dynamic_filters)
