from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from duckdb.experimental.spark.sql.dataframe import DataFrame as DuckDBSparkDataFrame
from pyspark.sql import Column, DataFrame, SparkSession

from usaspending_api.common.spark.utils import collect_concat
from usaspending_api.config import CONFIG
from usaspending_api.download.delta_downloads.abstract_downloads.account_download import (
    AbstractAccountDownload,
    AccountLevel,
    SubmissionType,
)
from usaspending_api.download.delta_downloads.abstract_factories.account_download_factory import (
    AbstractAccountDownloadFactory,
    AccountDownloadConditionName,
)
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class AccountBalancesMixin:
    """Shared code between concrete implementations of the AbstractAccountDownload"""

    spark: SparkSession | DuckDBSparkSession

    filters: AccountDownloadFilters
    dynamic_filters: Column | DuckDBSparkColumn

    group_by_cols: list[str]
    agg_cols: list[Column | DuckDBSparkColumn]
    select_cols: list[Column | DuckDBSparkColumn]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(self.spark, DuckDBSparkSession):
            from duckdb.experimental.spark.sql import functions
        else:
            from pyspark.sql import functions

        self.sf = functions

    @property
    def download_table(self) -> DataFrame | DuckDBSparkDataFrame:
        if isinstance(self.spark, DuckDBSparkSession):
            return self.spark.table("rpt.account_balances_download")
        else:
            # TODO: This should be reverted back after Spark downloads are migrated to EMR
            return self.spark.read.format("delta").load(
                f"s3a://{CONFIG.SPARK_S3_BUCKET}/{CONFIG.DELTA_LAKE_S3_PATH}/rpt/account_balances_download"
            )


    def _build_dataframes(self) -> list[DataFrame | DuckDBSparkDataFrame]:
        return [
            self.download_table.filter(
                self.sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.filters.reporting_fiscal_year,
                        self.filters.reporting_fiscal_quarter,
                        self.filters.reporting_fiscal_period,
                    )
                )
            )
            .filter(self.dynamic_filters)
            .groupby(self.group_by_cols)
            .agg(*self.agg_cols)
            .select(*self.select_cols),
        ]


class FederalAccountDownload(AccountBalancesMixin, AbstractAccountDownload):
    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.FEDERAL_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.ACCOUNT_BALANCES

    @property
    def group_by_cols(self) -> list[str]:
        return ["federal_account_symbol", "owning_agency_name", "federal_account_name", "submission_period"]

    @property
    def agg_cols(self) -> list[Column | DuckDBSparkColumn]:
        return [
            collect_concat("reporting_agency_name", spark=self.spark),
            collect_concat("agency_identifier_name", spark=self.spark),
            collect_concat("budget_function", spark=self.spark),
            collect_concat("budget_subfunction", spark=self.spark),
            self.sf.sum(self.sf.col("budget_authority_unobligated_balance_brought_forward")).alias(
                "budget_authority_unobligated_balance_brought_forward"
            ),
            self.sf.sum(self.sf.col("adjustments_to_unobligated_balance_brought_forward_cpe")).alias(
                "adjustments_to_unobligated_balance_brought_forward_cpe"
            ),
            self.sf.sum(self.sf.col("budget_authority_appropriated_amount")).alias(
                "budget_authority_appropriated_amount"
            ),
            self.sf.sum(self.sf.col("borrowing_authority_amount")).alias("borrowing_authority_amount"),
            self.sf.sum(self.sf.col("contract_authority_amount")).alias("contract_authority_amount"),
            self.sf.sum(self.sf.col("spending_authority_from_offsetting_collections_amount")).alias(
                "spending_authority_from_offsetting_collections_amount"
            ),
            self.sf.sum(self.sf.col("total_other_budgetary_resources_amount")).alias(
                "total_other_budgetary_resources_amount"
            ),
            self.sf.sum(self.sf.col("total_budgetary_resources")).alias("total_budgetary_resources"),
            self.sf.sum(self.sf.col("obligations_incurred")).alias("obligations_incurred"),
            self.sf.sum(self.sf.col("deobligations_or_recoveries_or_refunds_from_prior_year")).alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            self.sf.sum(self.sf.col("unobligated_balance")).alias("unobligated_balance"),
            self.sf.sum(
                self.sf.when(
                    (
                        (
                            self.sf.col("quarter_format_flag")
                            & (self.sf.col("reporting_fiscal_quarter") == self.filters.reporting_fiscal_quarter)
                        )
                        | (
                            ~self.sf.col("quarter_format_flag")
                            & (self.sf.col("reporting_fiscal_period") == self.filters.reporting_fiscal_period)
                        )
                    )
                    & (self.sf.col("reporting_fiscal_year") == self.filters.reporting_fiscal_year),
                    self.sf.col("gross_outlay_amount"),
                ).otherwise(0)
            ).alias("gross_outlay_amount"),
            self.sf.sum(self.sf.col("status_of_budgetary_resources_total")).alias(
                "status_of_budgetary_resources_total"
            ),
            (
                (
                    self.sf.max(self.sf.call_function("strptime", "last_modified_date", "yyyy-MM-dd")).alias(
                        "max_last_modified_date"
                    )
                    if isinstance(self.spark, DuckDBSparkSession)
                    else self.sf.max(self.sf.date_format("last_modified_date", "yyyy-MM-dd")).alias(
                        "last_modified_date"
                    )
                ),
            ),
        ]

    @property
    def select_cols(self) -> list[Column]:
        return [
            self.sf.col("owning_agency_name"),
            self.sf.col("reporting_agency_name"),
            self.sf.col("submission_period"),
            self.sf.col("federal_account_symbol"),
            self.sf.col("federal_account_name"),
            self.sf.col("agency_identifier_name"),
            self.sf.col("budget_function"),
            self.sf.col("budget_subfunction"),
            self.sf.col("budget_authority_unobligated_balance_brought_forward"),
            self.sf.col("adjustments_to_unobligated_balance_brought_forward_cpe"),
            self.sf.col("budget_authority_appropriated_amount"),
            self.sf.col("borrowing_authority_amount"),
            self.sf.col("contract_authority_amount"),
            self.sf.col("spending_authority_from_offsetting_collections_amount"),
            self.sf.col("total_other_budgetary_resources_amount"),
            self.sf.col("total_budgetary_resources"),
            self.sf.col("obligations_incurred"),
            self.sf.col("deobligations_or_recoveries_or_refunds_from_prior_year"),
            self.sf.col("unobligated_balance"),
            self.sf.col("gross_outlay_amount"),
            self.sf.col("status_of_budgetary_resources_total"),
            self.sf.col("last_modified_date"),
        ]


class TreasuryAccountDownload(AccountBalancesMixin, AbstractAccountDownload):
    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.TREASURY_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.ACCOUNT_BALANCES

    @property
    def group_by_cols(self) -> list[Column | DuckDBSparkColumn]:
        return [
            self.sf.col("data_source"),
            self.sf.col("appropriation_account_balances_id"),
            self.sf.col("budget_authority_unobligated_balance_brought_forward"),
            self.sf.col("adjustments_to_unobligated_balance_brought_forward_cpe"),
            self.sf.col("budget_authority_appropriated_amount"),
            self.sf.col("borrowing_authority_amount"),
            self.sf.col("contract_authority_amount"),
            self.sf.col("spending_authority_from_offsetting_collections_amount"),
            self.sf.col("total_other_budgetary_resources_amount"),
            self.sf.col("total_budgetary_resources"),
            self.sf.col("gross_outlay_amount"),
            self.sf.col("deobligations_or_recoveries_or_refunds_from_prior_year"),
            self.sf.col("unobligated_balance"),
            self.sf.col("status_of_budgetary_resources_total"),
            self.sf.col("obligations_incurred"),
            self.sf.col("drv_appropriation_availability_period_start_date"),
            self.sf.col("drv_appropriation_availability_period_end_date"),
            self.sf.col("drv_appropriation_account_expired_status"),
            self.sf.col("drv_obligations_unpaid_amount"),
            self.sf.col("drv_other_obligated_amount"),
            self.sf.col("reporting_period_start"),
            self.sf.col("reporting_period_end"),
            self.sf.col("appropriation_account_last_modified"),
            self.sf.col("certified_date"),
            self.sf.col("create_date"),
            self.sf.col("update_date"),
            self.sf.col("final_of_fy"),
            self.sf.col("submission_id"),
            self.sf.col("treasury_account_identifier"),
            self.sf.col("owning_agency_name"),
            self.sf.col("reporting_agency_name"),
            self.sf.col("allocation_transfer_agency_identifier_code"),
            self.sf.col("agency_identifier_code"),
            self.sf.col("beginning_period_of_availability"),
            self.sf.col("ending_period_of_availability"),
            self.sf.col("availability_type_code"),
            self.sf.col("main_account_code"),
            self.sf.col("sub_account_code"),
            self.sf.col("treasury_account_symbol"),
            self.sf.col("treasury_account_name"),
            self.sf.col("budget_function"),
            self.sf.col("budget_subfunction"),
            self.sf.col("federal_account_symbol"),
            self.sf.col("federal_account_name"),
            self.sf.col("agency_identifier_name"),
            self.sf.col("allocation_transfer_agency_identifier_name"),
            self.sf.col("submission_period"),
        ]

    @property
    def agg_cols(self) -> list[Column | DuckDBSparkColumn]:
        if isinstance(self.spark, DuckDBSparkSession):
            # DuckDB's Spark implementation doesn't include the `date_format()` function so we have to use Python's `strptime`
            return [
                self.sf.max(self.sf.call_function("strptime", "last_modified_date", "yyyy-MM-dd")).alias(
                    "max_last_modified_date"
                )
            ]
        else:
            return [
                self.sf.max(self.sf.date_format("last_modified_date", "yyyy-MM-dd")).alias("max_last_modified_date"),
            ]

    @property
    def select_cols(self) -> list[Column | DuckDBSparkColumn]:
        return [
            self.sf.col("owning_agency_name"),
            self.sf.col("reporting_agency_name"),
            self.sf.col("submission_period"),
            self.sf.col("allocation_transfer_agency_identifier_code"),
            self.sf.col("agency_identifier_code"),
            self.sf.col("beginning_period_of_availability"),
            self.sf.col("ending_period_of_availability"),
            self.sf.col("availability_type_code"),
            self.sf.col("main_account_code"),
            self.sf.col("sub_account_code"),
            self.sf.col("treasury_account_symbol"),
            self.sf.col("treasury_account_name"),
            self.sf.col("agency_identifier_name"),
            self.sf.col("allocation_transfer_agency_identifier_name"),
            self.sf.col("budget_function"),
            self.sf.col("budget_subfunction"),
            self.sf.col("federal_account_symbol"),
            self.sf.col("federal_account_name"),
            self.sf.col("budget_authority_unobligated_balance_brought_forward"),
            self.sf.col("adjustments_to_unobligated_balance_brought_forward_cpe"),
            self.sf.col("budget_authority_appropriated_amount"),
            self.sf.col("borrowing_authority_amount"),
            self.sf.col("contract_authority_amount"),
            self.sf.col("spending_authority_from_offsetting_collections_amount"),
            self.sf.col("total_other_budgetary_resources_amount"),
            self.sf.col("total_budgetary_resources"),
            self.sf.col("obligations_incurred"),
            self.sf.col("deobligations_or_recoveries_or_refunds_from_prior_year"),
            self.sf.col("unobligated_balance"),
            self.sf.col("gross_outlay_amount"),
            self.sf.col("status_of_budgetary_resources_total"),
            self.sf.col("max_last_modified_date").alias("last_modified_date"),
        ]


class AccountBalancesDownloadFactory(AbstractAccountDownloadFactory):
    @property
    def supported_filter_conditions(self) -> list[AccountDownloadConditionName]:
        return [
            condition_name
            for condition_name in AccountDownloadConditionName
            if condition_name != AccountDownloadConditionName.DEF_CODES
        ]

    def create_federal_account_download(self) -> FederalAccountDownload:
        return FederalAccountDownload(self.spark, self.filters, self.dynamic_filters)

    def create_treasury_account_download(self) -> TreasuryAccountDownload:
        return TreasuryAccountDownload(self.spark, self.filters, self.dynamic_filters)
