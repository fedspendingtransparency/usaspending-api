from pyspark.sql import functions as sf, Column, DataFrame, SparkSession

from usaspending_api.common.spark.utils import collect_concat
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

    spark: SparkSession

    filters: AccountDownloadFilters
    dynamic_filters: Column

    group_by_cols: list[str]
    agg_cols: list[Column]
    select_cols: list[Column]

    @property
    def download_table(self) -> DataFrame:
        return self.spark.table("rpt.account_balances_download")

    def _build_dataframes(self) -> list[DataFrame]:
        return [
            self.download_table.filter(
                sf.col("submission_id").isin(
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
    def agg_cols(self) -> list[Column]:
        return [
            collect_concat("reporting_agency_name", spark=self.spark),
            collect_concat("agency_identifier_name", spark=self.spark),
            collect_concat("budget_function", spark=self.spark),
            collect_concat("budget_subfunction", spark=self.spark),
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
                            & (sf.col("reporting_fiscal_quarter") == self.filters.reporting_fiscal_quarter)
                        )
                        | (
                            ~sf.col("quarter_format_flag")
                            & (sf.col("reporting_fiscal_period") == self.filters.reporting_fiscal_period)
                        )
                    )
                    & (sf.col("reporting_fiscal_year") == self.filters.reporting_fiscal_year),
                    sf.col("gross_outlay_amount"),
                ).otherwise(0)
            ).alias("gross_outlay_amount"),
            sf.sum(sf.col("status_of_budgetary_resources_total")).alias("status_of_budgetary_resources_total"),
            sf.max(sf.date_format("last_modified_date", "yyyy-MM-dd")).alias("last_modified_date"),
        ]

    @property
    def select_cols(self) -> list[Column]:
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


class TreasuryAccountDownload(AccountBalancesMixin, AbstractAccountDownload):

    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.TREASURY_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.ACCOUNT_BALANCES

    @property
    def group_by_cols(self) -> list[Column]:
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
    def agg_cols(self) -> list[Column]:
        return [
            sf.max(sf.date_format("last_modified_date", "yyyy-MM-dd")).alias("max_last_modified_date"),
        ]

    @property
    def select_cols(self) -> list[Column]:
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
