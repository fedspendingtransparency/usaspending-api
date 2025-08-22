from typing import TYPE_CHECKING

from pyspark.sql import functions as sf

from usaspending_api.common.spark.utils import collect_concat
from usaspending_api.download.delta_downloads.abstract_factories.account_download import (
    AbstractAccountDownload,
    AccountDownloadLevel,
    AccountDownloadType,
)
from usaspending_api.download.delta_downloads.abstract_factories.account_download_factory import (
    AbstractAccountDownloadFactory,
    AccountDownloadConditionName,
)
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


if TYPE_CHECKING:
    from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
    from pyspark.sql import Column, DataFrame, SparkSession


class AccountBalancesMixin:
    """Shared code between concrete implementations of the AbstractAccountDownload"""

    spark: SparkSession

    filters: AccountDownloadFilters
    dynamic_filters: Column

    group_by_cols: list[str]
    agg_cols: list[Column]
    select_cols: list[Column]

    @property
    def aab(self) -> DataFrame:
        return self.spark.table("global_temp.appropriation_account_balances")

    @property
    def sa(self) -> DataFrame:
        return self.spark.table("global_temp.submission_attributes")

    @property
    def taa(self) -> DataFrame:
        return self.spark.table("global_temp.treasury_appropriation_account")

    @property
    def cgac_aid(self) -> DataFrame:
        return self.spark.table("global_temp.cgac")

    @property
    def cgac_ata(self) -> DataFrame:
        return self.spark.table("global_temp.cgac")

    @property
    def fa(self) -> DataFrame:
        return self.spark.table("global_temp.federal_account")

    @property
    def ta(self) -> DataFrame:
        return self.spark.table("global_temp.toptier_agency")

    @property
    def download_table(self) -> DataFrame:
        return (
            self.aab.join(self.sa, on="submission_id", how="inner")
            .join(self.taa, on="treasury_account_identifier", how="leftouter")
            .join(
                self.cgac_aid.withColumnRenamed("agency_name", "agency_identifier_name"),
                on=(self.taa.agency_id == self.cgac_aid.cgac_code),
                how="leftouter",
            )
            .join(
                self.cgac_ata.withColumnRenamed("agency_name", "allocation_transfer_agency_identifier_name"),
                on=(self.taa.allocation_transfer_agency_id == self.cgac_ata.cgac_code),
                how="leftouter",
            )
            .join(self.fa, on=self.taa.federal_account_id == self.fa.id, how="leftouter")
            .join(self.ta, on=self.fa.parent_toptier_agency_id == self.ta.toptier_agency_id, how="leftouter")
            .filter(
                sf.col("submission_id").isin(
                    get_submission_ids_for_periods(
                        self.filters.reporting_fiscal_year,
                        self.filters.reporting_fiscal_quarter,
                        self.filters.reporting_fiscal_period,
                    )
                )
            )
            .filter(self.dynamic_filters)
            .withColumn("submission_period", self.fy_quarter_period)
        )

    @property
    def dataframe(self) -> DataFrame:
        return self.download_table.groupby(self.group_by_cols).agg(*self.agg_cols).select(*self.select_cols)

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


class FederalAccountDownload(AccountBalancesMixin, AbstractAccountDownload):

    @property
    def download_level(self) -> AccountDownloadLevel:
        return AccountDownloadLevel.FEDERAL_ACCOUNT

    @property
    def download_type(self) -> AccountDownloadType:
        return AccountDownloadType.ACCOUNT_BALANCES

    @property
    def group_by_cols(self) -> list[str]:
        return ["federal_account_code", "name", "federal_account.account_title", "submission_period"]

    @property
    def agg_cols(self) -> list[Column]:
        return [
            collect_concat(self.sa.reporting_agency_name, alias="reporting_agency_name"),
            collect_concat("agency_identifier_name"),
            collect_concat("budget_function_title", alias="budget_function"),
            collect_concat("budget_subfunction_title", alias="budget_subfunction"),
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
                            & (sf.col("reporting_fiscal_quarter") == self.filters.reporting_fiscal_quarter)
                        )
                        | (
                            ~sf.col("quarter_format_flag")
                            & (sf.col("reporting_fiscal_period") == self.filters.reporting_fiscal_period)
                        )
                    )
                    & (sf.col("reporting_fiscal_year") == self.filters.reporting_fiscal_year),
                    sf.col("gross_outlay_amount_by_tas_cpe"),
                ).otherwise(0)
            ).alias("gross_outlay_amount"),
            sf.sum(sf.col("status_of_budgetary_resources_total_cpe")).alias("status_of_budgetary_resources_total"),
            sf.max("published_date").alias("last_modified_date"),
        ]

    @property
    def select_cols(self) -> list[Column]:
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


class TreasuryAccountDownload(AccountBalancesMixin, AbstractAccountDownload):

    @property
    def download_level(self) -> AccountDownloadLevel:
        return AccountDownloadLevel.TREASURY_ACCOUNT

    @property
    def download_type(self) -> AccountDownloadType:
        return AccountDownloadType.ACCOUNT_BALANCES

    @property
    def group_by_cols(self) -> list[Column]:
        return [
            self.aab.data_source,
            self.aab.appropriation_account_balances_id,
            self.aab.budget_authority_unobligated_balance_brought_forward_fyb,
            self.aab.adjustments_to_unobligated_balance_brought_forward_cpe,
            self.aab.budget_authority_appropriated_amount_cpe,
            self.aab.borrowing_authority_amount_total_cpe,
            self.aab.contract_authority_amount_total_cpe,
            self.aab.spending_authority_from_offsetting_collections_amount_cpe,
            self.aab.other_budgetary_resources_amount_cpe,
            self.aab.total_budgetary_resources_amount_cpe,
            self.aab.gross_outlay_amount_by_tas_cpe,
            self.aab.deobligations_recoveries_refunds_by_tas_cpe,
            self.aab.unobligated_balance_cpe,
            self.aab.status_of_budgetary_resources_total_cpe,
            self.aab.obligations_incurred_total_by_tas_cpe,
            self.aab.drv_appropriation_availability_period_start_date,
            self.aab.drv_appropriation_availability_period_end_date,
            self.aab.drv_appropriation_account_expired_status,
            self.aab.drv_obligations_unpaid_amount,
            self.aab.drv_other_obligated_amount,
            self.aab.reporting_period_start,
            self.aab.reporting_period_end,
            self.aab.last_modified_date,
            self.aab.certified_date,
            self.aab.create_date,
            self.aab.update_date,
            self.aab.final_of_fy,
            self.aab.submission_id,
            self.aab.treasury_account_identifier,
            self.ta.name,
            self.sa.reporting_agency_name,
            self.taa.allocation_transfer_agency_id,
            self.taa.agency_id,
            self.taa.beginning_period_of_availability,
            self.taa.ending_period_of_availability,
            self.taa.availability_type_code,
            self.taa.main_account_code,
            self.taa.sub_account_code,
            self.taa.tas_rendering_label,
            self.taa.account_title,
            self.taa.budget_function_title,
            self.taa.budget_subfunction_title,
            self.fa.federal_account_code,
            self.fa.account_title,
            sf.col("agency_identifier_name"),
            sf.col("allocation_transfer_agency_identifier_name"),
            sf.col("submission_period"),
        ]

    @property
    def agg_cols(self) -> list[Column]:
        return [sf.max("published_date").alias("max_last_modified_date")]

    @property
    def select_cols(self) -> list[Column]:
        return [
            self.ta.name.alias("owning_agency_name"),
            self.sa.reporting_agency_name,
            sf.col("submission_period"),
            self.taa.allocation_transfer_agency_id.alias("allocation_transfer_agency_identifier_code"),
            self.taa.agency_id.alias("agency_identifier_code"),
            self.taa.beginning_period_of_availability,
            self.taa.ending_period_of_availability,
            self.taa.availability_type_code,
            self.taa.main_account_code,
            self.taa.sub_account_code,
            self.taa.tas_rendering_label.alias("treasury_account_symbol"),
            self.taa.account_title.alias("treasury_account_name"),
            sf.col("agency_identifier_name"),
            sf.col("allocation_transfer_agency_identifier_name"),
            self.taa.budget_function_title.alias("budget_function"),
            self.taa.budget_subfunction_title.alias("budget_subfunction"),
            self.fa.federal_account_code.alias("federal_account_symbol"),
            self.fa.account_title.alias("federal_account_name"),
            self.aab.budget_authority_unobligated_balance_brought_forward_fyb.alias(
                "budget_authority_unobligated_balance_brought_forward"
            ),
            self.aab.adjustments_to_unobligated_balance_brought_forward_cpe,
            self.aab.budget_authority_appropriated_amount_cpe.alias("budget_authority_appropriated_amount"),
            self.aab.borrowing_authority_amount_total_cpe.alias("borrowing_authority_amount"),
            self.aab.contract_authority_amount_total_cpe.alias("contract_authority_amount"),
            self.aab.spending_authority_from_offsetting_collections_amount_cpe.alias(
                "spending_authority_from_offsetting_collections_amount"
            ),
            self.aab.other_budgetary_resources_amount_cpe.alias("total_other_budgetary_resources_amount"),
            self.aab.total_budgetary_resources_amount_cpe.alias("total_budgetary_resources"),
            self.aab.obligations_incurred_total_by_tas_cpe.alias("obligations_incurred"),
            self.aab.deobligations_recoveries_refunds_by_tas_cpe.alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            self.aab.unobligated_balance_cpe.alias("unobligated_balance"),
            self.aab.gross_outlay_amount_by_tas_cpe.alias("gross_outlay_amount"),
            self.aab.status_of_budgetary_resources_total_cpe.alias("status_of_budgetary_resources_total"),
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
