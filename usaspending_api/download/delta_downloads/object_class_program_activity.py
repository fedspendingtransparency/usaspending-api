from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from duckdb.experimental.spark.sql.dataframe import DataFrame as DuckDBSparkDataFrame
from pyspark.sql import Column, DataFrame, SparkSession

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
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class ObjectClassProgramActivityMixin:
    """Shared code between concrete implementations of the ObjectClassProgramActivity"""

    spark: SparkSession | DuckDBSparkSession

    filters: AccountDownloadFilters
    dynamic_filters: Column | DuckDBSparkColumn

    group_by_cols: list[str]
    agg_cols: dict[str, callable]
    select_cols: list[Column | DuckDBSparkColumn]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if type(self.spark) is DuckDBSparkSession:
            from duckdb.experimental.spark.sql import functions
        else:
            from pyspark.sql import functions

        self.sf = functions

    @property
    def download_table(self) -> DataFrame | DuckDBSparkDataFrame:
        return self.spark.table("rpt.object_class_program_activity_download")

    def _build_dataframe(self) -> DataFrame | DuckDBSparkDataFrame:
        return (
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
            .agg(*[agg_func(col) for col, agg_func in self.agg_cols.items()])
            .drop(*[self.sf.col(f"object_class_program_activity_download.{col}") for col in self.agg_cols])
            .select(*self.select_cols)
            # Sorting by a value that is repeated often will help improve compression during the zipping step
            .sort(self.sort_by_cols)
        )


class FederalAccountDownload(ObjectClassProgramActivityMixin, AbstractAccountDownload):
    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.FEDERAL_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.OBJECT_CLASS_PROGRAM_ACTIVITY

    @property
    def group_by_cols(self) -> list[str]:
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
    def agg_cols(self) -> dict[str, callable]:
        return {
            "reporting_agency_name": lambda col: collect_concat(col, spark=self.spark),
            "budget_function": lambda col: collect_concat(col, spark=self.spark),
            "budget_subfunction": lambda col: collect_concat(col, spark=self.spark),
            "obligations_incurred": lambda col: self.sf.sum(col).alias(col),
            "obligations_undelivered_orders_unpaid_total": lambda col: self.sf.sum(col).alias(col),
            "obligations_undelivered_orders_unpaid_total_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL480100_undelivered_orders_obligations_unpaid": lambda col: self.sf.sum(col).alias(col),
            "USSGL480100_undelivered_orders_obligations_unpaid_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid": lambda col: self.sf.sum(col).alias(col),
            "obligations_delivered_orders_unpaid_total": lambda col: self.sf.sum(col).alias(col),
            "obligations_delivered_orders_unpaid_total_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL490100_delivered_orders_obligations_unpaid": lambda col: self.sf.sum(col).alias(col),
            "USSGL490100_delivered_orders_obligations_unpaid_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid": lambda col: self.sf.sum(col).alias(col),
            "gross_outlay_amount_FYB_to_period_end": lambda col: filter_submission_and_sum(
                col, self.filters, spark=self.spark
            ),
            "gross_outlay_amount_FYB": lambda col: self.sf.sum(col).alias(col),
            "gross_outlays_undelivered_orders_prepaid_total": lambda col: self.sf.sum(col).alias(col),
            "gross_outlays_undelivered_orders_prepaid_total_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL480200_undelivered_orders_obligations_prepaid_advanced": lambda col: self.sf.sum(col).alias(col),
            "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid": lambda col: self.sf.sum(col).alias(col),
            "gross_outlays_delivered_orders_paid_total": lambda col: self.sf.sum(col).alias(col),
            "gross_outlays_delivered_orders_paid_total_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL490200_delivered_orders_obligations_paid": lambda col: self.sf.sum(col).alias(col),
            "USSGL490800_authority_outlayed_not_yet_disbursed": lambda col: self.sf.sum(col).alias(col),
            "USSGL490800_authority_outlayed_not_yet_disbursed_FYB": lambda col: self.sf.sum(col).alias(col),
            "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid": lambda col: self.sf.sum(col).alias(col),
            "deobligations_or_recoveries_or_refunds_from_prior_year": lambda col: self.sf.sum(col).alias(col),
            "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig": lambda col: self.sf.sum(col).alias(col),
            "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig": lambda col: self.sf.sum(col).alias(col),
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": lambda col: filter_submission_and_sum(
                col, self.filters, spark=self.spark
            ),
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": lambda col: filter_submission_and_sum(
                col, self.filters, spark=self.spark
            ),
            "USSGL483100_undelivered_orders_obligations_transferred_unpaid": lambda col: self.sf.sum(col).alias(col),
            "USSGL493100_delivered_orders_obligations_transferred_unpaid": lambda col: self.sf.sum(col).alias(col),
            "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced": lambda col: self.sf.sum(col).alias(col),
            "last_modified_date": lambda col: self.sf.max(col).alias("max_last_modified_date"),
        }

    @property
    def select_cols(self) -> list[Column | DuckDBSparkColumn]:
        return [
            self.sf.col(col)
            for col in query_paths["object_class_program_activity"]["federal_account"].keys()
            if not col.startswith("last_modified_date")
        ] + [self.sf.col("max_last_modified_date").alias("last_modified_date")]

    @property
    def sort_by_cols(self) -> list[str]:
        # Sorting by a value that is repeated often will help improve compression during the zipping step
        return [
            "owning_agency_name",
            "reporting_agency_name",
        ]


class TreasuryAccountDownload(ObjectClassProgramActivityMixin, AbstractAccountDownload):
    @property
    def account_level(self) -> AccountLevel:
        return AccountLevel.TREASURY_ACCOUNT

    @property
    def submission_type(self) -> SubmissionType:
        return SubmissionType.OBJECT_CLASS_PROGRAM_ACTIVITY

    @property
    def group_by_cols(self) -> list[str]:
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
            "budget_subfunction",
            "object_class_name",
            "program_activity_code",
            "disaster_emergency_fund_name",
            "submission_period",
            "treasury_account_name",
            "main_account_code",
            "federal_account_symbol",
            "budget_function",
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
    def agg_cols(self) -> dict[str, callable]:
        return {
            "last_modified_date": lambda col: self.sf.max(col).alias(f"max_{col}"),
        }

    @property
    def sort_by_cols(self) -> list[str]:
        # Sorting by a value that is repeated often will help improve compression during the zipping step
        return [
            "owning_agency_name",
            "reporting_agency_name",
        ]

    @property
    def select_cols(self) -> list[Column | DuckDBSparkColumn]:
        return [
            self.sf.col(col)
            for col in query_paths["object_class_program_activity"]["treasury_account"].keys()
            if not col.startswith("last_modified_date")
        ] + [self.sf.col("max_last_modified_date").alias("last_modified_date")]


class ObjectClassProgramActivityDownloadFactory(AbstractAccountDownloadFactory):
    def create_federal_account_download(self) -> FederalAccountDownload:
        return FederalAccountDownload(self.spark, self.filters, self.dynamic_filters)

    def create_treasury_account_download(self) -> TreasuryAccountDownload:
        return TreasuryAccountDownload(self.spark, self.filters, self.dynamic_filters)
