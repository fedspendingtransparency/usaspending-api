from dataclasses import dataclass
from functools import reduce
from typing import Any, Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf, Column

from usaspending_api.submissions.helpers import get_submission_ids_for_periods


class AccountDownloadDataFrameBuilder:

    def __init__(
        self,
        spark: SparkSession,
        year: int,
        period: int,
        period_type: Literal["month", "quarter"] = "month",
        agency: int | None = None,
        federal_account_id: int | None = None,
        def_codes: list[str] | None = None,
    ):
        self.reporting_fiscal_year = year
        self.reporting_fiscal_quarter = period if period_type == "quarter" else period // 3
        self.reporting_fiscal_period = period if period_type == "month" else period * 3
        self.agency = agency
        self.federal_account_id = federal_account_id
        self.def_codes = def_codes
        self.df = spark.table("rpt.account_download")
        self.groupby_cols = [
            "owning_agency_name",
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
        self.select_cols = [
            "owning_agency_name",
            "reporting_agency_name",
            "submission_period",
            "federal_account_symbol",
            "federal_account_name",
            "agency_identifier_name",
            "budget_function",
            "budget_subfunction",
            "program_activity_code",
            "program_activity_name",
            "object_class_code",
            "object_class_name",
            "direct_or_reimbursable_funding_source",
            "disaster_emergency_fund_code",
            "disaster_emergency_fund_name",
            "transaction_obligated_amount",
            "gross_outlay_amount_FYB_to_period_end",
            "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
            "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
            "award_unique_key",
            "award_id_piid",
            "parent_award_id_piid",
            "award_id_fain",
            "award_id_uri",
            "award_base_action_date",
            "award_base_action_date_fiscal_year",
            "award_latest_action_date",
            "award_latest_action_date_fiscal_year",
            "period_of_performance_start_date",
            "period_of_performance_current_end_date",
            "ordering_period_end_date",
            "award_type_code",
            "award_type",
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
            "prime_award_summary_recipient_cd_original",
            "prime_award_summary_recipient_cd_current",
            "recipient_zip_code",
            "primary_place_of_performance_country",
            "primary_place_of_performance_state",
            "primary_place_of_performance_county",
            "prime_award_summary_place_of_performance_cd_original",
            "prime_award_summary_place_of_performance_cd_current",
            "primary_place_of_performance_zip_code",
            "cfda_number",
            "cfda_title",
            "product_or_service_code",
            "product_or_service_code_description",
            "naics_code",
            "naics_description",
            "national_interest_action_code",
            "national_interest_action",
            "usaspending_permalink",
            "last_modified_date",
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
