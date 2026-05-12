from duckdb.experimental.spark.sql import SparkSession as DuckDBSparkSession
from duckdb.experimental.spark.sql.column import Column as DuckDBSparkColumn
from duckdb.experimental.spark.sql.dataframe import DataFrame as DuckDBSparkDataFrame
from pyspark.sql import Column, DataFrame, SparkSession

from usaspending_api.download.delta_downloads.abstract_downloads.monthly_download import (
    AbstractMonthlyDownload,
)
from usaspending_api.download.delta_downloads.abstract_factories.monthly_download_factory import (
    AbstractMonthlyDownloadFactory,
    MonthlyDownload,
)
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import (
    MonthlyDownloadFilters,
)
from usaspending_api.download.delta_downloads.helpers.enums import AwardCategory, MonthlyType


class AssistanceMixin:
    spark: SparkSession

    download_table: DataFrame
    dynamic_filters: Column | DuckDBSparkColumn
    filters: MonthlyDownloadFilters

    select_cols: list[Column | DuckDBSparkColumn]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(self.spark, DuckDBSparkSession):
            from duckdb.experimental.spark.sql import functions
        else:
            from pyspark.sql import functions

        self.sf = functions

    @property
    def category(self) -> AwardCategory:
        return AwardCategory.ASSISTANCE

    @property
    def select_cols(self) -> list[Column | DuckDBSparkColumn]:
        return [
            self.sf.col("transaction_unique_key").alias("assistance_transaction_unique_key"),
            self.sf.col("generated_unique_award_id").alias("assistance_award_unique_key"),
            self.sf.col("fain").alias("award_id_fain"),
            self.sf.col("modification_number"),
            self.sf.col("uri").alias("award_id_uri"),
            self.sf.col("sai_number"),
            self.sf.col("federal_action_obligation"),
            self.sf.col("total_obligation").alias("total_obligated_amount"),
            self.sf.col("total_outlayed_amount_for_overall_award"),
            self.sf.col("indirect_federal_sharing").alias("indirect_cost_federal_share_amount"),
            self.sf.col("non_federal_funding_amount"),
            self.sf.col("total_non_federal_funding_amount"),
            self.sf.col("face_value_loan_guarantee").alias("face_value_of_loan"),
            self.sf.col("original_loan_subsidy_cost"),
            self.sf.col("total_loan_value").alias("total_face_value_of_loan"),
            self.sf.col("total_subsidy_cost").alias("total_loan_subsidy_cost"),
            self.sf.col("generated_pragmatic_obligation").alias("generated_pragmatic_obligations"),
            self.sf.col("defc_for_overall_award").alias("disaster_emergency_fund_codes_for_overall_award"),
            self.sf.col("covid_19_outlayed_amount").alias(
                "outlayed_amount_from_COVID-19_supplementals_for_overall_award"
            ),
            self.sf.col("covid_19_obligated_amount").alias(
                "obligated_amount_from_COVID-19_supplementals_for_overall_award"
            ),
            self.sf.col("iija_outlayed_amount").alias("outlayed_amount_from_IIJA_supplemental_for_overall_award"),
            self.sf.col("iija_obligated_amount").alias("obligated_amount_from_IIJA_supplemental_for_overall_award"),
            self.sf.col("action_date"),
            self.sf.col("action_date_fiscal_year"),
            self.sf.col("period_of_performance_start_date"),
            self.sf.col("period_of_performance_current_end_date"),
            self.sf.col("awarding_agency_code"),
            self.sf.col("awarding_toptier_agency_name_raw").alias("awarding_agency_name"),
            self.sf.col("awarding_sub_tier_agency_c").alias("awarding_sub_agency_code"),
            self.sf.col("awarding_subtier_agency_name_raw").alias("awarding_sub_agency_name"),
            self.sf.col("awarding_office_code"),
            self.sf.col("awarding_office_name"),
            self.sf.col("funding_agency_code"),
            self.sf.col("funding_toptier_agency_name_raw").alias("funding_agency_name"),
            self.sf.col("funding_sub_tier_agency_co").alias("funding_sub_agency_code"),
            self.sf.col("funding_subtier_agency_name_raw").alias("funding_sub_agency_name"),
            self.sf.col("funding_office_code"),
            self.sf.col("funding_office_name"),
            self.sf.col("treasury_accounts_funding_this_award"),
            self.sf.col("federal_accounts_funding_this_award"),
            self.sf.col("object_classes_funding_this_award"),
            self.sf.col("program_activities_funding_this_award"),
            self.sf.col("recipient_uei"),
            self.sf.col("recipient_unique_id").alias("recipient_duns"),
            self.sf.col("recipient_name"),
            self.sf.col("recipient_name_raw"),
            self.sf.col("parent_uei").alias("recipient_parent_uei"),
            self.sf.col("parent_recipient_unique_id").alias("recipient_parent_duns"),
            self.sf.col("parent_recipient_name").alias("recipient_parent_name"),
            self.sf.col("parent_recipient_name_raw").alias("recipient_parent_name_raw"),
            self.sf.col("recipient_location_country_code").alias("recipient_country_code"),
            self.sf.col("recipient_location_country_name").alias("recipient_country_name"),
            self.sf.col("legal_entity_address_line1").alias("recipient_address_line_1"),
            self.sf.col("legal_entity_address_line2").alias("recipient_address_line_2"),
            self.sf.col("legal_entity_city_code").alias("recipient_city_code"),
            self.sf.col("recipient_location_city_name").alias("recipient_city_name"),
            self.sf.col("recipient_location_county_fips").alias("prime_award_transaction_recipient_county_fips_code"),
            self.sf.col("recipient_location_county_name").alias("recipient_county_name"),
            self.sf.col("recipient_location_state_fips").alias("prime_award_transaction_recipient_state_fips_code"),
            self.sf.col("recipient_location_state_code").alias("recipient_state_code"),
            self.sf.col("recipient_location_state_name").alias("recipient_state_name"),
            self.sf.col("recipient_location_zip5").alias("recipient_zip_code"),
            self.sf.col("legal_entity_zip_last4").alias("recipient_zip_last_4_code"),
            self.sf.col("recipient_location_cd_original").alias("prime_award_transaction_recipient_cd_original"),
            self.sf.col("recipient_location_cd_current").alias("prime_award_transaction_recipient_cd_current"),
            self.sf.col("legal_entity_foreign_city").alias("recipient_foreign_city_name"),
            self.sf.col("legal_entity_foreign_provi").alias("recipient_foreign_province_name"),
            self.sf.col("legal_entity_foreign_posta").alias("recipient_foreign_postal_code"),
            self.sf.col("place_of_performance_scope").alias("primary_place_of_performance_scope"),
            self.sf.col("pop_country_code").alias("primary_place_of_performance_country_code"),
            self.sf.col("pop_country_name").alias("primary_place_of_performance_country_name"),
            self.sf.col("place_of_performance_code").alias("primary_place_of_performance_code"),
            self.sf.col("pop_city_name").alias("primary_place_of_performance_city_name"),
            self.sf.col("pop_county_fips").alias("prime_award_transaction_place_of_performance_county_fips_code"),
            self.sf.col("pop_county_name").alias("primary_place_of_performance_county_name"),
            self.sf.col("pop_state_fips").alias("prime_award_transaction_place_of_performance_state_fips_code"),
            self.sf.col("pop_state_name").alias("primary_place_of_performance_state_name"),
            self.sf.col("place_of_performance_zip4a").alias("primary_place_of_performance_zip_4"),
            self.sf.col("pop_cd_original").alias("prime_award_transaction_place_of_performance_cd_original"),
            self.sf.col("pop_cd_current").alias("prime_award_transaction_place_of_performance_cd_current"),
            self.sf.col("place_of_performance_forei").alias("primary_place_of_performance_foreign_location"),
            self.sf.col("cfda_number"),
            self.sf.col("cfda_title"),
            self.sf.col("funding_opportunity_number"),
            self.sf.col("funding_opportunity_goals").alias("funding_opportunity_goals_text"),
            self.sf.col("type").alias("assistance_type_code"),
            self.sf.col("type_description").alias("assistance_type_description"),
            self.sf.col("transaction_description"),
            self.sf.col("award_description").alias("prime_award_base_transaction_description"),
            self.sf.col("business_funds_indicator").alias("business_funds_indicator_code"),
            self.sf.col("business_funds_ind_desc").alias("business_funds_indicator_description"),
            self.sf.col("business_types").alias("business_types_code"),
            self.sf.col("business_types_desc").alias("business_types_description"),
            self.sf.col("correction_delete_indicatr").alias("correction_delete_indicator_code"),
            self.sf.col("correction_delete_ind_desc").alias("correction_delete_indicator_description"),
            self.sf.col("action_type").alias("action_type_code"),
            self.sf.col("action_type_description").alias("action_type_description"),
            self.sf.col("record_type").alias("record_type_code"),
            self.sf.col("record_type_description").alias("record_type_description"),
            self.sf.col("officer_1_name").alias("highly_compensated_officer_1_name"),
            self.sf.col("officer_1_amount").alias("highly_compensated_officer_1_amount"),
            self.sf.col("officer_2_name").alias("highly_compensated_officer_2_name"),
            self.sf.col("officer_2_amount").alias("highly_compensated_officer_2_amount"),
            self.sf.col("officer_3_name").alias("highly_compensated_officer_3_name"),
            self.sf.col("officer_3_amount").alias("highly_compensated_officer_3_amount"),
            self.sf.col("officer_4_name").alias("highly_compensated_officer_4_name"),
            self.sf.col("officer_4_amount").alias("highly_compensated_officer_4_amount"),
            self.sf.col("officer_5_name").alias("highly_compensated_officer_5_name"),
            self.sf.col("officer_5_amount").alias("highly_compensated_officer_5_amount"),
            self.sf.col("usaspending_permalink"),
            self.sf.col("initial_report_date"),
            self.sf.col("last_modified_date"),
        ]

    def _build_dataframes(self) -> list[DataFrame | DuckDBSparkDataFrame]:
        return [self.download_table.filter(self.dynamic_filters).select(*self.select_cols)]


class AssistanceDeltaMonthlyDownload(AssistanceMixin, AbstractMonthlyDownload):
    @property
    def monthly_type(self) -> MonthlyType:
        return MonthlyType.DELTA


class AssistanceFullMonthlyDownload(AssistanceMixin, AbstractMonthlyDownload):
    @property
    def monthly_type(self) -> MonthlyType:
        return MonthlyType.FULL


class TransactionAssistanceMonthlyDownloadFactory(AbstractMonthlyDownloadFactory):
    def _create_delta_download(self) -> MonthlyDownload:
        return AssistanceDeltaMonthlyDownload(self.spark, self.filters, self.dynamic_filters)

    def _create_full_download(self) -> MonthlyDownload:
        return AssistanceFullMonthlyDownload(self.spark, self.filters, self.dynamic_filters)
