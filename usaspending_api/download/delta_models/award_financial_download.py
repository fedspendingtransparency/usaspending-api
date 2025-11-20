from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as sf
from pyspark.sql.functions import add_months
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    LongType,
)
from usaspending_api.download.helpers.delta_models_helpers import fy_quarter_period


award_financial_schema = StructType(
    [
        StructField("financial_accounts_by_awards_id", IntegerType(), False),
        StructField("submission_id", IntegerType(), False),
        StructField("federal_owning_agency_name", StringType()),
        StructField("treasury_owning_agency_name", StringType()),
        StructField("federal_account_symbol", StringType()),
        StructField("federal_account_name", StringType()),
        StructField("agency_identifier_name", StringType()),
        StructField("allocation_transfer_agency_identifier_name", StringType()),
        StructField("program_activity_code", StringType()),
        StructField("program_activity_name", StringType()),
        StructField("object_class_code", StringType()),
        StructField("object_class_name", StringType()),
        StructField("direct_or_reimbursable_funding_source", StringType()),
        StructField("disaster_emergency_fund_code", StringType()),
        StructField("disaster_emergency_fund_name", StringType()),
        StructField("award_unique_key", StringType()),
        StructField("award_id_piid", StringType()),
        StructField("parent_award_id_piid", StringType()),
        StructField("award_id_fain", StringType()),
        StructField("award_id_uri", StringType()),
        StructField("award_base_action_date", DateType()),
        StructField("award_latest_action_date", DateType()),
        StructField("period_of_performance_start_date", DateType()),
        StructField("period_of_performance_current_end_date", DateType()),
        StructField("ordering_period_end_date", DateType()),
        StructField("idv_type_code", StringType()),
        StructField("idv_type", StringType()),
        StructField("prime_award_base_transaction_description", StringType()),
        StructField("awarding_agency_code", StringType()),
        StructField("awarding_agency_name", StringType()),
        StructField("awarding_subagency_code", StringType()),
        StructField("awarding_subagency_name", StringType()),
        StructField("awarding_office_code", StringType()),
        StructField("awarding_office_name", StringType()),
        StructField("funding_agency_code", StringType()),
        StructField("funding_agency_name", StringType()),
        StructField("funding_sub_agency_code", StringType()),
        StructField("funding_sub_agency_name", StringType()),
        StructField("funding_office_code", StringType()),
        StructField("funding_office_name", StringType()),
        StructField("recipient_uei", StringType()),
        StructField("recipient_duns", StringType()),
        StructField("recipient_name", StringType()),
        StructField("recipient_name_raw", StringType()),
        StructField("recipient_parent_uei", StringType()),
        StructField("recipient_parent_duns", StringType()),
        StructField("recipient_parent_name", StringType()),
        StructField("recipient_parent_name_raw", StringType()),
        StructField("recipient_country", StringType()),
        StructField("recipient_state", StringType()),
        StructField("recipient_county", StringType()),
        StructField("recipient_city", StringType()),
        StructField("primary_place_of_performance_country", StringType()),
        StructField("primary_place_of_performance_state", StringType()),
        StructField("primary_place_of_performance_county", StringType()),
        StructField("primary_place_of_performance_zip_code", StringType()),
        StructField("cfda_number", StringType()),
        StructField("cfda_title", StringType()),
        StructField("product_or_service_code", StringType()),
        StructField("product_or_service_code_description", StringType()),
        StructField("naics_code", StringType()),
        StructField("naics_description", StringType()),
        StructField("national_interest_action_code", StringType()),
        StructField("national_interest_action", StringType()),
        StructField("reporting_agency_name", StringType()),
        StructField("submission_period", StringType()),
        StructField("allocation_transfer_agency_identifier_code", StringType()),
        StructField("agency_identifier_code", StringType()),
        StructField("beginning_period_of_availability", DateType()),
        StructField("ending_period_of_availability", DateType()),
        StructField("availability_type_code", StringType()),
        StructField("main_account_code", StringType()),
        StructField("sub_account_code", StringType()),
        StructField("treasury_account_symbol", StringType()),
        StructField("treasury_account_name", StringType()),
        StructField("funding_toptier_agency_id", IntegerType()),
        StructField("federal_account_id", IntegerType()),
        StructField("budget_function", StringType()),
        StructField("budget_function_code", StringType()),
        StructField("budget_subfunction", StringType()),
        StructField("budget_subfunction_code", StringType()),
        StructField("transaction_obligated_amount", DecimalType(23, 2)),
        StructField("gross_outlay_amount_fyb_to_period_end", DecimalType(23, 2)),
        StructField("ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig", DecimalType(23, 2)),
        StructField("ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig", DecimalType(23, 2)),
        StructField("award_base_action_date_fiscal_year", IntegerType()),
        StructField("award_latest_action_date_fiscal_year", IntegerType()),
        StructField("award_type_code", StringType()),
        StructField("award_type", StringType()),
        StructField("prime_award_summary_recipient_cd_original", StringType()),
        StructField("prime_award_summary_recipient_cd_current", StringType()),
        StructField("recipient_zip_code", StringType()),
        StructField("prime_award_summary_place_of_performance_cd_original", StringType()),
        StructField("prime_award_summary_place_of_performance_cd_current", StringType()),
        StructField("usaspending_permalink", StringType()),
        StructField("last_modified_date", DateType()),
        StructField("reporting_fiscal_period", IntegerType()),
        StructField("reporting_fiscal_quarter", IntegerType()),
        StructField("reporting_fiscal_year", IntegerType()),
        StructField("quarter_format_flag", BooleanType()),
        StructField("merged_hash_key", LongType()),
    ]
)


def award_financial_df(spark: SparkSession):
    faba = spark.table("int.financial_accounts_by_awards")
    sa = spark.table("global_temp.submission_attributes")
    taa = spark.table("global_temp.treasury_appropriation_account")
    award_search = spark.table("award_search")
    ts = spark.table("transaction_search")
    rpa = spark.table("global_temp.ref_program_activity")
    oc = spark.table("global_temp.object_class")
    defc = spark.table("global_temp.disaster_emergency_fund_code")
    fa = spark.table("global_temp.federal_account")
    fta = spark.table("global_temp.toptier_agency")
    tta = spark.table("global_temp.toptier_agency")
    cgac_aid = spark.table("global_temp.cgac")
    cgac_ata = spark.table("global_temp.cgac")

    return (
        faba.join(sa, on="submission_id", how="inner")
        .join(taa, on=taa.treasury_account_identifier == faba.treasury_account_id, how="left")
        .join(award_search, on=award_search.award_id == faba.award_id, how="left")
        .join(rpa, on=faba.program_activity_id == rpa.id, how="left")
        .join(ts, on=award_search.latest_transaction_search_id == ts.transaction_id, how="left")
        .join(oc, on=oc.id == faba.object_class_id, how="left")
        .join(defc, on=defc.code == faba.disaster_emergency_fund_code, how="left")
        .join(fa, on=taa.federal_account_id == fa.id, how="left")
        .join(fta, on=fa.parent_toptier_agency_id == fta.toptier_agency_id, how="left")
        .join(tta, on=tta.toptier_agency_id == taa.funding_toptier_agency_id, how="left")
        .join(cgac_aid, on=cgac_aid.code == taa.agency_id, how="left")
        .join(cgac_ata, on=cgac_ata.cgac_code == taa.allocation_transfer_agency_id, how="left")
        .withColumn("submission_period", fy_quarter_period())
        .select(
            faba.financial_accounts_by_awards_id,
            faba.submission_id,
            sf.col("submission_period"),
            fta.name.alias("federal_owning_agency_name"),
            tta.name.alias("treasury_owning_agency_name"),
            fa.federal_account_code.alias("federal_account_symbol"),
            fa.account_title.alias("federal_account_name"),
            cgac_aid.agency_name.alias("agency_identifier_name"),
            cgac_ata.agency_name.alias("allocation_transfer_agency_identifier_name"),
            rpa.program_activity_code,
            rpa.program_activity_name,
            oc.object_class.alias("object_class_code"),
            oc.object_class_name,
            oc.direct_reimbursable.alias("direct_or_reimbursable_funding_source"),
            faba.disaster_emergency_fund_code,
            defc.title.alias("disaster_emergency_fund_name"),
            award_search.generated_unique_award_id.alias("award_unique_key"),
            faba.piid.alias("award_id_piid"),
            faba.parent_award_id.alias("parent_award_id_piid"),
            faba.fain.alias("award_id_fain"),
            faba.uri.alias("award_id_uri"),
            award_search.date_signed.alias("award_base_action_date").cast(DateType()),
            award_search.certified_date.alias("award_latest_action_date").cast(DateType()),
            award_search.period_of_performance_start_date.cast(DateType()),
            award_search.period_of_performance_current_end_date.cast(DateType()),
            ts.ordering_period_end_date.cast(DateType()),
            ts.idv_type.alias("idv_type_code"),
            ts.idv_type_description.alias("idv_type"),
            award_search.description.alias("prime_award_base_transaction_description"),
            ts.awarding_agency_code,
            ts.awarding_toptier_agency_name_raw.alias("awarding_agency_name"),
            ts.awarding_sub_tier_agency_c.alias("awarding_subagency_code"),
            ts.awarding_subtier_agency_name_raw.alias("awarding_subagency_name"),
            ts.awarding_office_code,
            ts.awarding_office_name,
            ts.funding_agency_code,
            ts.funding_toptier_agency_name_raw.alias("funding_agency_name"),
            ts.funding_sub_tier_agency_co.alias("funding_sub_agency_code"),
            ts.funding_subtier_agency_name_raw.alias("funding_sub_agency_name"),
            ts.funding_office_code,
            ts.funding_office_name,
            ts.recipient_uei,
            ts.recipient_unique_id.alias("recipient_duns"),
            ts.recipient_name,
            ts.recipient_name_raw,
            ts.parent_uei.alias("recipient_parent_uei"),
            ts.parent_uei.alias("recipient_parent_duns"),
            ts.parent_recipient_name.alias("recipient_parent_name"),
            ts.parent_recipient_name_raw.alias("recipient_parent_name_raw"),
            ts.recipient_location_country_code.alias("recipient_country"),
            ts.recipient_location_state_code.alias("recipient_state"),
            ts.recipient_location_county_name.alias("recipient_county"),
            ts.recipient_location_city_name.alias("recipient_city"),
            ts.pop_country_name.alias("primary_place_of_performance_country"),
            ts.pop_state_name.alias("primary_place_of_performance_state"),
            ts.pop_county_name.alias("primary_place_of_performance_county"),
            ts.place_of_performance_zip4a.alias("primary_place_of_performance_zip_code"),
            ts.cfda_number,
            ts.cfda_title,
            ts.product_or_service_code,
            ts.product_or_service_description.alias("product_or_service_code_description"),
            ts.naics_code,
            ts.naics_description,
            ts.national_interest_action.alias("national_interest_action_code"),
            ts.national_interest_desc.alias("national_interest_action"),
            sa.reporting_agency_name.alias("reporting_agency_name"),
            taa.allocation_transfer_agency_id.alias("allocation_transfer_agency_identifier_code"),
            taa.agency_id.alais("agency_identifier_code"),
            taa.beginning_period_of_availability.alias("beginning_period_of_availability"),
            taa.ending_period_of_availability.alias("ending_period_of_availability"),
            taa.availability_type_code.alias("availability_type_code"),
            taa.main_account_code.alias("main_account_code"),
            taa.sub_account_code.alias("sub_account_code"),
            taa.tas_rendering_label.alias("treasury_account_symbol"),
            taa.account_title.alias("treasury_account_name"),
            taa.funding_toptier_agency_id.alias("funding_toptier_agency_id"),
            taa.federal_account_id.alias("federal_account_id"),
            taa.budget_function_title.alias("budget_function"),
            taa.budget_function_code.alias("budget_function_code"),
            taa.budget_subfunction_title.alias("budget_subfunction"),
            taa.budget_subfunction_code.alias("budget_subfunction_code"),
            faba.transaction_obligated_amount.alias("transaction_obligated_amount"),
            faba.gross_outlay_amount_by_award_cpe.alias("gross_outlay_amount_fyb_to_period_end"),
            faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe.alias(
                "ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig"
            ),
            faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe.alias(
                "ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig"
            ),
            sf.extract("year", add_months(award_search.date_signed), 3).alias("award_base_action_date_fiscal_year"),
            sf.extract("year", add_months(award_search.certified_date), 3).alias(
                "award_latest_action_date_fiscal_year"
            ),
            sf.coalesce(ts.contract_award_type, ts.type).alias("award_type_code"),
            sf.coalesce(ts.contract_award_type_desc, ts.type_description).alias("award_type"),
            sf.when(
                ts.recipient_location_state_code.isNotNull()
                & ts.recipient_location_congressional_code.isNotNull()
                & ~(ts.recipient_location_state_code == "" & ts.recipient_location_state_code.isNotNull()),
                sf.concat(ts.recipient_location_state_code, "-", ts.recipient_location_congressional_code),
            )
            .otherwise(ts.recipient_location_conggressional_code)
            .alias("prime_award_summary_recipient_cd_original"),
            sf.when(
                ts.recipient_location_state_code.isNotNull()
                & ts.recipient_location_congressional_code_current.isNotNull()
                & ~(ts.recipient_location_state_code.isNotNull() & ts.recipient_location_state_code == ""),
                sf.concat(ts.recipient_location_state_code, "-", ts.recipient_location_congressional_code_current),
            )
            .otherwise(ts.recipient_location_congressional_code_current)
            .alias("prime_award_summary_recipient_cd_current"),
            sf.coalesce(
                ts.legal_entity_zip4,
                sf.concat(ts.recipient_location_zip5.cast(StringType()), ts.legal_entity_zip_last4.cast(StringType())),
            ).alias("recipient_zip_code"),
            sf.when(
                ts.pop_state_code.isNotNull()
                & ts.pop_congressional_code.isNotNull()
                & ~(ts.pop_state_code.isNotNull() & ts.pop_state_code == ""),
                sf.concat(ts.pop_state_code, "-", ts.pop_congressional_code),
            )
            .otherwise(ts.pop_congressional_code)
            .alias("prime_award_summary_place_of_performance_cd_original"),
            sf.when(
                ts.pop_state_code.isNotNull()
                & ts.pop_congressional_code_current.isNotNull()
                & ~(ts.pop_state_code.isNotNull() & ts.pop_state_code == ""),
                sf.concat(ts.pop_state_code, "-", ts.pop_congressional_code_current),
            )
            .otherwise(ts.pop_congressional_code_current)
            .alias("prime_award_summary_place_of_performance_cd_current"),
            sf.when(
                award_search.generated_unique_award_id.isNotNull(),
                sf.concat("{{AWARD_URL}}", sf.url_encode(award_search.generated_unique_award_id), "/"),
            )
            .otherwise("")
            .alias("usaspending_permalink"),
            sa.published_date.cast(DateType()).alias("last_modified_date"),
            sa.reporting_fiscal_period,
            sa.reporting_fiscal_quarter,
            sa.reporting_fiscal_year,
            sa.quarter_format_flag,
        )
        .withColumn("merge_hash_key", sf.xxhash64("*"))
    )


def load_award_financial(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    df = award_financial_df(spark)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{destination_database}.{destination_table_name}")


def load_award_financial_incremental(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = award_financial_df(spark).dataframe.alias("s")
    (
        target.merge(
            source,
            "s.financial_accounts_by_awards_id = t.financial_accounts_by_awards_id and s.merge_hash_key = t.merge_hash_key",
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
