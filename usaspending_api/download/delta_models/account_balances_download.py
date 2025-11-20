from delta.tables import DeltaTable
from usaspending_api.download.helpers.delta_models_helpers import fy_quarter_period
from pyspark.sql import DataFrame, functions as sf, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    LongType,
)

account_balances_schema = StructType(
    [
        StructField("funding_toptier_agency_id", IntegerType()),
        StructField("owning_agency_name", StringType()),
        StructField("reporting_agency_name", StringType()),
        StructField("submission_period", StringType()),
        StructField("allocation_transfer_agency_identifier_code", StringType()),
        StructField("agency_identifier_code", StringType()),
        StructField("beginning_period_of_availability", StringType()),
        StructField("ending_period_of_availability", StringType()),
        StructField("availability_type_code", StringType()),
        StructField("main_account_code", StringType()),
        StructField("sub_account_code", StringType()),
        StructField("treasury_account_symbol", StringType()),
        StructField("treasury_account_name", StringType()),
        StructField("agency_identifier_name", StringType()),
        StructField("allocation_transfer_agency_identifier_name", StringType()),
        StructField("budget_function_code", StringType()),
        StructField("budget_function", StringType()),
        StructField("budget_subfunction_code", StringType()),
        StructField("budget_subfunction", StringType()),
        StructField("federal_account_id", IntegerType()),
        StructField("federal_account_symbol", StringType()),
        StructField("federal_account_name", StringType()),
        StructField("budget_authority_unobligated_balance_brought_forward", DecimalType(23, 2)),
        StructField("adjustments_to_unobligated_balance_brought_forward_cpe", DecimalType(23, 2)),
        StructField("budget_authority_appropriated_amount", DecimalType(23, 2)),
        StructField("borrowing_authority_amount", DecimalType(23, 2)),
        StructField("contract_authority_amount", DecimalType(23, 2)),
        StructField("spending_authority_from_offsetting_collections_amount", DecimalType(23, 2)),
        StructField("total_other_budgetary_resources_amount", DecimalType(23, 2)),
        StructField("total_budgetary_resources", DecimalType(23, 2)),
        StructField("obligations_incurred", DecimalType(23, 2)),
        StructField("deobligations_or_recoveries_or_refunds_from_prior_year", DecimalType(23, 2)),
        StructField("unobligated_balance", DecimalType(23, 2)),
        StructField("gross_outlay_amount", DecimalType(23, 2)),
        StructField("status_of_budgetary_resources_total", DecimalType(23, 2)),
        StructField("submission_id", IntegerType()),
        StructField("data_source", StringType()),
        StructField("appropriation_account_balances_id", IntegerType()),
        StructField("drv_appropriation_availability_period_start_date", DateType()),
        StructField("drv_appropriation_availability_period_end_date", DateType()),
        StructField("drv_appropriation_account_expired_status", StringType()),
        StructField("drv_obligations_unpaid_amount", DecimalType(23, 2)),
        StructField("drv_other_obligated_amount", DecimalType(23, 2)),
        StructField("reporting_period_start", DateType()),
        StructField("reporting_period_end", DateType()),
        StructField("appropriation_account_last_modified", DateType()),
        StructField("certified_date", DateType()),
        StructField("create_date", TimestampType()),
        StructField("update_date", TimestampType()),
        StructField("final_of_fy", BooleanType()),
        StructField("treasury_account_identifier", IntegerType()),
        StructField("last_modified_date", DateType()),
        StructField("reporting_fiscal_period", IntegerType()),
        StructField("reporting_fiscal_quarter", IntegerType()),
        StructField("reporting_fiscal_year", IntegerType()),
        StructField("quarter_format_flag", BooleanType()),
        StructField("merge_hash_key", LongType()),
    ]
)


def account_balances_df(spark: SparkSession) -> DataFrame:
    aab = spark.table("global_temp.appropriation_account_balances")
    sa = spark.table("global_temp.submission_attributes")
    taa = spark.table("global_temp.treasury_appropriation_account")
    cgac_aid = spark.table("global_temp.cgac")
    cgac_ata = spark.table("global_temp.cgac")
    fa = spark.table("global_temp.federal_account")
    ta = spark.table("global_temp.toptier_agency")
    return (
        aab.join(sa, on="submission_id", how="inner")
        .join(taa, on="treasury_account_identifier", how="leftouter")
        .join(
            cgac_aid.withColumnRenamed("agency_name", "agency_identifier_name"),
            on=(taa.agency_id == cgac_aid.cgac_code),
            how="leftouter",
        )
        .join(
            cgac_ata.withColumnRenamed("agency_name", "allocation_transfer_agency_identifier_name"),
            on=(taa.allocation_transfer_agency_id == cgac_ata.cgac_code),
            how="leftouter",
        )
        .join(fa, on=taa.federal_account_id == fa.id, how="leftouter")
        .join(ta, on=fa.parent_toptier_agency_id == ta.toptier_agency_id, how="leftouter")
        .withColumn("submission_period", fy_quarter_period())
        .select(
            taa.funding_toptier_agency_id,
            ta.name.alias("owning_agency_name"),
            sa.reporting_agency_name,
            sf.col("submission_period"),
            taa.allocation_transfer_agency_id.alias("allocation_transfer_agency_identifier_code"),
            taa.agency_id.alias("agency_identifier_code"),
            taa.beginning_period_of_availability,
            taa.ending_period_of_availability,
            taa.availability_type_code,
            taa.main_account_code,
            taa.sub_account_code,
            taa.tas_rendering_label.alias("treasury_account_symbol"),
            taa.account_title.alias("treasury_account_name"),
            sf.col("agency_identifier_name"),
            sf.col("allocation_transfer_agency_identifier_name"),
            taa.budget_function_code,
            taa.budget_function_title.alias("budget_function"),
            taa.budget_subfunction_code,
            taa.budget_subfunction_title.alias("budget_subfunction"),
            taa.federal_account_id,
            fa.federal_account_code.alias("federal_account_symbol"),
            fa.account_title.alias("federal_account_name"),
            aab.budget_authority_unobligated_balance_brought_forward_fyb.alias(
                "budget_authority_unobligated_balance_brought_forward"
            ),
            aab.adjustments_to_unobligated_balance_brought_forward_cpe,
            aab.budget_authority_appropriated_amount_cpe.alias("budget_authority_appropriated_amount"),
            aab.borrowing_authority_amount_total_cpe.alias("borrowing_authority_amount"),
            aab.contract_authority_amount_total_cpe.alias("contract_authority_amount"),
            aab.spending_authority_from_offsetting_collections_amount_cpe.alias(
                "spending_authority_from_offsetting_collections_amount"
            ),
            aab.other_budgetary_resources_amount_cpe.alias("total_other_budgetary_resources_amount"),
            aab.total_budgetary_resources_amount_cpe.alias("total_budgetary_resources"),
            aab.obligations_incurred_total_by_tas_cpe.alias("obligations_incurred"),
            aab.deobligations_recoveries_refunds_by_tas_cpe.alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            aab.unobligated_balance_cpe.alias("unobligated_balance"),
            aab.gross_outlay_amount_by_tas_cpe.alias("gross_outlay_amount"),
            aab.status_of_budgetary_resources_total_cpe.alias("status_of_budgetary_resources_total"),
            aab.submission_id,
            aab.data_source,
            aab.appropriation_account_balances_id,
            aab.drv_appropriation_availability_period_start_date,
            aab.drv_appropriation_availability_period_end_date,
            aab.drv_appropriation_account_expired_status,
            aab.drv_obligations_unpaid_amount,
            aab.drv_other_obligated_amount,
            aab.reporting_period_start,
            aab.reporting_period_end,
            aab.last_modified_date.alias("appropriation_account_last_modified"),
            aab.certified_date,
            aab.create_date,
            aab.update_date,
            aab.final_of_fy,
            aab.treasury_account_identifier,
            sf.to_date(sa.published_date).alias("last_modified_date"),
            sa.reporting_fiscal_period,
            sa.reporting_fiscal_quarter,
            sa.reporting_fiscal_year,
            sa.quarter_format_flag,
        )
        .withColumn("merge_hash_key", sf.xxhash64("*"))
    )


def load_account_balances(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    df = account_balances_df(spark)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{destination_database}.{destination_table_name}")


def load_account_balances_incremental(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = account_balances_df(spark).dataframe.alias("s")
    (
        target.merge(
            source,
            "s.appropriation_account_balances_id = t.appropriation_account_balances_id and s.merge_hash_key = t.merge_hash_key",
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
