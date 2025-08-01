from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf, Column
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType


account_download_schema = StructType(
    [
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
        StructField("budget_function", StringType()),
        StructField("budget_subfunction", StringType()),
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
        StructField("last_modified_date", DateType()),
    ]
)


def fy_quarter_period() -> Column:
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


def build_account_download_df(spark: SparkSession) -> DataFrame:
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
            cgac_aid.agency_identifier_name,
            cgac_ata.allocation_transfer_agency_identifier_name,
            taa.budget_function_title.alias("budget_function"),
            taa.budget_subfunction_title.alias("budget_subfunction"),
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
            sa.published_date.alias("last_modified_date"),
        )
    )
