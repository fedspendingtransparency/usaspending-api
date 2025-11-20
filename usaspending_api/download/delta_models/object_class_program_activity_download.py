from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as sf
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


object_class_program_activity_schema = StructType(
    [
        StructField("financial_accounts_by_program_activity_object_class_id", IntegerType(), False),
        StructField("submission_id", IntegerType(), False),
        StructField("owning_agency_name", StringType()),
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
        StructField("funding_toptier_agency_id", IntegerType()),
        StructField("federal_account_id", IntegerType()),
        StructField("budget_function", StringType()),
        StructField("budget_function_code", StringType()),
        StructField("budget_subfunction", StringType()),
        StructField("budget_subfunction_code", StringType()),
        StructField("reporting_agency_name", StringType()),
        StructField("reporting_fiscal_period", IntegerType()),
        StructField("reporting_fiscal_quarter", IntegerType()),
        StructField("reporting_fiscal_year", IntegerType()),
        StructField("quarter_format_flag", BooleanType()),
        StructField("submission_period", StringType()),
        StructField("USSGL480100_undelivered_orders_obligations_unpaid_FYB", DecimalType(23, 2)),
        StructField("USSGL480100_undelivered_orders_obligations_unpaid", DecimalType(23, 2)),
        StructField("USSGL483100_undelivered_orders_obligations_transferred_unpaid", DecimalType(23, 2)),
        StructField("USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid", DecimalType(23, 2)),
        StructField("USSGL490100_delivered_orders_obligations_unpaid_FYB", DecimalType(23, 2)),
        StructField("USSGL490100_delivered_orders_obligations_unpaid", DecimalType(23, 2)),
        StructField("USSGL493100_delivered_orders_obligations_transferred_unpaid", DecimalType(23, 2)),
        StructField("USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid", DecimalType(23, 2)),
        StructField("USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB", DecimalType(23, 2)),
        StructField("USSGL480200_undelivered_orders_obligations_prepaid_advanced", DecimalType(23, 2)),
        StructField("USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced", DecimalType(23, 2)),
        StructField("USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid", DecimalType(23, 2)),
        StructField("USSGL490200_delivered_orders_obligations_paid", DecimalType(23, 2)),
        StructField("USSGL490800_authority_outlayed_not_yet_disbursed_FYB", DecimalType(23, 2)),
        StructField("USSGL490800_authority_outlayed_not_yet_disbursed", DecimalType(23, 2)),
        StructField("USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid", DecimalType(23, 2)),
        StructField("obligations_undelivered_orders_unpaid_total_FYB", DecimalType(23, 2)),
        StructField("obligations_undelivered_orders_unpaid_total", DecimalType(23, 2)),
        StructField("obligations_delivered_orders_unpaid_total", DecimalType(23, 2)),
        StructField("obligations_delivered_orders_unpaid_total_FYB", DecimalType(23, 2)),
        StructField("gross_outlays_undelivered_orders_prepaid_total", DecimalType(23, 2)),
        StructField("gross_outlays_undelivered_orders_prepaid_total_fyb", DecimalType(23, 2)),
        StructField("gross_outlays_delivered_orders_paid_total_FYB", DecimalType(23, 2)),
        StructField("gross_outlays_delivered_orders_paid_total", DecimalType(23, 2)),
        StructField("gross_outlay_amount_FYB", DecimalType(23, 2)),
        StructField("gross_outlay_amount_FYB_to_period_end", DecimalType(23, 2)),
        StructField("obligations_incurred", DecimalType(23, 2)),
        StructField("USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig", DecimalType(23, 2)),
        StructField("USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig", DecimalType(23, 2)),
        StructField("USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig", DecimalType(23, 2)),
        StructField("USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig", DecimalType(23, 2)),
        StructField("deobligations_or_recoveries_or_refunds_from_prior_year", DecimalType(23, 2)),
        StructField("last_modified_date", DateType()),
        StructField("data_source", StringType()),
        StructField("program_activity_id", IntegerType()),
        StructField("object_class_id", IntegerType()),
        StructField("prior_year_adjustment", StringType()),
        StructField("ussgl480110_rein_undel_ord_cpe", DecimalType(23, 2)),
        StructField("ussgl490110_rein_deliv_ord_cpe", DecimalType(23, 2)),
        StructField("drv_obligations_incurred_by_program_object_class", DecimalType(23, 2)),
        StructField("drv_obligations_undelivered_orders_unpaid", DecimalType(23, 2)),
        StructField("reporting_period_start", DateType()),
        StructField("reporting_period_end", DateType()),
        StructField("certified_date", DateType()),
        StructField("create_date", DateType()),
        StructField("update_date", DateType()),
        StructField("treasury_account_id", IntegerType()),
        StructField("allocation_transfer_agency_identifier_code", StringType()),
        StructField("agency_identifier_code", StringType()),
        StructField("beginning_period_of_availability", StringType()),
        StructField("ending_period_of_availability", StringType()),
        StructField("availability_type_code", StringType()),
        StructField("main_account_code", StringType()),
        StructField("sub_account_code", StringType()),
        StructField("treasury_account_symbol", StringType()),
        StructField("treasury_account_name", StringType()),
        StructField("merged_hash_key", LongType()),
    ]
)


def object_class_program_activity_df(spark: SparkSession):
    fabpaoc = spark.table("global_temp.financial_accounts_by_program_activity_object_class")
    taa = spark.table("global_temp.treasury_appropriation_account")
    sa = spark.table("global_temp.submission_attributes")
    fa = spark.table("global_temp.federal_account")
    ta = spark.table("global_temp.toptier_agency")
    rpa = spark.table("global_temp.ref_program_activity")
    oc = spark.table("global_temp.object_class")
    defc = spark.table("global_temp.disaster_emergency_fund_code")
    cgac_aid = spark.table("global_temp.cgac")
    cgac_ata = spark.table("global_temp.cgac")

    return (
        fabpaoc.join(sa, on="submission_id", how="inner")
        .join(taa, on=taa.treasury_account_identifier == fabpaoc.treasury_account_id, how="left")
        .join(fa, on=taa.federal_account_id == fa.id, how="left")
        .join(ta, on=ta.toptier_agency_id == fa.parent_toptier_agency_id, how="left")
        .join(rpa, on=rpa.id == fabpaoc.program_activity_id, how="left")
        .join(oc, on=fabpaoc.object_class_id == oc.id, how="left")
        .join(defc, on=defc.code == fabpaoc.disaster_emergency_fund_code, how="left")
        .join(
            cgac_aid.withColumnRenamed("agency_name", "agency_identifier_name"),
            on=taa.agency_id == cgac_aid.cgac_code,
            how="left",
        )
        .join(
            cgac_ata.withColumnRenamed("agency_name", "allocation_transfer_agency_identifier_name"),
            on=cgac_ata.cgac_code == taa.allocation_transfer_agency_id,
            how="left",
        )
        .withColumn("submission_period", fy_quarter_period())
        .select(
            fabpaoc.financial_accounts_by_program_activity_object_class_id,
            fabpaoc.submission_id,
            sf.col("submission_period"),
            ta.name.alias("owning_agency_name"),
            fa.federal_account_code.alias("federal_account_symbol"),
            fa.account_title.alias("federal_account_name"),
            rpa.program_activity_code,
            rpa.program_activity_name,
            oc.object_class.alias("object_class_code"),
            oc.object_class_name,
            oc.direct_reimbursable.alias("direct_or_reimbursable_funding_source"),
            fabpaoc.disaster_emergency_fund_code,
            defc.title.alias("disaster_emergency_fund_name"),
            taa.funding_toptier_agency_id,
            taa.federal_account_id,
            taa.budget_function_title.alias("budget_function"),
            taa.budget_function_code,
            taa.budget_subfunction_title.alias("budget_subfunction"),
            taa.budget_subfunction_code,
            sa.reporting_agency_name,
            sa.reporting_fiscal_period,
            sa.reporting_fiscal_quarter,
            sa.reporting_fiscal_year,
            sa.quarter_format_flag,
            fabpaoc.ussgl480100_undelivered_orders_obligations_unpaid_fyb.alias(
                "USSGL480100_undelivered_orders_obligations_unpaid_FYB"
            ),
            fabpaoc.ussgl480100_undelivered_orders_obligations_unpaid_cpe.alias(
                "USSGL480100_undelivered_orders_obligations_unpaid"
            ),
            fabpaoc.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe.alias(
                "USSGL483100_undelivered_orders_obligations_transferred_unpaid"
            ),
            fabpaoc.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe.alias(
                "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid"
            ),
            fabpaoc.ussgl490100_delivered_orders_obligations_unpaid_fyb.alias(
                "USSGL490100_delivered_orders_obligations_unpaid_FYB"
            ),
            fabpaoc.ussgl490100_delivered_orders_obligations_unpaid_cpe.alias(
                "USSGL490100_delivered_orders_obligations_unpaid"
            ),
            fabpaoc.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe.alias(
                "USSGL493100_delivered_orders_obligations_transferred_unpaid"
            ),
            fabpaoc.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe.alias(
                "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid"
            ),
            fabpaoc.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb.alias(
                "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB"
            ),
            fabpaoc.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe.alias(
                "USSGL480200_undelivered_orders_obligations_prepaid_advanced"
            ),
            fabpaoc.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe.alias(
                "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced"
            ),
            fabpaoc.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe.alias(
                "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid"
            ),
            fabpaoc.ussgl490200_delivered_orders_obligations_paid_cpe.alias(
                "USSGL490200_delivered_orders_obligations_paid"
            ),
            fabpaoc.ussgl490800_authority_outlayed_not_yet_disbursed_fyb.alias(
                "USSGL490800_authority_outlayed_not_yet_disbursed_FYB"
            ),
            fabpaoc.ussgl490800_authority_outlayed_not_yet_disbursed_cpe.alias(
                "USSGL490800_authority_outlayed_not_yet_disbursed"
            ),
            fabpaoc.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe.alias(
                "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid"
            ),
            fabpaoc.obligations_undelivered_orders_unpaid_total_fyb.alias(
                "obligations_undelivered_orders_unpaid_total_FYB"
            ),
            fabpaoc.obligations_undelivered_orders_unpaid_total_cpe.alias(
                "obligations_undelivered_orders_unpaid_total"
            ),
            fabpaoc.obligations_delivered_orders_unpaid_total_cpe.alias("obligations_delivered_orders_unpaid_total"),
            fabpaoc.obligations_delivered_orders_unpaid_total_cpe.alias(
                "obligations_delivered_orders_unpaid_total_FYB"
            ),
            fabpaoc.gross_outlays_undelivered_orders_prepaid_total_cpe.alias(
                "gross_outlays_undelivered_orders_prepaid_total"
            ),
            fabpaoc.gross_outlays_undelivered_orders_prepaid_total_cpe.alias(
                "gross_outlays_undelivered_orders_prepaid_total_FYB"
            ),
            fabpaoc.gross_outlays_delivered_orders_paid_total_fyb.alias(
                "gross_outlays_delivered_orders_paid_total_FYB"
            ),
            fabpaoc.gross_outlays_delivered_orders_paid_total_cpe.alias("gross_outlays_delivered_orders_paid_total"),
            fabpaoc.gross_outlay_amount_by_program_object_class_fyb.alias("gross_outlay_amount_FYB"),
            fabpaoc.gross_outlay_amount_by_program_object_class_cpe.alias("gross_outlay_amount_FYB_to_period_end"),
            fabpaoc.obligations_incurred_by_program_object_class_cpe.alias("obligations_incurred"),
            fabpaoc.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe.alias(
                "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig"
            ),
            fabpaoc.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe.alias(
                "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig"
            ),
            fabpaoc.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe.alias(
                "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig"
            ),
            fabpaoc.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe.alias(
                "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig"
            ),
            fabpaoc.deobligations_recoveries_refund_pri_program_object_class_cpe.alias(
                "deobligations_or_recoveries_or_refunds_from_prior_year"
            ),
            sa.published_date.alias("last_modified_date").cast(DateType()),
            fabpaoc.data_source,
            fabpaoc.program_activity_id,
            fabpaoc.object_class_id,
            fabpaoc.prior_year_adjustment,
            fabpaoc.ussgl480110_rein_undel_ord_cpe,
            fabpaoc.ussgl490110_rein_deliv_ord_cpe,
            fabpaoc.drv_obligations_incurred_by_program_object_class,
            fabpaoc.drv_obligations_undelivered_orders_unpaid,
            fabpaoc.reporting_period_start,
            fabpaoc.reporting_period_end,
            fabpaoc.certified_date,
            fabpaoc.create_date.cast(DateType()),
            fabpaoc.update_date,
            fabpaoc.treasury_account_id,
            taa.allocation_transfer_agency_id.alias("allocation_transfer_agency_identifier_code"),
            taa.agency_id.alias("agency_identifier_code"),
            taa.beginning_period_of_availability,
            taa.ending_period_of_availability,
            taa.availability_type_code,
            taa.main_account_code,
            taa.sub_account_code,
            taa.tas_rendering_label.alias("treasury_account_symbol"),
            taa.account_title.alias("treasury_account_name"),
        )
        .withColumn("merge_hash_key", sf.xxhash64("*"))
    )


def load_object_class_program_activity(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    df = object_class_program_activity_df(spark)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{destination_database}.{destination_table_name}")


def load_object_class_program_activity_incremental(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = object_class_program_activity_df(spark).dataframe.alias("s")
    (
        target.merge(
            source,
            "s.financial_accounts_by_program_activity_object_class_id = t.financial_accounts_by_program_activity_object_class_id and s.merge_hash_key = t.merge_hash_key",
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
