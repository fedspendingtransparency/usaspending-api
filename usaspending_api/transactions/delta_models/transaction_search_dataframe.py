from pyspark.sql import DataFrame, SparkSession, types, functions as sf, Column, Window
from pyspark.sql.types import ShortType, StringType


def hash_col(col):
    return sf.regexp_replace(sf.md5(sf.upper(col)), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")


def load_transaction_search_dataframe(spark: SparkSession) -> DataFrame:

    # Base Tables
    tn = spark.read.format("delta").load("s3a://data/data/delta/int/transaction_normalized")
    tfpds = spark.read.format("delta").load("s3a://data/data/delta/int/transaction_fpds")
    tfabs = spark.read.format("delta").load("s3a://data/data/delta/int/transaction_fabs")
    awards = spark.read.format("delta").load("s3a://data/data/delta/int/awards")
    rcfda = spark.table("global_temp.references_cfda")
    rlu = spark.read.format("delta").load("s3a://data/data/delta/rpt/recipient_lookup")
    prl = (
        spark.read.format("delta")
        .load("s3a://data/data/delta/rpt/recipient_lookup")
        .select(
            sf.col("recipient_hash").alias("parent_recipient_hash"),
            sf.col("legal_business_name").alias("parent_recipient_name"),
        )
    )
    rp = spark.read.format("delta").load("s3a://data/data/delta/rpt/recipient_profile")
    aa = spark.table("global_temp.agency")
    taa = (
        spark.table("global_temp.toptier_agency")
        .withColumn("awarding_toptier_agency_abbreviation", sf.col("abbreviation"))
        .withColumn("awarding_toptier_agency_name", sf.col("name"))
    )
    saa = (
        spark.table("global_temp.subtier_agency")
        .withColumn("awarding_subtier_agency_abbreviation", sf.col("abbreviation"))
        .withColumn("awarding_subtier_agency_name", sf.col("name"))
    )
    aa_id = spark.table("global_temp.agency").withColumn("awarding_toptier_agency_id", sf.col("id"))
    fa = (
        spark.table("global_temp.agency")
        .withColumn("funding_toptier_agency_id", aa.toptier_agency_id)
        .withColumn("funding_subtier_agency_id", sf.col("subtier_agency_id"))
    )
    tfa = (
        spark.table("global_temp.toptier_agency")
        .alias("tfa")
        .withColumn("funding_toptier_agency_id", sf.col("toptier_agency_id"))
        .withColumn("funding_toptier_agency_abbreviation", sf.col("abbreviation"))
        .withColumn("funding_toptier_agency_name", sf.col("name"))
    )
    sfa = (
        spark.table("global_temp.subtier_agency")
        .withColumn("funding_subtier_agency_abbreviation", sf.col("abbreviation"))
        .withColumn("funding_subtier_agency_name", sf.col("name"))
    )
    w = Window.partitionBy(aa.toptier_agency_id).orderBy(aa.toptier_flag.desc(), aa.id.asc())
    fa_id = (
        spark.table("global_temp.agency")
        .withColumn("funding_toptier_agency_id", sf.col("id"))
        .withColumn("row_num", sf.row_number().over(w))
    )
    ao = spark.table("global_temp.office").withColumn("awarding_office_name", sf.col("office_name"))
    fo = spark.table("global_temp.office").withColumn("funding_office_name", sf.col("office_name"))
    state_data = spark.table("global_temp.state_data")
    ref_population_county = spark.table("global_temp.ref_population_county")
    ref_population_cong_district = spark.table("global_temp.ref_population_cong_district")

    # Subqueries
    recipient_hash_and_levels = (
        rp.groupBy("recipient_hash", "uei")
        .agg(sf.sort_array(sf.collect_set("recipient_level")).alias("recipient_levels"))
        .select(
            sf.col("recipient_hash").alias("recipient_level_hash"),
            sf.col("recipient_levels"),
        )
    )

    pop_state_lookup = (
        state_data.groupBy("code", "name", "fips")
        .agg(sf.max(sf.col("id")))
        .select(
            sf.col("code").alias("pop_state_code"),
            sf.col("fips").alias("pop_state_fips"),
        )
    )

    rl_state_lookup = (
        state_data.groupBy("code", "name", "fips")
        .agg(sf.max(sf.col("id")))
        .select(
            sf.col("code").alias("rl_state_code"),
            sf.col("fips").alias("recipient_location_state_fips"),
        )
    )

    key_cols = [
        tn.id.alias("transaction_id"),
        tn.award_id,
        tn.transaction_unique_id,
        tn.usaspending_unique_transaction_id,
        tn.modification_number,
        awards.generated_unique_award_id,
    ]

    date_cols = [
        sf.to_date(tn.action_date),
        sf.add_months(sf.to_date(tn.action_date), 3).alias("fiscal_action_date"),
        sf.to_date(tn.last_modified_date),
        tn.fiscal_year,
        awards.certified_date.alias("award_certified_date"),
        sf.year(sf.add_months(sf.to_date(awards.certified_date), 3)).alias("award_fiscal_year"),
        tn.create_date,
        tn.update_date,
        awards.update_date.alias("award_update_date"),
        sf.to_date(awards.date_signed).alias("award_date_signed"),
        sf.greatest(sf.to_timestamp(tn.update_date), awards.update_date).alias("etl_update_date"),
        tn.period_of_performance_start_date,
        tn.period_of_performance_current_end_date,
        sf.coalesce(
            sf.to_date(tfabs.created_at),
            sf.to_date(tfpds.initial_report_date),
        ).alias("initial_report_date"),
    ]

    agency_cols = [
        sf.coalesce(tfabs.awarding_agency_code, tfpds.awarding_agency_code).alias("awarding_agency_code"),
        taa.awarding_toptier_agency_name,
        sf.coalesce(tfabs.awarding_agency_name, tfpds.awarding_agency_name).alias("awarding_toptier_agency_name_raw"),
        sf.coalesce(tfabs.funding_agency_code, tfpds.funding_agency_code).alias("funding_agency_code"),
        tfa.funding_toptier_agency_name,
        sf.coalesce(tfabs.funding_agency_name, tfpds.funding_agency_name).alias("funding_toptier_agency_name_raw"),
        sf.coalesce(tfabs.awarding_sub_tier_agency_c, tfpds.awarding_sub_tier_agency_c).alias(
            "awarding_sub_tier_agency_c"
        ),
        saa.awarding_subtier_agency_name,
        sf.coalesce(tfabs.awarding_sub_tier_agency_n, tfpds.awarding_sub_tier_agency_n).alias(
            "awarding_subtier_agency_name_raw"
        ),
        sf.coalesce(tfabs.funding_sub_tier_agency_co, tfpds.funding_sub_tier_agency_co).alias(
            "funding_sub_tier_agency_co"
        ),
        sfa.funding_subtier_agency_name,
        sf.coalesce(tfabs.funding_sub_tier_agency_na, tfpds.funding_sub_tier_agency_na).alias(
            "funding_subtier_agency_name_raw"
        ),
        aa_id.awarding_toptier_agency_id,
        fa_id.funding_toptier_agency_id,
        tn.awarding_agency_id,
        tn.funding_agency_id,
        taa.awarding_toptier_agency_abbreviation,
        tfa.funding_toptier_agency_abbreviation,
        saa.awarding_subtier_agency_abbreviation,
        sfa.funding_subtier_agency_abbreviation,
        sf.coalesce(tfabs.awarding_office_code, tfpds.awarding_office_code).alias("awarding_office_code"),
        sf.coalesce(ao.awarding_office_name, tfabs.awarding_office_name, tfpds.awarding_office_name).alias(
            "awarding_office_name"
        ),
        sf.coalesce(tfabs.funding_office_code, tfpds.funding_office_code).alias("funding_office_code"),
        sf.coalesce(fo.funding_office_name, tfabs.funding_office_name, tfpds.funding_office_name).alias(
            "funding_office_name"
        ),
    ]

    recipient_hash = hash_col(
        sf.when(
            sf.coalesce(tfpds.awardee_or_recipient_uei, tfabs.uei).isNotNull(),
            sf.concat(sf.lit("uei-"), sf.coalesce(tfpds.awardee_or_recipient_uei, tfabs.uei)),
        ).otherwise(
            sf.when(
                sf.coalesce(tfpds.awardee_or_recipient_uniqu, tfabs.awardee_or_recipient_uniqu).isNotNull(),
                sf.concat(
                    sf.lit("duns-"), sf.coalesce(tfpds.awardee_or_recipient_uniqu, tfabs.awardee_or_recipient_uniqu)
                ),
            ).otherwise(
                sf.concat(
                    sf.lit("name-"), sf.coalesce(tfpds.awardee_or_recipient_legal, tfabs.awardee_or_recipient_legal)
                )
            )
        )
    )
    parent_recipient_hash = hash_col(
        sf.when(
            sf.coalesce(tfpds.ultimate_parent_uei, tfabs.ultimate_parent_uei).isNotNull(),
            sf.concat(sf.lit("uei-"), sf.coalesce(tfpds.ultimate_parent_uei, tfabs.ultimate_parent_uei)),
        ).otherwise(
            sf.when(
                sf.coalesce(tfpds.ultimate_parent_unique_ide, tfabs.ultimate_parent_unique_ide).isNotNull(),
                sf.concat(
                    sf.lit("duns-"), sf.coalesce(tfpds.ultimate_parent_unique_ide, tfabs.ultimate_parent_unique_ide)
                ),
            ).otherwise(
                sf.concat(
                    sf.lit("name-"), sf.coalesce(tfpds.ultimate_parent_legal_enti, tfabs.ultimate_parent_legal_enti)
                )
            )
        )
    )

    df = (
        tn.join(tfabs, (tn.id == tfabs.transaction_id) & ~tn.is_fpds, "leftouter")
        .join(tfpds, (tn.id == tfpds.transaction_id) & tn.is_fpds, "leftouter")
        .join(rcfda, tfabs.cfda_number == rcfda.program_number, "leftouter")
        .join(rlu, rlu.recipient_hash == recipient_hash, "leftouter")
        .join(awards, tn.award_id == awards.id, "leftouter")
        .join(aa, tn.awarding_agency_id == aa.id, "leftouter")
        .join(taa, aa.toptier_agency_id == taa.toptier_agency_id, "leftouter")
        .join(saa, aa.subtier_agency_id == saa.subtier_agency_id, "leftouter")
        .join(aa_id, ((aa_id.toptier_agency_id == taa.toptier_agency_id) & aa_id.toptier_flag), "left_outer")
        .join(fa, tn.funding_agency_id == fa.id, "leftouter")
        .join(tfa, fa.funding_toptier_agency_id == tfa.toptier_agency_id, "leftouter")
        .join(sfa, fa.funding_subtier_agency_id == sfa.subtier_agency_id, "leftouter")
        .join(fa_id, ((fa_id.toptier_agency_id == tfa.funding_toptier_agency_id) & (fa_id.row_num == 1)), "leftouter")
        .join(prl, prl.parent_recipient_hash == parent_recipient_hash, "leftouter")
        .join(ao, ao.office_code == sf.coalesce(tfabs.awarding_office_code, tfpds.awarding_office_code), "leftouter")
        .join(fo, fo.office_code == sf.coalesce(tfabs.funding_office_code, tfpds.funding_office_code), "leftouter")
        .join(
            recipient_hash_and_levels,
            (sf.col("recipient_hash") == recipient_hash_and_levels.recipient_level_hash)
            & ~(
                sf.col("legal_business_name").isin(
                    [
                        "MULTIPLE RECIPIENTS",
                        "REDACTED DUE TO PII",
                        "MULTIPLE FOREIGN RECIPIENTS",
                        "PRIVATE INDIVIDUAL",
                        "INDIVIDUAL RECIPIENT",
                        "MISCELLANEOUS FOREIGN AWARDEES",
                    ]
                )
            ),
            "leftouter",
        )
        .join(
            pop_state_lookup,
            pop_state_lookup.pop_state_code
            == sf.coalesce(tfpds.place_of_performance_state, tfabs.place_of_perfor_state_code),
            "leftouter",
        )
        .join(
            ref_population_county,
            (ref_population_county.state_code == pop_state_lookup.pop_state_fips)
            & (
                ref_population_county.county_number
                == sf.lpad(
                    sf.regexp_extract(
                        sf.coalesce(tfpds.place_of_perform_county_co, tfabs.place_of_perform_county_co),
                        "^[A-Z]*(\d+)(?:.\d+)?$",
                        1,
                    )
                    .cast(ShortType())
                    .cast(StringType()),
                    3,
                    "0",
                )
            ),
            "leftouter",
        )
        .join(
            ref_population_cong_district,
            (ref_population_cong_district.state_code == pop_state_lookup.pop_state_fips)
            & (
                ref_population_cong_district.congressional_district
                == sf.lpad(
                    sf.regexp_extract(
                        sf.coalesce(tfpds.place_of_performance_congr, tfabs.place_of_performance_congr),
                        "^[A-Z]*(\d+)(?:.\d+)?$",
                        1,
                    )
                    .cast(ShortType())
                    .cast(StringType()),
                    2,
                    "0",
                )
            ),
            "leftouter",
        )
        .join(
            rl_state_lookup,
            rl_state_lookup.rl_state_code == sf.coalesce(tfpds.legal_entity_state_code, tfabs.legal_entity_state_code),
            "leftouter",
        )
        .select(
            *key_cols,
            *date_cols,
            *agency_cols,
        )
    )

    return df
