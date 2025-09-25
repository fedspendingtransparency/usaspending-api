from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as sf, Column, Window
from pyspark.sql.types import (
    DecimalType,
    ShortType,
    StringType,
    TimestampType,
)

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping


ALL_AWARD_TYPES = list(award_type_mapping.keys())


def hash_col(col: Column) -> Column:
    return sf.regexp_replace(sf.md5(sf.upper(col)), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")


def extract_numbers_as_string(col: Column, length: int = 2, pad: str = "0") -> Column:
    return sf.lpad(
        sf.regexp_extract(col, r"^[A-Z]*(\d+)(?:.\d+)?$", 1).cast(ShortType()).cast(StringType()), length, pad
    )


def transaction_search_dataframe(spark: SparkSession) -> DataFrame:

    # Base Tables
    tn = spark.table("int.transaction_normalized")
    tfpds = spark.table("int.transaction_fpds")
    tfabs = spark.table("int.transaction_fabs")
    awards = spark.table("int.awards")
    rcfda = spark.table("global_temp.references_cfda")
    rlu = spark.table("rpt.recipient_lookup")
    prl = spark.table("rpt.recipient_lookup").select(
        sf.col("recipient_hash").alias("parent_recipient_hash"),
        sf.col("legal_business_name").alias("parent_recipient_name"),
    )
    rp = spark.table("rpt.recipient_profile")
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
    pop_state_population = ref_population_county.alias("pop_state_population").withColumn(
        "pop_state_population", ref_population_county.latest_population
    )
    pop_county_population = ref_population_county.alias("pop_county_population").withColumn(
        "pop_county_population", ref_population_county.latest_population
    )
    rl_state_population = ref_population_county.alias("rl_state_population").withColumn(
        "recipient_location_state_population", ref_population_county.latest_population
    )
    rl_county_population = ref_population_county.alias("rl_county_population").withColumn(
        "recipient_location_county_population", ref_population_county.latest_population
    )
    ref_population_cong_district = spark.table("global_temp.ref_population_cong_district")
    pop_district_population = ref_population_cong_district.alias("pop_district_population").withColumn(
        "pop_congressional_population", ref_population_cong_district.latest_population
    )
    rl_district_population = ref_population_cong_district.alias("rl_district_population").withColumn(
        "recipient_location_congressional_population", ref_population_cong_district.latest_population
    )
    current_cd = spark.table("int.transaction_current_cd_lookup")
    faba = spark.table("int.financial_accounts_by_awards")
    federal_account = spark.table("global_temp.federal_account")
    treasury_appropriation_account = spark.table("global_temp.treasury_appropriation_account")
    rpa = spark.table("global_temp.ref_program_activity")
    pap = spark.table("global_temp.program_activity_park")

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

    fed_and_tres_acct = (
        faba.join(
            treasury_appropriation_account,
            treasury_appropriation_account.treasury_account_identifier == faba.treasury_account_id,
            "inner",
        )
        .join(federal_account, federal_account.id == treasury_appropriation_account.federal_account_id, "inner")
        .join(taa, federal_account.parent_toptier_agency_id == taa.toptier_agency_id, "inner")
        .join(rpa, faba.program_activity_id == rpa.id, "left")
        .join(pap, faba.program_activity_reporting_key == pap.code, "left")
        .filter(faba["award_id"].isNotNull())
        .groupBy(faba.award_id)
        .agg(
            sf.to_json(
                sf.sort_array(
                    sf.collect_set(
                        sf.struct(
                            federal_account.id.alias("id"),
                            federal_account.account_title.alias("account_title"),
                            federal_account.federal_account_code.alias("federal_account_code"),
                        )
                    )
                )
            ).alias("federal_accounts"),
            sf.when(
                sf.size(sf.collect_set(faba.disaster_emergency_fund_code)) > 0,
                sf.sort_array(sf.collect_set(faba.disaster_emergency_fund_code)),
            )
            .otherwise(None)
            .alias("disaster_emergency_fund_codes"),
            sf.sort_array(sf.collect_set(treasury_appropriation_account.treasury_account_identifier)).alias(
                "treasury_account_identifiers"
            ),
            sf.sort_array(
                sf.collect_set(
                    sf.concat(
                        sf.lit("agency="),
                        sf.coalesce(taa.toptier_code, sf.lit("")),
                        sf.lit("faaid="),
                        sf.coalesce(federal_account.agency_identifier, sf.lit("")),
                        sf.lit("famain="),
                        sf.coalesce(federal_account.main_account_code, sf.lit("")),
                        sf.lit("aid="),
                        sf.coalesce(treasury_appropriation_account.agency_id, sf.lit("")),
                        sf.lit("main="),
                        sf.coalesce(treasury_appropriation_account.main_account_code, sf.lit("")),
                        sf.lit("ata="),
                        sf.coalesce(treasury_appropriation_account.allocation_transfer_agency_id, sf.lit("")),
                        sf.lit("sub="),
                        sf.coalesce(treasury_appropriation_account.sub_account_code, sf.lit("")),
                        sf.lit("bpoa="),
                        sf.coalesce(treasury_appropriation_account.beginning_period_of_availability, sf.lit("")),
                        sf.lit("epoa="),
                        sf.coalesce(treasury_appropriation_account.ending_period_of_availability, sf.lit("")),
                        sf.lit("a="),
                        sf.coalesce(treasury_appropriation_account.availability_type_code, sf.lit("")),
                    )
                )
            ).alias("tas_paths"),
            sf.sort_array(
                sf.collect_set(
                    sf.concat(
                        sf.lit("aid="),
                        sf.coalesce(treasury_appropriation_account.agency_id, sf.lit("")),
                        sf.lit("main="),
                        sf.coalesce(treasury_appropriation_account.main_account_code, sf.lit("")),
                        sf.lit("ata="),
                        sf.coalesce(treasury_appropriation_account.allocation_transfer_agency_id, sf.lit("")),
                        sf.lit("sub="),
                        sf.coalesce(treasury_appropriation_account.sub_account_code, sf.lit("")),
                        sf.lit("bpoa="),
                        sf.coalesce(treasury_appropriation_account.beginning_period_of_availability, sf.lit("")),
                        sf.lit("epoa="),
                        sf.coalesce(treasury_appropriation_account.ending_period_of_availability, sf.lit("")),
                        sf.lit("a="),
                        sf.coalesce(treasury_appropriation_account.availability_type_code, sf.lit("")),
                    )
                ),
                True,
            ).alias("tas_components"),
            sf.collect_set(
                sf.to_json(
                    sf.struct(
                        sf.coalesce(pap.name, sf.upper(rpa.program_activity_name)).alias("name"),
                        sf.coalesce(pap.code, sf.lpad(rpa.program_activity_code, 4, "0")).alias("code"),
                        sf.when(pap["code"].isNotNull(), sf.lit("PARK")).otherwise(sf.lit("PAC/PAN")).alias("type"),
                    )
                )
            ).alias("program_activities"),
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

    # Select Columns

    key_cols = [
        tn.id.alias("transaction_id"),
        tn.award_id,
        tn.transaction_unique_id,
        tn.usaspending_unique_transaction_id,
        tn.modification_number,
        awards.generated_unique_award_id,
    ]

    date_cols = [
        sf.to_date(tn.action_date).alias("action_date"),
        sf.add_months(sf.to_date(tn.action_date), 3).alias("fiscal_action_date"),
        sf.to_date(tn.last_modified_date).alias("last_modified_date"),
        tn.fiscal_year,
        awards.certified_date.alias("award_certified_date"),
        sf.year(sf.add_months(sf.to_date(awards.certified_date), 3)).alias("award_fiscal_year"),
        tn.create_date.cast(TimestampType()),
        tn.update_date.cast(TimestampType()),
        awards.update_date.cast(TimestampType()).alias("award_update_date"),
        sf.to_date(awards.date_signed).alias("award_date_signed"),
        sf.greatest(sf.to_timestamp(tn.update_date), awards.update_date).alias("etl_update_date"),
        sf.to_date(tn.period_of_performance_start_date).alias("period_of_performance_start_date"),
        sf.to_date(tn.period_of_performance_current_end_date).alias("period_of_performance_current_end_date"),
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

    typing_cols = [
        tn.is_fpds,
        tn.type.alias("type_raw"),
        tn.type_description.alias("type_description_raw"),
        sf.when(~tn["type"].isin(ALL_AWARD_TYPES) | tn["type"].isNull(), "-1").otherwise(tn.type).alias("type"),
        sf.when(~tn["type"].isin(ALL_AWARD_TYPES) | tn["type"].isNull(), "NOT SPECIFIED")
        .otherwise(tn.type_description)
        .alias("type_description"),
        tn.action_type,
        tn.action_type_description,
        awards.category.alias("award_category"),
        tn.description.alias("transaction_description"),
        tn.business_categories,
    ]

    amounts_cols = [
        sf.coalesce(
            sf.when(tn["type"].isin(["07", "08"]), awards.total_subsidy_cost).otherwise(awards.total_obligation),
            sf.lit(0),
        )
        .cast(DecimalType(23, 2))
        .alias("award_amount"),
        sf.coalesce(
            sf.when(tn["type"].isin(["07", "08"]), tn.original_loan_subsidy_cost).otherwise(
                tn.federal_action_obligation
            ),
            sf.lit(0),
        )
        .cast(DecimalType(23, 2))
        .alias("generated_pragmatic_obligation"),
        sf.coalesce(tn.federal_action_obligation, sf.lit(0))
        .cast(DecimalType(23, 2))
        .alias("federal_action_obligation"),
        sf.coalesce(tn.original_loan_subsidy_cost, sf.lit(0))
        .cast(DecimalType(23, 2))
        .alias("original_loan_subsidy_cost"),
        sf.coalesce(tn.face_value_loan_guarantee, sf.lit(0))
        .cast(DecimalType(23, 2))
        .alias("face_value_loan_guarantee"),
        tn.indirect_federal_sharing.cast(DecimalType(23, 2)),
        tn.funding_amount,
        sf.coalesce(tfabs.total_funding_amount, sf.lit("0")).cast(DecimalType(23, 2)).alias("total_funding_amount"),
        tn.non_federal_funding_amount,
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

    recipient_cols = [
        sf.coalesce(
            rlu.recipient_hash,
            hash_col(
                sf.when(
                    sf.coalesce(tfpds.awardee_or_recipient_uei, tfabs.uei).isNotNull(),
                    sf.concat(sf.lit("uei-"), sf.coalesce(tfpds.awardee_or_recipient_uei, tfabs.uei)),
                ).otherwise(
                    sf.when(
                        sf.coalesce(tfpds.awardee_or_recipient_uniqu, tfabs.awardee_or_recipient_uniqu).isNotNull(),
                        sf.concat(
                            sf.lit("duns-"),
                            sf.coalesce(tfpds.awardee_or_recipient_uniqu, tfabs.awardee_or_recipient_uniqu),
                        ),
                    ).otherwise(
                        sf.concat(
                            sf.lit("name-"),
                            sf.coalesce(tfpds.awardee_or_recipient_legal, tfabs.awardee_or_recipient_legal, sf.lit("")),
                        )
                    )
                )
            ),
        ).alias("recipient_hash"),
        recipient_hash_and_levels.recipient_levels,
        sf.coalesce(tfpds.awardee_or_recipient_uei, tfabs.uei).alias("recipient_uei"),
        sf.coalesce(tfpds.awardee_or_recipient_legal, tfabs.awardee_or_recipient_legal).alias("recipient_name_raw"),
        sf.upper(
            sf.coalesce(
                rlu.legal_business_name,
                tfpds.awardee_or_recipient_legal,
                tfabs.awardee_or_recipient_legal,
            )
        ).alias("recipient_name"),
        sf.coalesce(tfpds.awardee_or_recipient_uniqu, tfabs.awardee_or_recipient_uniqu).alias("recipient_unique_id"),
        prl.parent_recipient_hash,
        sf.coalesce(tfpds.ultimate_parent_uei, tfabs.ultimate_parent_uei).alias("parent_uei"),
        sf.coalesce(tfpds.ultimate_parent_legal_enti, tfabs.ultimate_parent_legal_enti).alias(
            "parent_recipient_name_raw"
        ),
        sf.upper(prl.parent_recipient_name).alias("parent_recipient_name"),
        sf.coalesce(tfpds.ultimate_parent_unique_ide, tfabs.ultimate_parent_unique_ide).alias(
            "parent_recipient_unique_id"
        ),
    ]

    recipient_location_cols = [
        sf.coalesce(tfpds.legal_entity_country_code, tfabs.legal_entity_country_code).alias(
            "recipient_location_country_code"
        ),
        sf.coalesce(tfpds.legal_entity_country_name, tfabs.legal_entity_country_name).alias(
            "recipient_location_country_name"
        ),
        sf.coalesce(tfpds.legal_entity_state_code, tfabs.legal_entity_state_code).alias(
            "recipient_location_state_code"
        ),
        sf.coalesce(tfpds.legal_entity_state_descrip, tfabs.legal_entity_state_name).alias(
            "recipient_location_state_name"
        ),
        rl_state_lookup.recipient_location_state_fips,
        rl_state_population.recipient_location_state_population,
        extract_numbers_as_string(sf.coalesce(tfpds.legal_entity_county_code, tfabs.legal_entity_county_code), 3).alias(
            "recipient_location_county_code"
        ),
        sf.coalesce(tfpds.legal_entity_county_name, tfabs.legal_entity_county_name).alias(
            "recipient_location_county_name"
        ),
        rl_county_population.recipient_location_county_population,
        extract_numbers_as_string(
            sf.coalesce(tfpds.legal_entity_congressional, tfabs.legal_entity_congressional)
        ).alias("recipient_location_congressional_code"),
        rl_district_population.recipient_location_congressional_population,
        current_cd.recipient_location_congressional_code_current.alias("recipient_location_congressional_code_current"),
        sf.coalesce(tfpds.legal_entity_zip5, tfabs.legal_entity_zip5).alias("recipient_location_zip5"),
        tfpds.legal_entity_zip4,
        sf.coalesce(tfpds.legal_entity_zip_last4, tfabs.legal_entity_zip_last4).alias("legal_entity_zip_last4"),
        tfabs.legal_entity_city_code,
        sf.rtrim(sf.coalesce(tfpds.legal_entity_city_name, tfabs.legal_entity_city_name)).alias(
            "recipient_location_city_name"
        ),
        sf.coalesce(tfpds.legal_entity_address_line1, tfabs.legal_entity_address_line1).alias(
            "legal_entity_address_line1"
        ),
        sf.coalesce(tfpds.legal_entity_address_line2, tfabs.legal_entity_address_line2).alias(
            "legal_entity_address_line2"
        ),
        sf.coalesce(tfpds.legal_entity_address_line3, tfabs.legal_entity_address_line3).alias(
            "legal_entity_address_line3"
        ),
        tfabs.legal_entity_foreign_city,
        tfabs.legal_entity_foreign_descr,
        tfabs.legal_entity_foreign_posta,
        tfabs.legal_entity_foreign_provi,
        sf.concat(
            rl_state_lookup.recipient_location_state_fips,
            sf.coalesce(tfpds.legal_entity_county_code, tfabs.legal_entity_county_code),
        ).alias("recipient_location_county_fips"),
    ]

    place_of_performance_cols = [
        tfabs.place_of_performance_code,
        tfabs.place_of_performance_scope,
        sf.coalesce(tfpds.place_of_perform_country_c, tfabs.place_of_perform_country_c).alias("pop_country_code"),
        sf.coalesce(tfpds.place_of_perf_country_desc, tfabs.place_of_perform_country_n).alias("pop_country_name"),
        sf.coalesce(tfpds.place_of_performance_state, tfabs.place_of_perfor_state_code).alias("pop_state_code"),
        sf.coalesce(tfpds.place_of_perfor_state_desc, tfabs.place_of_perform_state_nam).alias("pop_state_name"),
        pop_state_lookup.pop_state_fips,
        pop_state_population.pop_state_population,
        extract_numbers_as_string(
            sf.coalesce(tfpds.place_of_perform_county_co, tfabs.place_of_perform_county_co), 3
        ).alias("pop_county_code"),
        sf.coalesce(tfpds.place_of_perform_county_na, tfabs.place_of_perform_county_na).alias("pop_county_name"),
        pop_county_population.pop_county_population,
        extract_numbers_as_string(
            sf.coalesce(tfpds.place_of_performance_congr, tfabs.place_of_performance_congr)
        ).alias("pop_congressional_code"),
        pop_district_population.pop_congressional_population,
        current_cd.pop_congressional_code_current,
        sf.coalesce(tfpds.place_of_performance_zip5, tfabs.place_of_performance_zip5).alias("pop_zip5"),
        sf.coalesce(tfpds.place_of_performance_zip4a, tfabs.place_of_performance_zip4a).alias(
            "place_of_performance_zip4a"
        ),
        sf.coalesce(tfpds.place_of_perform_zip_last4, tfabs.place_of_perform_zip_last4).alias(
            "place_of_perform_zip_last4"
        ),
        sf.rtrim(sf.coalesce(tfpds.place_of_perform_city_name, tfabs.place_of_performance_city)).alias("pop_city_name"),
        tfabs.place_of_performance_forei,
        sf.concat(
            pop_state_lookup.pop_state_fips,
            sf.coalesce(tfpds.place_of_perform_county_co, tfabs.place_of_perform_county_co),
        ).alias("pop_county_fips"),
    ]

    accounts_cols = [
        fed_and_tres_acct.treasury_account_identifiers,
        fed_and_tres_acct.tas_paths,
        fed_and_tres_acct.tas_components,
        fed_and_tres_acct.federal_accounts,
        fed_and_tres_acct.disaster_emergency_fund_codes,
    ]

    officer_amounts_cols = [
        sf.coalesce(tfabs.officer_1_name, tfpds.officer_1_name).alias("officer_1_name"),
        sf.coalesce(tfabs.officer_1_amount, tfpds.officer_1_amount).alias("officer_1_amount"),
        sf.coalesce(tfabs.officer_2_name, tfpds.officer_2_name).alias("officer_2_name"),
        sf.coalesce(tfabs.officer_2_amount, tfpds.officer_2_amount).alias("officer_2_amount"),
        sf.coalesce(tfabs.officer_3_name, tfpds.officer_3_name).alias("officer_3_name"),
        sf.coalesce(tfabs.officer_3_amount, tfpds.officer_3_amount).alias("officer_3_amount"),
        sf.coalesce(tfabs.officer_4_name, tfpds.officer_4_name).alias("officer_4_name"),
        sf.coalesce(tfabs.officer_4_amount, tfpds.officer_4_amount).alias("officer_4_amount"),
        sf.coalesce(tfabs.officer_5_name, tfpds.officer_5_name).alias("officer_5_name"),
        sf.coalesce(tfabs.officer_5_amount, tfpds.officer_5_amount).alias("officer_5_amount"),
    ]

    fabs_cols = [
        tfabs.published_fabs_id,
        tfabs.afa_generated_unique,
        tfabs.business_funds_ind_desc,
        tfabs.business_funds_indicator,
        tfabs.business_types,
        tfabs.business_types_desc,
        tfabs.cfda_number,
        tfabs.cfda_title,
        rcfda.id.alias("cfda_id"),
        tfabs.correction_delete_indicatr,
        tfabs.correction_delete_ind_desc,
        awards.fain,
        tfabs.funding_opportunity_goals,
        tfabs.funding_opportunity_number,
        tfabs.record_type,
        tfabs.record_type_description,
        tfabs.sai_number,
        awards.uri,
    ]

    fpds_cols = [
        tfpds.detached_award_procurement_id,
        tfpds.detached_award_proc_unique,
        tfpds.a_76_fair_act_action,
        tfpds.a_76_fair_act_action_desc,
        tfpds.agency_id,
        tfpds.airport_authority,
        tfpds.alaskan_native_owned_corpo,
        tfpds.alaskan_native_servicing_i,
        tfpds.american_indian_owned_busi,
        tfpds.asian_pacific_american_own,
        tfpds.base_and_all_options_value,
        tfpds.base_exercised_options_val,
        tfpds.black_american_owned_busin,
        tfpds.c1862_land_grant_college,
        tfpds.c1890_land_grant_college,
        tfpds.c1994_land_grant_college,
        tfpds.c8a_program_participant,
        tfpds.cage_code,
        tfpds.city_local_government,
        tfpds.clinger_cohen_act_planning,
        tfpds.clinger_cohen_act_pla_desc,
        tfpds.commercial_item_acqui_desc,
        tfpds.commercial_item_acquisitio,
        tfpds.commercial_item_test_desc,
        tfpds.commercial_item_test_progr,
        tfpds.community_developed_corpor,
        tfpds.community_development_corp,
        tfpds.consolidated_contract,
        tfpds.consolidated_contract_desc,
        tfpds.construction_wage_rat_desc,
        tfpds.construction_wage_rate_req,
        tfpds.contingency_humanitar_desc,
        tfpds.contingency_humanitarian_o,
        tfpds.contract_award_type,
        tfpds.contract_award_type_desc,
        tfpds.contract_bundling,
        tfpds.contract_bundling_descrip,
        tfpds.contract_financing,
        tfpds.contract_financing_descrip,
        tfpds.contracting_officers_desc,
        tfpds.contracting_officers_deter,
        tfpds.contracts,
        tfpds.corporate_entity_not_tax_e,
        tfpds.corporate_entity_tax_exemp,
        tfpds.cost_accounting_stand_desc,
        tfpds.cost_accounting_standards,
        tfpds.cost_or_pricing_data,
        tfpds.cost_or_pricing_data_desc,
        tfpds.council_of_governments,
        tfpds.country_of_product_or_desc,
        tfpds.country_of_product_or_serv,
        tfpds.county_local_government,
        tfpds.current_total_value_award,
        tfpds.dod_claimant_prog_cod_desc,
        tfpds.dod_claimant_program_code,
        tfpds.domestic_or_foreign_e_desc,
        tfpds.domestic_or_foreign_entity,
        tfpds.domestic_shelter,
        tfpds.dot_certified_disadvantage,
        tfpds.economically_disadvantaged,
        tfpds.educational_institution,
        tfpds.emerging_small_business,
        tfpds.epa_designated_produc_desc,
        tfpds.epa_designated_product,
        tfpds.evaluated_preference,
        tfpds.evaluated_preference_desc,
        tfpds.extent_competed,
        tfpds.extent_compete_description,
        tfpds.fair_opportunity_limi_desc,
        tfpds.fair_opportunity_limited_s,
        tfpds.fed_biz_opps,
        tfpds.fed_biz_opps_description,
        tfpds.federal_agency,
        tfpds.federally_funded_research,
        tfpds.for_profit_organization,
        tfpds.foreign_funding,
        tfpds.foreign_funding_desc,
        tfpds.foreign_government,
        tfpds.foreign_owned_and_located,
        tfpds.foundation,
        tfpds.government_furnished_desc,
        tfpds.government_furnished_prope,
        tfpds.grants,
        tfpds.hispanic_american_owned_bu,
        tfpds.hispanic_servicing_institu,
        tfpds.historically_black_college,
        tfpds.historically_underutilized,
        tfpds.hospital_flag,
        tfpds.housing_authorities_public,
        tfpds.idv_type,
        tfpds.idv_type_description,
        tfpds.indian_tribe_federally_rec,
        tfpds.information_technolog_desc,
        tfpds.information_technology_com,
        tfpds.inherently_government_desc,
        tfpds.inherently_government_func,
        tfpds.inter_municipal_local_gove,
        tfpds.interagency_contract_desc,
        tfpds.interagency_contracting_au,
        tfpds.international_organization,
        tfpds.interstate_entity,
        tfpds.joint_venture_economically,
        tfpds.joint_venture_women_owned,
        tfpds.labor_standards,
        tfpds.labor_standards_descrip,
        tfpds.labor_surplus_area_firm,
        tfpds.limited_liability_corporat,
        tfpds.local_area_set_aside,
        tfpds.local_area_set_aside_desc,
        tfpds.local_government_owned,
        tfpds.major_program,
        tfpds.manufacturer_of_goods,
        tfpds.materials_supplies_article,
        tfpds.materials_supplies_descrip,
        tfpds.minority_institution,
        tfpds.minority_owned_business,
        tfpds.multi_year_contract,
        tfpds.multi_year_contract_desc,
        tfpds.multiple_or_single_aw_desc,
        tfpds.multiple_or_single_award_i,
        tfpds.municipality_local_governm,
        tfpds.naics.alias("naics_code"),
        tfpds.naics_description.alias("naics_description"),
        tfpds.national_interest_action,
        tfpds.national_interest_desc,
        tfpds.native_american_owned_busi,
        tfpds.native_hawaiian_owned_busi,
        tfpds.native_hawaiian_servicing,
        tfpds.nonprofit_organization,
        tfpds.number_of_actions,
        tfpds.number_of_offers_received,
        tfpds.ordering_period_end_date,
        tfpds.organizational_type,
        tfpds.other_minority_owned_busin,
        tfpds.other_not_for_profit_organ,
        tfpds.other_statutory_authority,
        tfpds.other_than_full_and_o_desc,
        tfpds.other_than_full_and_open_c,
        tfpds.parent_award_id,
        tfpds.partnership_or_limited_lia,
        tfpds.performance_based_se_desc,
        tfpds.performance_based_service,
        tfpds.period_of_perf_potential_e,
        awards.piid,
        tfpds.place_of_manufacture,
        tfpds.place_of_manufacture_desc,
        tfpds.planning_commission,
        tfpds.port_authority,
        tfpds.potential_total_value_awar,
        tfpds.price_evaluation_adjustmen,
        tfpds.private_university_or_coll,
        tfpds.product_or_service_code,
        tfpds.product_or_service_co_desc.alias("product_or_service_description"),
        tfpds.program_acronym,
        tfpds.program_system_or_equ_desc,
        tfpds.program_system_or_equipmen,
        tfpds.pulled_from,
        tfpds.purchase_card_as_paym_desc,
        tfpds.purchase_card_as_payment_m,
        tfpds.receives_contracts_and_gra,
        tfpds.recovered_materials_s_desc,
        tfpds.recovered_materials_sustai,
        tfpds.referenced_idv_agency_desc,
        tfpds.referenced_idv_agency_iden,
        tfpds.referenced_idv_modificatio,
        tfpds.referenced_idv_type,
        tfpds.referenced_idv_type_desc,
        tfpds.referenced_mult_or_si_desc,
        tfpds.referenced_mult_or_single,
        tfpds.research,
        tfpds.research_description,
        tfpds.sam_exception,
        tfpds.sam_exception_description,
        tfpds.sba_certified_8_a_joint_ve,
        tfpds.school_district_local_gove,
        tfpds.school_of_forestry,
        tfpds.sea_transportation,
        tfpds.sea_transportation_desc,
        tfpds.self_certified_small_disad,
        tfpds.service_disabled_veteran_o,
        tfpds.small_agricultural_coopera,
        tfpds.small_business_competitive,
        tfpds.small_disadvantaged_busine,
        tfpds.sole_proprietorship,
        tfpds.solicitation_date,
        tfpds.solicitation_identifier,
        tfpds.solicitation_procedur_desc,
        tfpds.solicitation_procedures,
        tfpds.state_controlled_instituti,
        tfpds.subchapter_s_corporation,
        tfpds.subcontinent_asian_asian_i,
        tfpds.subcontracting_plan,
        tfpds.subcontracting_plan_desc,
        tfpds.the_ability_one_program,
        tfpds.total_obligated_amount,
        tfpds.township_local_government,
        tfpds.transaction_number,
        tfpds.transit_authority,
        tfpds.tribal_college,
        tfpds.tribally_owned_business,
        tfpds.type_of_contract_pricing,
        tfpds.type_of_contract_pric_desc,
        tfpds.type_of_idc,
        tfpds.type_of_idc_description,
        tfpds.type_set_aside,
        tfpds.type_set_aside_description,
        tfpds.undefinitized_action,
        tfpds.undefinitized_action_desc,
        tfpds.us_federal_government,
        tfpds.us_government_entity,
        tfpds.us_local_government,
        tfpds.us_state_government,
        tfpds.us_tribal_government,
        tfpds.vendor_doing_as_business_n,
        tfpds.vendor_fax_number,
        tfpds.vendor_phone_number,
        tfpds.veteran_owned_business,
        tfpds.veterinary_college,
        tfpds.veterinary_hospital,
        tfpds.woman_owned_business,
        tfpds.women_owned_small_business,
        fed_and_tres_acct.program_activities.cast(StringType()),
    ]

    df = (
        tn.join(tfabs, (tn.id == tfabs.transaction_id) & ~tn.is_fpds, "leftouter")
        .join(tfpds, (tn.id == tfpds.transaction_id) & tn.is_fpds, "leftouter")
        .join(rcfda, tfabs.cfda_number == rcfda.program_number, "leftouter")
        .join(rlu, rlu.recipient_hash == recipient_hash, "leftouter")
        .join(awards, tn.award_id == awards.id, "leftouter")
        .join(aa, tn.awarding_agency_id == aa.id, "leftouter")
        .join(taa, aa.toptier_agency_id == taa.toptier_agency_id, "leftouter")
        .join(saa, aa.subtier_agency_id == saa.subtier_agency_id, "leftouter")
        .join(aa_id, ((aa_id.toptier_agency_id == taa.toptier_agency_id) & aa_id.toptier_flag), "leftouter")
        .join(fa, tn.funding_agency_id == fa.id, "leftouter")
        .join(tfa, fa.funding_toptier_agency_id == tfa.toptier_agency_id, "leftouter")
        .join(sfa, fa.funding_subtier_agency_id == sfa.subtier_agency_id, "leftouter")
        .join(fa_id, ((fa_id.toptier_agency_id == tfa.funding_toptier_agency_id) & (fa_id.row_num == 1)), "leftouter")
        .join(prl, prl.parent_recipient_hash == parent_recipient_hash, "leftouter")
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
            pop_state_population,
            (pop_state_population.state_code == pop_state_lookup.pop_state_fips)
            & (pop_state_population.county_number == "000"),
            "leftouter",
        )
        .join(
            pop_county_population,
            (pop_county_population.state_code == pop_state_lookup.pop_state_fips)
            & (
                pop_county_population.county_number
                == extract_numbers_as_string(
                    sf.coalesce(tfpds.place_of_perform_county_co, tfabs.place_of_perform_county_co),
                    3,
                )
            ),
            "leftouter",
        )
        .join(
            pop_district_population,
            (pop_district_population.state_code == pop_state_lookup.pop_state_fips)
            & (
                pop_district_population.congressional_district
                == extract_numbers_as_string(
                    sf.coalesce(tfpds.place_of_performance_congr, tfabs.place_of_performance_congr)
                )
            ),
            "leftouter",
        )
        .join(
            rl_state_lookup,
            rl_state_lookup.rl_state_code == sf.coalesce(tfpds.legal_entity_state_code, tfabs.legal_entity_state_code),
            "leftouter",
        )
        .join(
            rl_state_population,
            (rl_state_population.state_code == rl_state_lookup.recipient_location_state_fips)
            & (rl_state_population.county_number == "000"),
            "leftouter",
        )
        .join(
            rl_county_population,
            (rl_county_population.state_code == rl_state_lookup.recipient_location_state_fips)
            & (
                rl_county_population.county_number
                == extract_numbers_as_string(
                    sf.coalesce(tfpds.place_of_perform_county_co, tfabs.place_of_perform_county_co),
                    3,
                )
            ),
            "leftouter",
        )
        .join(
            rl_district_population,
            (rl_district_population.state_code == rl_state_lookup.recipient_location_state_fips)
            & (
                rl_district_population.congressional_district
                == extract_numbers_as_string(
                    sf.coalesce(tfpds.place_of_performance_congr, tfabs.place_of_performance_congr),
                )
            ),
            "leftouter",
        )
        .join(current_cd, tn.id == current_cd.transaction_id, "leftouter")
        .join(ao, ao.office_code == sf.coalesce(tfabs.awarding_office_code, tfpds.awarding_office_code), "leftouter")
        .join(fo, fo.office_code == sf.coalesce(tfabs.funding_office_code, tfpds.funding_office_code), "leftouter")
        .join(fed_and_tres_acct, fed_and_tres_acct.award_id == tn.award_id, "leftouter")
        .select(
            *key_cols,
            *date_cols,
            *agency_cols,
            *typing_cols,
            *amounts_cols,
            *recipient_cols,
            *recipient_location_cols,
            *place_of_performance_cols,
            *accounts_cols,
            *officer_amounts_cols,
            *fabs_cols,
            *fpds_cols,
        )
        .withColumn("merge_hash_key", sf.xxhash64("*"))
    )
    return df


def load_transaction_search(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    df = transaction_search_dataframe(spark)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{destination_database}.{destination_table_name}")


def load_transaction_search_incremental(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = transaction_search_dataframe(spark).alias("s")
    (
        target.merge(source, "s.transaction_id = t.transaction_id and s.merge_hash_key = t.merge_hash_key")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
