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


class TransactionSearchDataframe:

    def __init__(self, spark: SparkSession):
        # Base Tables
        self.transaction_normalized = spark.table("int.transaction_normalized")
        self.transaction_fpds = spark.table("int.transaction_fpds")
        self.transaction_fabs = spark.table("int.transaction_fabs")
        self.awards = spark.table("int.awards")
        self.references_cfda = spark.table("global_temp.references_cfda")
        self.recipient_lookup = spark.table("rpt.recipient_lookup")
        self.parent_recipient = spark.table("rpt.recipient_lookup").select(
            sf.col("recipient_hash").alias("parent_recipient_hash"),
            sf.col("legal_business_name").alias("parent_recipient_name"),
        )
        self.recipient_profile = spark.table("rpt.recipient_profile")
        self.awarding_agency = (
            spark.table("global_temp.agency")
            .withColumn("awarding_toptier_agency_id", sf.col("toptier_agency_id"))
            .withColumn("funding_subtier_agency_id", sf.col("subtier_agency_id"))
        )
        self.awarding_toptier_agency = (
            spark.table("global_temp.toptier_agency")
            .withColumn("awarding_toptier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("awarding_toptier_agency_name", sf.col("name"))
        )
        self.awarding_subtier_agency = (
            spark.table("global_temp.subtier_agency")
            .withColumn("awarding_subtier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("awarding_subtier_agency_name", sf.col("name"))
        )
        self.awarding_agency_id = spark.table("global_temp.agency").withColumn(
            "awarding_toptier_agency_id", sf.col("id")
        )
        self.funding_agency = (
            spark.table("global_temp.agency")
            .withColumn("funding_toptier_agency_id", sf.col("toptier_agency_id"))
            .withColumn("funding_subtier_agency_id", sf.col("subtier_agency_id"))
        )
        self.funding_toptier_agency = (
            spark.table("global_temp.toptier_agency")
            .alias("funding_toptier_agency")
            .withColumn("funding_toptier_agency_id", sf.col("toptier_agency_id"))
            .withColumn("funding_toptier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("funding_toptier_agency_name", sf.col("name"))
        )
        self.funding_subtier_agency = (
            spark.table("global_temp.subtier_agency")
            .withColumn("funding_subtier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("funding_subtier_agency_name", sf.col("name"))
        )
        w = Window.partitionBy(self.funding_agency.toptier_agency_id).orderBy(
            self.funding_agency.toptier_flag.desc(), self.funding_agency.id.asc()
        )
        self.funding_agency_id = (
            spark.table("global_temp.agency")
            .withColumn("funding_toptier_agency_id", sf.col("id"))
            .withColumn("row_num", sf.row_number().over(w))
        )
        self.awarding_office = spark.table("global_temp.office").withColumn(
            "awarding_office_name", sf.col("office_name")
        )
        self.funding_office = spark.table("global_temp.office").withColumn("funding_office_name", sf.col("office_name"))
        self.state_data = spark.table("global_temp.state_data")
        ref_population_county = spark.table("global_temp.ref_population_county")
        self.pop_state_population = ref_population_county.alias("pop_state_population").withColumn(
            "pop_state_population", ref_population_county.latest_population
        )
        self.pop_county_population = ref_population_county.alias("pop_county_population").withColumn(
            "pop_county_population", ref_population_county.latest_population
        )
        self.rl_state_population = ref_population_county.alias("rl_state_population").withColumn(
            "recipient_location_state_population", ref_population_county.latest_population
        )
        self.rl_county_population = ref_population_county.alias("rl_county_population").withColumn(
            "recipient_location_county_population", ref_population_county.latest_population
        )
        ref_population_cong_district = spark.table("global_temp.ref_population_cong_district")
        self.pop_district_population = ref_population_cong_district.alias("pop_district_population").withColumn(
            "pop_congressional_population", ref_population_cong_district.latest_population
        )
        self.rl_district_population = ref_population_cong_district.alias("rl_district_population").withColumn(
            "recipient_location_congressional_population", ref_population_cong_district.latest_population
        )
        self.current_cd = spark.table("int.transaction_current_cd_lookup")
        self.faba = spark.table("int.financial_accounts_by_awards")
        self.federal_account = spark.table("global_temp.federal_account")
        self.treasury_appropriation_account = spark.table("global_temp.treasury_appropriation_account")
        self.ref_program_activity = spark.table("global_temp.ref_program_activity")
        self.program_activity_park = spark.table("global_temp.program_activity_park")

    @property
    def recipient_hash_and_levels(self) -> DataFrame:
        return (
            self.recipient_profile.groupBy("recipient_hash", "uei")
            .agg(sf.sort_array(sf.collect_set("recipient_level")).alias("recipient_levels"))
            .select(
                sf.col("recipient_hash").alias("recipient_level_hash"),
                sf.col("recipient_levels"),
            )
        )

    @property
    def pop_state_lookup(self) -> DataFrame:
        return (
            self.state_data.groupBy("code", "name", "fips")
            .agg(sf.max(sf.col("id")))
            .select(
                sf.col("code").alias("pop_state_code"),
                sf.col("fips").alias("pop_state_fips"),
            )
        )

    @property
    def tas_shared(self) -> Column:
        return sf.concat(
            sf.lit("aid="),
            sf.coalesce(self.treasury_appropriation_account.agency_id, sf.lit("")),
            sf.lit("main="),
            sf.coalesce(self.treasury_appropriation_account.main_account_code, sf.lit("")),
            sf.lit("ata="),
            sf.coalesce(self.treasury_appropriation_account.allocation_transfer_agency_id, sf.lit("")),
            sf.lit("sub="),
            sf.coalesce(self.treasury_appropriation_account.sub_account_code, sf.lit("")),
            sf.lit("bpoa="),
            sf.coalesce(self.treasury_appropriation_account.beginning_period_of_availability, sf.lit("")),
            sf.lit("epoa="),
            sf.coalesce(self.treasury_appropriation_account.ending_period_of_availability, sf.lit("")),
            sf.lit("a="),
            sf.coalesce(self.treasury_appropriation_account.availability_type_code, sf.lit("")),
        )

    @property
    def fed_and_tres_acct(self) -> DataFrame:
        return (
            self.faba.join(
                self.treasury_appropriation_account,
                self.treasury_appropriation_account.treasury_account_identifier == self.faba.treasury_account_id,
                "inner",
            )
            .join(
                self.federal_account,
                self.federal_account.id == self.treasury_appropriation_account.federal_account_id,
                "inner",
            )
            .join(
                self.awarding_toptier_agency,
                self.federal_account.parent_toptier_agency_id == self.awarding_toptier_agency.toptier_agency_id,
                "inner",
            )
            .join(self.ref_program_activity, self.faba.program_activity_id == self.ref_program_activity.id, "left")
            .join(
                self.program_activity_park,
                self.faba.program_activity_reporting_key == self.program_activity_park.code,
                "left",
            )
            .filter(self.faba["award_id"].isNotNull())
            .groupBy(self.faba.award_id)
            .agg(
                sf.to_json(
                    sf.sort_array(
                        sf.collect_set(
                            sf.struct(
                                self.federal_account.id.alias("id"),
                                self.federal_account.account_title.alias("account_title"),
                                self.federal_account.federal_account_code.alias("federal_account_code"),
                            )
                        )
                    )
                ).alias("federal_accounts"),
                sf.when(
                    sf.size(sf.collect_set(self.faba.disaster_emergency_fund_code)) > 0,
                    sf.sort_array(sf.collect_set(self.faba.disaster_emergency_fund_code)),
                )
                .otherwise(None)
                .alias("disaster_emergency_fund_codes"),
                sf.sort_array(sf.collect_set(self.treasury_appropriation_account.treasury_account_identifier)).alias(
                    "treasury_account_identifiers"
                ),
                sf.sort_array(
                    sf.collect_set(
                        sf.concat(
                            sf.lit("agency="),
                            sf.coalesce(self.awarding_toptier_agency.toptier_code, sf.lit("")),
                            sf.lit("faaid="),
                            sf.coalesce(self.federal_account.agency_identifier, sf.lit("")),
                            sf.lit("famain="),
                            sf.coalesce(self.federal_account.main_account_code, sf.lit("")),
                            self.tas_shared,
                        )
                    )
                ).alias("tas_paths"),
                sf.sort_array(sf.collect_set(self.tas_shared)).alias("tas_components"),
                sf.sort_array(
                    sf.collect_set(
                        sf.to_json(
                            sf.struct(
                                sf.coalesce(
                                    self.program_activity_park.name,
                                    sf.upper(self.ref_program_activity.program_activity_name),
                                ).alias("name"),
                                sf.coalesce(
                                    self.program_activity_park.code,
                                    sf.lpad(self.ref_program_activity.program_activity_code, 4, "0"),
                                ).alias("code"),
                                sf.when(
                                    self.program_activity_park["code"].isNotNull(),
                                    sf.lit("PARK"),
                                )
                                .otherwise(sf.lit("PAC/PAN"))
                                .alias("type"),
                            )
                        )
                    )
                ).alias("program_activities"),
            )
        )

    @property
    def rl_state_lookup(self) -> DataFrame:
        return (
            self.state_data.groupBy("code", "name", "fips")
            .agg(sf.max(sf.col("id")))
            .select(
                sf.col("code").alias("rl_state_code"),
                sf.col("fips").alias("recipient_location_state_fips"),
            )
        )

    @property
    def key_cols(self) -> list[Column]:
        return [
            self.transaction_normalized.id.alias("transaction_id"),
            self.transaction_normalized.award_id,
            self.transaction_normalized.transaction_unique_id,
            self.transaction_normalized.usaspending_unique_transaction_id,
            self.transaction_normalized.modification_number,
            self.awards.generated_unique_award_id,
        ]

    @property
    def date_cols(self) -> list[Column]:
        return [
            sf.to_date(self.transaction_normalized.action_date).alias("action_date"),
            sf.add_months(sf.to_date(self.transaction_normalized.action_date), 3).alias("fiscal_action_date"),
            sf.to_date(self.transaction_normalized.last_modified_date).alias("last_modified_date"),
            self.transaction_normalized.fiscal_year,
            self.awards.certified_date.alias("award_certified_date"),
            sf.year(sf.add_months(sf.to_date(self.awards.certified_date), 3)).alias("award_fiscal_year"),
            self.transaction_normalized.create_date.cast(TimestampType()),
            self.transaction_normalized.update_date.cast(TimestampType()),
            self.awards.update_date.cast(TimestampType()).alias("award_update_date"),
            sf.to_date(self.awards.date_signed).alias("award_date_signed"),
            sf.greatest(sf.to_timestamp(self.transaction_normalized.update_date), self.awards.update_date).alias(
                "etl_update_date"
            ),
            sf.to_date(self.transaction_normalized.period_of_performance_start_date).alias(
                "period_of_performance_start_date"
            ),
            sf.to_date(self.transaction_normalized.period_of_performance_current_end_date).alias(
                "period_of_performance_current_end_date"
            ),
            sf.coalesce(
                sf.to_date(self.transaction_fabs.created_at),
                sf.to_date(self.transaction_fpds.initial_report_date),
            ).alias("initial_report_date"),
        ]

    @property
    def agency_cols(self) -> list[Column]:
        return [
            sf.coalesce(self.transaction_fabs.awarding_agency_code, self.transaction_fpds.awarding_agency_code).alias(
                "awarding_agency_code"
            ),
            self.awarding_toptier_agency.awarding_toptier_agency_name,
            sf.coalesce(self.transaction_fabs.awarding_agency_name, self.transaction_fpds.awarding_agency_name).alias(
                "awarding_toptier_agency_name_raw"
            ),
            sf.coalesce(self.transaction_fabs.funding_agency_code, self.transaction_fpds.funding_agency_code).alias(
                "funding_agency_code"
            ),
            self.funding_toptier_agency.funding_toptier_agency_name,
            sf.coalesce(self.transaction_fabs.funding_agency_name, self.transaction_fpds.funding_agency_name).alias(
                "funding_toptier_agency_name_raw"
            ),
            sf.coalesce(
                self.transaction_fabs.awarding_sub_tier_agency_c, self.transaction_fpds.awarding_sub_tier_agency_c
            ).alias("awarding_sub_tier_agency_c"),
            self.awarding_subtier_agency.awarding_subtier_agency_name,
            sf.coalesce(
                self.transaction_fabs.awarding_sub_tier_agency_n, self.transaction_fpds.awarding_sub_tier_agency_n
            ).alias("awarding_subtier_agency_name_raw"),
            sf.coalesce(
                self.transaction_fabs.funding_sub_tier_agency_co, self.transaction_fpds.funding_sub_tier_agency_co
            ).alias("funding_sub_tier_agency_co"),
            self.funding_subtier_agency.funding_subtier_agency_name,
            sf.coalesce(
                self.transaction_fabs.funding_sub_tier_agency_na, self.transaction_fpds.funding_sub_tier_agency_na
            ).alias("funding_subtier_agency_name_raw"),
            self.awarding_agency_id.awarding_toptier_agency_id,
            self.funding_agency_id.funding_toptier_agency_id,
            self.transaction_normalized.awarding_agency_id,
            self.transaction_normalized.funding_agency_id,
            self.awarding_toptier_agency.awarding_toptier_agency_abbreviation,
            self.funding_toptier_agency.funding_toptier_agency_abbreviation,
            self.awarding_subtier_agency.awarding_subtier_agency_abbreviation,
            self.funding_subtier_agency.funding_subtier_agency_abbreviation,
            sf.coalesce(self.transaction_fabs.awarding_office_code, self.transaction_fpds.awarding_office_code).alias(
                "awarding_office_code"
            ),
            sf.coalesce(
                self.awarding_office.awarding_office_name,
                self.transaction_fabs.awarding_office_name,
                self.transaction_fpds.awarding_office_name,
            ).alias("awarding_office_name"),
            sf.coalesce(self.transaction_fabs.funding_office_code, self.transaction_fpds.funding_office_code).alias(
                "funding_office_code"
            ),
            sf.coalesce(
                self.funding_office.funding_office_name,
                self.transaction_fabs.funding_office_name,
                self.transaction_fpds.funding_office_name,
            ).alias("funding_office_name"),
        ]

    @property
    def typing_cols(self) -> list[Column]:
        return [
            self.transaction_normalized.is_fpds,
            self.transaction_normalized.type.alias("type_raw"),
            self.transaction_normalized.type_description.alias("type_description_raw"),
            sf.when(
                ~self.transaction_normalized["type"].isin(ALL_AWARD_TYPES)
                | self.transaction_normalized["type"].isNull(),
                "-1",
            )
            .otherwise(self.transaction_normalized.type)
            .alias("type"),
            sf.when(
                ~self.transaction_normalized["type"].isin(ALL_AWARD_TYPES)
                | self.transaction_normalized["type"].isNull(),
                "NOT SPECIFIED",
            )
            .otherwise(self.transaction_normalized.type_description)
            .alias("type_description"),
            self.transaction_normalized.action_type,
            self.transaction_normalized.action_type_description,
            self.awards.category.alias("award_category"),
            self.transaction_normalized.description.alias("transaction_description"),
            self.transaction_normalized.business_categories,
        ]

    @property
    def amounts_cols(self) -> list[Column]:
        return [
            sf.coalesce(
                sf.when(
                    self.transaction_normalized["type"].isin(["07", "08"]), self.awards.total_subsidy_cost
                ).otherwise(self.awards.total_obligation),
                sf.lit(0),
            )
            .cast(DecimalType(23, 2))
            .alias("award_amount"),
            sf.coalesce(
                sf.when(
                    self.transaction_normalized["type"].isin(["07", "08"]),
                    self.transaction_normalized.original_loan_subsidy_cost,
                ).otherwise(self.transaction_normalized.federal_action_obligation),
                sf.lit(0),
            )
            .cast(DecimalType(23, 2))
            .alias("generated_pragmatic_obligation"),
            sf.coalesce(self.transaction_normalized.federal_action_obligation, sf.lit(0))
            .cast(DecimalType(23, 2))
            .alias("federal_action_obligation"),
            sf.coalesce(self.transaction_normalized.original_loan_subsidy_cost, sf.lit(0))
            .cast(DecimalType(23, 2))
            .alias("original_loan_subsidy_cost"),
            sf.coalesce(self.transaction_normalized.face_value_loan_guarantee, sf.lit(0))
            .cast(DecimalType(23, 2))
            .alias("face_value_loan_guarantee"),
            self.transaction_normalized.indirect_federal_sharing.cast(DecimalType(23, 2)),
            self.transaction_normalized.funding_amount,
            sf.coalesce(self.transaction_fabs.total_funding_amount, sf.lit("0"))
            .cast(DecimalType(23, 2))
            .alias("total_funding_amount"),
            self.transaction_normalized.non_federal_funding_amount,
        ]

    @property
    def generated_recipient_hash(self) -> Column:
        return hash_col(
            sf.when(
                sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei).isNotNull(),
                sf.concat(
                    sf.lit("uei-"),
                    sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei),
                ),
            ).otherwise(
                sf.when(
                    sf.coalesce(
                        self.transaction_fpds.awardee_or_recipient_uniqu,
                        self.transaction_fabs.awardee_or_recipient_uniqu,
                    ).isNotNull(),
                    sf.concat(
                        sf.lit("duns-"),
                        sf.coalesce(
                            self.transaction_fpds.awardee_or_recipient_uniqu,
                            self.transaction_fabs.awardee_or_recipient_uniqu,
                        ),
                    ),
                ).otherwise(
                    sf.concat(
                        sf.lit("name-"),
                        sf.coalesce(
                            self.transaction_fpds.awardee_or_recipient_legal,
                            self.transaction_fabs.awardee_or_recipient_legal,
                        ),
                    )
                )
            )
        )

    @property
    def generated_parent_recipient_hash(self) -> Column:
        return hash_col(
            sf.when(
                sf.coalesce(
                    self.transaction_fpds.ultimate_parent_uei, self.transaction_fabs.ultimate_parent_uei
                ).isNotNull(),
                sf.concat(
                    sf.lit("uei-"),
                    sf.coalesce(self.transaction_fpds.ultimate_parent_uei, self.transaction_fabs.ultimate_parent_uei),
                ),
            ).otherwise(
                sf.when(
                    sf.coalesce(
                        self.transaction_fpds.ultimate_parent_unique_ide,
                        self.transaction_fabs.ultimate_parent_unique_ide,
                    ).isNotNull(),
                    sf.concat(
                        sf.lit("duns-"),
                        sf.coalesce(
                            self.transaction_fpds.ultimate_parent_unique_ide,
                            self.transaction_fabs.ultimate_parent_unique_ide,
                        ),
                    ),
                ).otherwise(
                    sf.concat(
                        sf.lit("name-"),
                        sf.coalesce(
                            self.transaction_fpds.ultimate_parent_legal_enti,
                            self.transaction_fabs.ultimate_parent_legal_enti,
                        ),
                    )
                )
            )
        )

    @property
    def recipient_cols(self) -> list[Column]:
        return [
            sf.coalesce(
                self.recipient_lookup.recipient_hash,
                hash_col(
                    sf.when(
                        sf.coalesce(
                            self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei
                        ).isNotNull(),
                        sf.concat(
                            sf.lit("uei-"),
                            sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei),
                        ),
                    ).otherwise(
                        sf.when(
                            sf.coalesce(
                                self.transaction_fpds.awardee_or_recipient_uniqu,
                                self.transaction_fabs.awardee_or_recipient_uniqu,
                            ).isNotNull(),
                            sf.concat(
                                sf.lit("duns-"),
                                sf.coalesce(
                                    self.transaction_fpds.awardee_or_recipient_uniqu,
                                    self.transaction_fabs.awardee_or_recipient_uniqu,
                                ),
                            ),
                        ).otherwise(
                            sf.concat(
                                sf.lit("name-"),
                                sf.coalesce(
                                    self.transaction_fpds.awardee_or_recipient_legal,
                                    self.transaction_fabs.awardee_or_recipient_legal,
                                    sf.lit(""),
                                ),
                            )
                        )
                    )
                ),
            ).alias("recipient_hash"),
            self.recipient_hash_and_levels.recipient_levels,
            sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei).alias(
                "recipient_uei"
            ),
            sf.coalesce(
                self.transaction_fpds.awardee_or_recipient_legal, self.transaction_fabs.awardee_or_recipient_legal
            ).alias("recipient_name_raw"),
            sf.upper(
                sf.coalesce(
                    self.recipient_lookup.legal_business_name,
                    self.transaction_fpds.awardee_or_recipient_legal,
                    self.transaction_fabs.awardee_or_recipient_legal,
                )
            ).alias("recipient_name"),
            sf.coalesce(
                self.transaction_fpds.awardee_or_recipient_uniqu, self.transaction_fabs.awardee_or_recipient_uniqu
            ).alias("recipient_unique_id"),
            self.parent_recipient.parent_recipient_hash,
            sf.coalesce(self.transaction_fpds.ultimate_parent_uei, self.transaction_fabs.ultimate_parent_uei).alias(
                "parent_uei"
            ),
            sf.coalesce(
                self.transaction_fpds.ultimate_parent_legal_enti, self.transaction_fabs.ultimate_parent_legal_enti
            ).alias("parent_recipient_name_raw"),
            sf.upper(self.parent_recipient.parent_recipient_name).alias("parent_recipient_name"),
            sf.coalesce(
                self.transaction_fpds.ultimate_parent_unique_ide, self.transaction_fabs.ultimate_parent_unique_ide
            ).alias("parent_recipient_unique_id"),
        ]

    @property
    def recipient_location_cols(self) -> list[Column]:
        return [
            sf.coalesce(
                self.transaction_fpds.legal_entity_country_code, self.transaction_fabs.legal_entity_country_code
            ).alias("recipient_location_country_code"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_country_name, self.transaction_fabs.legal_entity_country_name
            ).alias("recipient_location_country_name"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_state_code, self.transaction_fabs.legal_entity_state_code
            ).alias("recipient_location_state_code"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_state_descrip, self.transaction_fabs.legal_entity_state_name
            ).alias("recipient_location_state_name"),
            self.rl_state_lookup.recipient_location_state_fips,
            self.rl_state_population.recipient_location_state_population,
            extract_numbers_as_string(
                sf.coalesce(
                    self.transaction_fpds.legal_entity_county_code, self.transaction_fabs.legal_entity_county_code
                ),
                3,
            ).alias("recipient_location_county_code"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_county_name, self.transaction_fabs.legal_entity_county_name
            ).alias("recipient_location_county_name"),
            self.rl_county_population.recipient_location_county_population,
            extract_numbers_as_string(
                sf.coalesce(
                    self.transaction_fpds.legal_entity_congressional, self.transaction_fabs.legal_entity_congressional
                )
            ).alias("recipient_location_congressional_code"),
            self.rl_district_population.recipient_location_congressional_population,
            self.current_cd.recipient_location_congressional_code_current.alias(
                "recipient_location_congressional_code_current"
            ),
            sf.coalesce(self.transaction_fpds.legal_entity_zip5, self.transaction_fabs.legal_entity_zip5).alias(
                "recipient_location_zip5"
            ),
            self.transaction_fpds.legal_entity_zip4,
            sf.coalesce(
                self.transaction_fpds.legal_entity_zip_last4, self.transaction_fabs.legal_entity_zip_last4
            ).alias("legal_entity_zip_last4"),
            self.transaction_fabs.legal_entity_city_code,
            sf.rtrim(
                sf.coalesce(self.transaction_fpds.legal_entity_city_name, self.transaction_fabs.legal_entity_city_name)
            ).alias("recipient_location_city_name"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_address_line1, self.transaction_fabs.legal_entity_address_line1
            ).alias("legal_entity_address_line1"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_address_line2, self.transaction_fabs.legal_entity_address_line2
            ).alias("legal_entity_address_line2"),
            sf.coalesce(
                self.transaction_fpds.legal_entity_address_line3, self.transaction_fabs.legal_entity_address_line3
            ).alias("legal_entity_address_line3"),
            self.transaction_fabs.legal_entity_foreign_city,
            self.transaction_fabs.legal_entity_foreign_descr,
            self.transaction_fabs.legal_entity_foreign_posta,
            self.transaction_fabs.legal_entity_foreign_provi,
            sf.concat(
                self.rl_state_lookup.recipient_location_state_fips,
                sf.coalesce(
                    self.transaction_fpds.legal_entity_county_code, self.transaction_fabs.legal_entity_county_code
                ),
            ).alias("recipient_location_county_fips"),
        ]

    @property
    def place_of_performance_cols(self) -> list[Column]:
        return [
            self.transaction_fabs.place_of_performance_code,
            self.transaction_fabs.place_of_performance_scope,
            sf.coalesce(
                self.transaction_fpds.place_of_perform_country_c, self.transaction_fabs.place_of_perform_country_c
            ).alias("pop_country_code"),
            sf.coalesce(
                self.transaction_fpds.place_of_perf_country_desc, self.transaction_fabs.place_of_perform_country_n
            ).alias("pop_country_name"),
            sf.coalesce(
                self.transaction_fpds.place_of_performance_state, self.transaction_fabs.place_of_perfor_state_code
            ).alias("pop_state_code"),
            sf.coalesce(
                self.transaction_fpds.place_of_perfor_state_desc, self.transaction_fabs.place_of_perform_state_nam
            ).alias("pop_state_name"),
            self.pop_state_lookup.pop_state_fips,
            self.pop_state_population.pop_state_population,
            extract_numbers_as_string(
                sf.coalesce(
                    self.transaction_fpds.place_of_perform_county_co, self.transaction_fabs.place_of_perform_county_co
                ),
                3,
            ).alias("pop_county_code"),
            sf.coalesce(
                self.transaction_fpds.place_of_perform_county_na, self.transaction_fabs.place_of_perform_county_na
            ).alias("pop_county_name"),
            self.pop_county_population.pop_county_population,
            extract_numbers_as_string(
                sf.coalesce(
                    self.transaction_fpds.place_of_performance_congr, self.transaction_fabs.place_of_performance_congr
                )
            ).alias("pop_congressional_code"),
            self.pop_district_population.pop_congressional_population,
            self.current_cd.pop_congressional_code_current,
            sf.coalesce(
                self.transaction_fpds.place_of_performance_zip5, self.transaction_fabs.place_of_performance_zip5
            ).alias("pop_zip5"),
            sf.coalesce(
                self.transaction_fpds.place_of_performance_zip4a, self.transaction_fabs.place_of_performance_zip4a
            ).alias("place_of_performance_zip4a"),
            sf.coalesce(
                self.transaction_fpds.place_of_perform_zip_last4, self.transaction_fabs.place_of_perform_zip_last4
            ).alias("place_of_perform_zip_last4"),
            sf.rtrim(
                sf.coalesce(
                    self.transaction_fpds.place_of_perform_city_name, self.transaction_fabs.place_of_performance_city
                )
            ).alias("pop_city_name"),
            self.transaction_fabs.place_of_performance_forei,
            sf.concat(
                self.pop_state_lookup.pop_state_fips,
                sf.coalesce(
                    self.transaction_fpds.place_of_perform_county_co, self.transaction_fabs.place_of_perform_county_co
                ),
            ).alias("pop_county_fips"),
        ]

    @property
    def accounts_cols(self) -> list[Column]:
        return [
            self.fed_and_tres_acct.treasury_account_identifiers,
            self.fed_and_tres_acct.tas_paths,
            self.fed_and_tres_acct.tas_components,
            self.fed_and_tres_acct.federal_accounts,
            self.fed_and_tres_acct.disaster_emergency_fund_codes,
        ]

    @property
    def officer_amounts_cols(self) -> list[Column]:
        return [
            sf.coalesce(self.transaction_fabs.officer_1_name, self.transaction_fpds.officer_1_name).alias(
                "officer_1_name"
            ),
            sf.coalesce(self.transaction_fabs.officer_1_amount, self.transaction_fpds.officer_1_amount).alias(
                "officer_1_amount"
            ),
            sf.coalesce(self.transaction_fabs.officer_2_name, self.transaction_fpds.officer_2_name).alias(
                "officer_2_name"
            ),
            sf.coalesce(self.transaction_fabs.officer_2_amount, self.transaction_fpds.officer_2_amount).alias(
                "officer_2_amount"
            ),
            sf.coalesce(self.transaction_fabs.officer_3_name, self.transaction_fpds.officer_3_name).alias(
                "officer_3_name"
            ),
            sf.coalesce(self.transaction_fabs.officer_3_amount, self.transaction_fpds.officer_3_amount).alias(
                "officer_3_amount"
            ),
            sf.coalesce(self.transaction_fabs.officer_4_name, self.transaction_fpds.officer_4_name).alias(
                "officer_4_name"
            ),
            sf.coalesce(self.transaction_fabs.officer_4_amount, self.transaction_fpds.officer_4_amount).alias(
                "officer_4_amount"
            ),
            sf.coalesce(self.transaction_fabs.officer_5_name, self.transaction_fpds.officer_5_name).alias(
                "officer_5_name"
            ),
            sf.coalesce(self.transaction_fabs.officer_5_amount, self.transaction_fpds.officer_5_amount).alias(
                "officer_5_amount"
            ),
        ]

    @property
    def fabs_cols(self) -> list[Column]:
        return [
            self.transaction_fabs.published_fabs_id,
            self.transaction_fabs.afa_generated_unique,
            self.transaction_fabs.business_funds_ind_desc,
            self.transaction_fabs.business_funds_indicator,
            self.transaction_fabs.business_types,
            self.transaction_fabs.business_types_desc,
            self.transaction_fabs.cfda_number,
            self.transaction_fabs.cfda_title,
            self.references_cfda.id.alias("cfda_id"),
            self.transaction_fabs.correction_delete_indicatr,
            self.transaction_fabs.correction_delete_ind_desc,
            self.awards.fain,
            self.transaction_fabs.funding_opportunity_goals,
            self.transaction_fabs.funding_opportunity_number,
            self.transaction_fabs.record_type,
            self.transaction_fabs.record_type_description,
            self.transaction_fabs.sai_number,
            self.awards.uri,
        ]

    @property
    def fpds_cols(self) -> list[Column]:
        return [
            self.transaction_fpds.detached_award_procurement_id,
            self.transaction_fpds.detached_award_proc_unique,
            self.transaction_fpds.a_76_fair_act_action,
            self.transaction_fpds.a_76_fair_act_action_desc,
            self.transaction_fpds.agency_id,
            self.transaction_fpds.airport_authority,
            self.transaction_fpds.alaskan_native_owned_corpo,
            self.transaction_fpds.alaskan_native_servicing_i,
            self.transaction_fpds.american_indian_owned_busi,
            self.transaction_fpds.asian_pacific_american_own,
            self.transaction_fpds.base_and_all_options_value,
            self.transaction_fpds.base_exercised_options_val,
            self.transaction_fpds.black_american_owned_busin,
            self.transaction_fpds.c1862_land_grant_college,
            self.transaction_fpds.c1890_land_grant_college,
            self.transaction_fpds.c1994_land_grant_college,
            self.transaction_fpds.c8a_program_participant,
            self.transaction_fpds.cage_code,
            self.transaction_fpds.city_local_government,
            self.transaction_fpds.clinger_cohen_act_planning,
            self.transaction_fpds.clinger_cohen_act_pla_desc,
            self.transaction_fpds.commercial_item_acqui_desc,
            self.transaction_fpds.commercial_item_acquisitio,
            self.transaction_fpds.commercial_item_test_desc,
            self.transaction_fpds.commercial_item_test_progr,
            self.transaction_fpds.community_developed_corpor,
            self.transaction_fpds.community_development_corp,
            self.transaction_fpds.consolidated_contract,
            self.transaction_fpds.consolidated_contract_desc,
            self.transaction_fpds.construction_wage_rat_desc,
            self.transaction_fpds.construction_wage_rate_req,
            self.transaction_fpds.contingency_humanitar_desc,
            self.transaction_fpds.contingency_humanitarian_o,
            self.transaction_fpds.contract_award_type,
            self.transaction_fpds.contract_award_type_desc,
            self.transaction_fpds.contract_bundling,
            self.transaction_fpds.contract_bundling_descrip,
            self.transaction_fpds.contract_financing,
            self.transaction_fpds.contract_financing_descrip,
            self.transaction_fpds.contracting_officers_desc,
            self.transaction_fpds.contracting_officers_deter,
            self.transaction_fpds.contracts,
            self.transaction_fpds.corporate_entity_not_tax_e,
            self.transaction_fpds.corporate_entity_tax_exemp,
            self.transaction_fpds.cost_accounting_stand_desc,
            self.transaction_fpds.cost_accounting_standards,
            self.transaction_fpds.cost_or_pricing_data,
            self.transaction_fpds.cost_or_pricing_data_desc,
            self.transaction_fpds.council_of_governments,
            self.transaction_fpds.country_of_product_or_desc,
            self.transaction_fpds.country_of_product_or_serv,
            self.transaction_fpds.county_local_government,
            self.transaction_fpds.current_total_value_award,
            self.transaction_fpds.dod_claimant_prog_cod_desc,
            self.transaction_fpds.dod_claimant_program_code,
            self.transaction_fpds.domestic_or_foreign_e_desc,
            self.transaction_fpds.domestic_or_foreign_entity,
            self.transaction_fpds.domestic_shelter,
            self.transaction_fpds.dot_certified_disadvantage,
            self.transaction_fpds.economically_disadvantaged,
            self.transaction_fpds.educational_institution,
            self.transaction_fpds.emerging_small_business,
            self.transaction_fpds.epa_designated_produc_desc,
            self.transaction_fpds.epa_designated_product,
            self.transaction_fpds.evaluated_preference,
            self.transaction_fpds.evaluated_preference_desc,
            self.transaction_fpds.extent_competed,
            self.transaction_fpds.extent_compete_description,
            self.transaction_fpds.fair_opportunity_limi_desc,
            self.transaction_fpds.fair_opportunity_limited_s,
            self.transaction_fpds.fed_biz_opps,
            self.transaction_fpds.fed_biz_opps_description,
            self.transaction_fpds.federal_agency,
            self.transaction_fpds.federally_funded_research,
            self.transaction_fpds.for_profit_organization,
            self.transaction_fpds.foreign_funding,
            self.transaction_fpds.foreign_funding_desc,
            self.transaction_fpds.foreign_government,
            self.transaction_fpds.foreign_owned_and_located,
            self.transaction_fpds.foundation,
            self.transaction_fpds.government_furnished_desc,
            self.transaction_fpds.government_furnished_prope,
            self.transaction_fpds.grants,
            self.transaction_fpds.hispanic_american_owned_bu,
            self.transaction_fpds.hispanic_servicing_institu,
            self.transaction_fpds.historically_black_college,
            self.transaction_fpds.historically_underutilized,
            self.transaction_fpds.hospital_flag,
            self.transaction_fpds.housing_authorities_public,
            self.transaction_fpds.idv_type,
            self.transaction_fpds.idv_type_description,
            self.transaction_fpds.indian_tribe_federally_rec,
            self.transaction_fpds.information_technolog_desc,
            self.transaction_fpds.information_technology_com,
            self.transaction_fpds.inherently_government_desc,
            self.transaction_fpds.inherently_government_func,
            self.transaction_fpds.inter_municipal_local_gove,
            self.transaction_fpds.interagency_contract_desc,
            self.transaction_fpds.interagency_contracting_au,
            self.transaction_fpds.international_organization,
            self.transaction_fpds.interstate_entity,
            self.transaction_fpds.joint_venture_economically,
            self.transaction_fpds.joint_venture_women_owned,
            self.transaction_fpds.labor_standards,
            self.transaction_fpds.labor_standards_descrip,
            self.transaction_fpds.labor_surplus_area_firm,
            self.transaction_fpds.limited_liability_corporat,
            self.transaction_fpds.local_area_set_aside,
            self.transaction_fpds.local_area_set_aside_desc,
            self.transaction_fpds.local_government_owned,
            self.transaction_fpds.major_program,
            self.transaction_fpds.manufacturer_of_goods,
            self.transaction_fpds.materials_supplies_article,
            self.transaction_fpds.materials_supplies_descrip,
            self.transaction_fpds.minority_institution,
            self.transaction_fpds.minority_owned_business,
            self.transaction_fpds.multi_year_contract,
            self.transaction_fpds.multi_year_contract_desc,
            self.transaction_fpds.multiple_or_single_aw_desc,
            self.transaction_fpds.multiple_or_single_award_i,
            self.transaction_fpds.municipality_local_governm,
            self.transaction_fpds.naics.alias("naics_code"),
            self.transaction_fpds.naics_description.alias("naics_description"),
            self.transaction_fpds.national_interest_action,
            self.transaction_fpds.national_interest_desc,
            self.transaction_fpds.native_american_owned_busi,
            self.transaction_fpds.native_hawaiian_owned_busi,
            self.transaction_fpds.native_hawaiian_servicing,
            self.transaction_fpds.nonprofit_organization,
            self.transaction_fpds.number_of_actions,
            self.transaction_fpds.number_of_offers_received,
            self.transaction_fpds.ordering_period_end_date,
            self.transaction_fpds.organizational_type,
            self.transaction_fpds.other_minority_owned_busin,
            self.transaction_fpds.other_not_for_profit_organ,
            self.transaction_fpds.other_statutory_authority,
            self.transaction_fpds.other_than_full_and_o_desc,
            self.transaction_fpds.other_than_full_and_open_c,
            self.transaction_fpds.parent_award_id,
            self.transaction_fpds.parself.transaction_normalizedership_or_limited_lia,
            self.transaction_fpds.performance_based_se_desc,
            self.transaction_fpds.performance_based_service,
            self.transaction_fpds.period_of_perf_potential_e,
            self.awards.piid,
            self.transaction_fpds.place_of_manufacture,
            self.transaction_fpds.place_of_manufacture_desc,
            self.transaction_fpds.planning_commission,
            self.transaction_fpds.port_authority,
            self.transaction_fpds.potential_total_value_awar,
            self.transaction_fpds.price_evaluation_adjustmen,
            self.transaction_fpds.private_university_or_coll,
            self.transaction_fpds.product_or_service_code,
            self.transaction_fpds.product_or_service_co_desc.alias("product_or_service_description"),
            self.transaction_fpds.program_acronym,
            self.transaction_fpds.program_system_or_equ_desc,
            self.transaction_fpds.program_system_or_equipmen,
            self.transaction_fpds.pulled_from,
            self.transaction_fpds.purchase_card_as_paym_desc,
            self.transaction_fpds.purchase_card_as_payment_m,
            self.transaction_fpds.receives_contracts_and_gra,
            self.transaction_fpds.recovered_materials_s_desc,
            self.transaction_fpds.recovered_materials_sustai,
            self.transaction_fpds.referenced_idv_agency_desc,
            self.transaction_fpds.referenced_idv_agency_iden,
            self.transaction_fpds.referenced_idv_modificatio,
            self.transaction_fpds.referenced_idv_type,
            self.transaction_fpds.referenced_idv_type_desc,
            self.transaction_fpds.referenced_mult_or_si_desc,
            self.transaction_fpds.referenced_mult_or_single,
            self.transaction_fpds.research,
            self.transaction_fpds.research_description,
            self.transaction_fpds.sam_exception,
            self.transaction_fpds.sam_exception_description,
            self.transaction_fpds.sba_certified_8_a_joint_ve,
            self.transaction_fpds.school_district_local_gove,
            self.transaction_fpds.school_of_forestry,
            self.transaction_fpds.sea_transportation,
            self.transaction_fpds.sea_transportation_desc,
            self.transaction_fpds.self_certified_small_disad,
            self.transaction_fpds.service_disabled_veteran_o,
            self.transaction_fpds.small_agricultural_coopera,
            self.transaction_fpds.small_business_competitive,
            self.transaction_fpds.small_disadvantaged_busine,
            self.transaction_fpds.sole_proprietorship,
            self.transaction_fpds.solicitation_date,
            self.transaction_fpds.solicitation_identifier,
            self.transaction_fpds.solicitation_procedur_desc,
            self.transaction_fpds.solicitation_procedures,
            self.transaction_fpds.state_controlled_instituti,
            self.transaction_fpds.subchapter_s_corporation,
            self.transaction_fpds.subcontinent_asian_asian_i,
            self.transaction_fpds.subcontracting_plan,
            self.transaction_fpds.subcontracting_plan_desc,
            self.transaction_fpds.the_ability_one_program,
            self.transaction_fpds.total_obligated_amount,
            self.transaction_fpds.township_local_government,
            self.transaction_fpds.transaction_number,
            self.transaction_fpds.transit_authority,
            self.transaction_fpds.tribal_college,
            self.transaction_fpds.tribally_owned_business,
            self.transaction_fpds.type_of_contract_pricing,
            self.transaction_fpds.type_of_contract_pric_desc,
            self.transaction_fpds.type_of_idc,
            self.transaction_fpds.type_of_idc_description,
            self.transaction_fpds.type_set_aside,
            self.transaction_fpds.type_set_aside_description,
            self.transaction_fpds.undefinitized_action,
            self.transaction_fpds.undefinitized_action_desc,
            self.transaction_fpds.us_federal_government,
            self.transaction_fpds.us_government_entity,
            self.transaction_fpds.us_local_government,
            self.transaction_fpds.us_state_government,
            self.transaction_fpds.us_tribal_government,
            self.transaction_fpds.vendor_doing_as_business_n,
            self.transaction_fpds.vendor_fax_number,
            self.transaction_fpds.vendor_phone_number,
            self.transaction_fpds.veteran_owned_business,
            self.transaction_fpds.veterinary_college,
            self.transaction_fpds.veterinary_hospital,
            self.transaction_fpds.woman_owned_business,
            self.transaction_fpds.women_owned_small_business,
            self.fed_and_tres_acct.program_activities.cast(StringType()),
        ]

    def __call__(self) -> DataFrame:
        return (
            self.transaction_normalized.join(
                self.transaction_fabs,
                (self.transaction_normalized.id == self.transaction_fabs.transaction_id)
                & ~self.transaction_normalized.is_fpds,
                "leftouter",
            )
            .join(
                self.transaction_fpds,
                (self.transaction_normalized.id == self.transaction_fpds.transaction_id)
                & self.transaction_normalized.is_fpds,
                "leftouter",
            )
            .join(
                self.references_cfda,
                self.transaction_fabs.cfda_number == self.references_cfda.program_number,
                "leftouter",
            )
            .join(
                self.recipient_lookup,
                self.recipient_lookup.recipient_hash == self.generated_recipient_hash,
                "leftouter",
            )
            .join(self.awards, self.transaction_normalized.award_id == self.awards.id, "leftouter")
            .join(
                self.awarding_agency,
                self.transaction_normalized.awarding_agency_id == self.awarding_agency.id,
                "leftouter",
            )
            .join(
                self.awarding_toptier_agency,
                self.awarding_agency.toptier_agency_id == self.awarding_toptier_agency.toptier_agency_id,
                "leftouter",
            )
            .join(
                self.awarding_subtier_agency,
                self.awarding_agency.subtier_agency_id == self.awarding_subtier_agency.subtier_agency_id,
                "leftouter",
            )
            .join(
                self.awarding_agency_id,
                (
                    (self.awarding_agency_id.toptier_agency_id == self.awarding_toptier_agency.toptier_agency_id)
                    & self.awarding_agency_id.toptier_flag
                ),
                "leftouter",
            )
            .join(
                self.funding_agency,
                self.transaction_normalized.funding_agency_id == self.funding_agency.id,
                "leftouter",
            )
            .join(
                self.funding_toptier_agency,
                self.funding_agency.funding_toptier_agency_id == self.funding_toptier_agency.toptier_agency_id,
                "leftouter",
            )
            .join(
                self.funding_subtier_agency,
                self.funding_agency.funding_subtier_agency_id == self.funding_subtier_agency.subtier_agency_id,
                "leftouter",
            )
            .join(
                self.funding_agency_id,
                (
                    (self.funding_agency_id.toptier_agency_id == self.funding_toptier_agency.funding_toptier_agency_id)
                    & (self.funding_agency_id.row_num == 1)
                ),
                "leftouter",
            )
            .join(
                self.parent_recipient,
                self.parent_recipient.parent_recipient_hash == self.generated_parent_recipient_hash,
                "leftouter",
            )
            .join(
                self.recipient_hash_and_levels,
                (sf.col("recipient_hash") == sf.col("recipient_level_hash"))
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
                self.pop_state_lookup,
                sf.col("pop_state_code")
                == sf.coalesce(
                    self.transaction_fpds.place_of_performance_state, self.transaction_fabs.place_of_perfor_state_code
                ),
                "leftouter",
            )
            .join(
                self.pop_state_population,
                (self.pop_state_population.state_code == sf.col("pop_state_fips"))
                & (self.pop_state_population.county_number == "000"),
                "leftouter",
            )
            .join(
                self.pop_county_population,
                (self.pop_county_population.state_code == sf.col("pop_state_fips"))
                & (
                    self.pop_county_population.county_number
                    == extract_numbers_as_string(
                        sf.coalesce(
                            self.transaction_fpds.place_of_perform_county_co,
                            self.transaction_fabs.place_of_perform_county_co,
                        ),
                        3,
                    )
                ),
                "leftouter",
            )
            .join(
                self.pop_district_population,
                (self.pop_district_population.state_code == sf.col("pop_state_fips"))
                & (
                    self.pop_district_population.congressional_district
                    == extract_numbers_as_string(
                        sf.coalesce(
                            self.transaction_fpds.place_of_performance_congr,
                            self.transaction_fabs.place_of_performance_congr,
                        )
                    )
                ),
                "leftouter",
            )
            .join(
                self.rl_state_lookup,
                sf.col("rl_state_code")
                == sf.coalesce(
                    self.transaction_fpds.legal_entity_state_code, self.transaction_fabs.legal_entity_state_code
                ),
                "leftouter",
            )
            .join(
                self.rl_state_population,
                (self.rl_state_population.state_code == sf.col("recipient_location_state_fips"))
                & (self.rl_state_population.county_number == "000"),
                "leftouter",
            )
            .join(
                self.rl_county_population,
                (self.rl_county_population.state_code == sf.col("recipient_location_state_fips"))
                & (
                    self.rl_county_population.county_number
                    == extract_numbers_as_string(
                        sf.coalesce(
                            self.transaction_fpds.legal_entity_county_code,
                            self.transaction_fabs.legal_entity_county_code,
                        ),
                        3,
                    )
                ),
                "leftouter",
            )
            .join(
                self.rl_district_population,
                (self.rl_district_population.state_code == sf.col("recipient_location_state_fips"))
                & (
                    self.rl_district_population.congressional_district
                    == extract_numbers_as_string(
                        sf.coalesce(
                            self.transaction_fpds.legal_entity_congressional,
                            self.transaction_fabs.legal_entity_congressional,
                        ),
                    )
                ),
                "leftouter",
            )
            .join(self.current_cd, self.transaction_normalized.id == self.current_cd.transaction_id, "leftouter")
            .join(
                self.awarding_office,
                self.awarding_office.office_code
                == sf.coalesce(self.transaction_fabs.awarding_office_code, self.transaction_fpds.awarding_office_code),
                "leftouter",
            )
            .join(
                self.funding_office,
                self.funding_office.office_code
                == sf.coalesce(self.transaction_fabs.funding_office_code, self.transaction_fpds.funding_office_code),
                "leftouter",
            )
            .join(
                self.fed_and_tres_acct,
                self.fed_and_tres_acct.award_id == self.transaction_normalized.award_id,
                "leftouter",
            )
            .select(
                *self.key_cols,
                *self.date_cols,
                *self.agency_cols,
                *self.typing_cols,
                *self.amounts_cols,
                *self.recipient_cols,
                *self.recipient_location_cols,
                *self.place_of_performance_cols,
                *self.accounts_cols,
                *self.officer_amounts_cols,
                *self.fabs_cols,
                *self.fpds_cols,
            )
            .withColumn("merge_hash_key", sf.xxhash64("*"))
        )


def load_transaction_search(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    df = TransactionSearchDataframe(spark)()
    df.write.format("delta").mode("overwrite").saveAsTable(f"{destination_database}.{destination_table_name}")


def load_transaction_search_incremental(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = TransactionSearchDataframe(spark)().alias("s")
    (
        target.merge(source, "s.transaction_id = t.transaction_id and s.merge_hash_key = t.merge_hash_key")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
