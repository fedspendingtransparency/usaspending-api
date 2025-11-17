from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession, functions as sf, Column, Window
from pyspark.sql.types import (
    DateType,
    DecimalType,
    ShortType,
    StringType,
    TimestampType,
)


def hash_col(col: Column) -> Column:
    return sf.regexp_replace(sf.md5(sf.upper(col)), "^(.{8})(.{4})(.{4})(.{4})(.{12})$", "$1-$2-$3-$4-$5")


def extract_numbers_as_string(col: Column, length: int = 2, pad: str = "0") -> Column:
    return sf.lpad(
        sf.regexp_extract(col, r"^[A-Z]*(\d+)(?:.\d+)?$", 1).cast(ShortType()).cast(StringType()), length, pad
    )


class AbstractSearch(ABC):

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
            .withColumn("awarding_subtier_agency_id", sf.col("subtier_agency_id"))
        )
        self.awarding_toptier_agency = (
            spark.table("global_temp.toptier_agency")
            .withColumn("awarding_toptier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("awarding_toptier_agency_name", sf.col("name"))
            .withColumn("awarding_toptier_agency_code", sf.col("toptier_code"))
        )
        self.awarding_subtier_agency = (
            spark.table("global_temp.subtier_agency")
            .withColumn("awarding_subtier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("awarding_subtier_agency_name", sf.col("name"))
            .withColumn("awarding_subtier_agency_code", sf.col("subtier_code"))
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
            .withColumn("funding_toptier_agency_code", sf.col("toptier_code"))
        )
        self.funding_subtier_agency = (
            spark.table("global_temp.subtier_agency")
            .withColumn("funding_subtier_agency_abbreviation", sf.col("abbreviation"))
            .withColumn("funding_subtier_agency_name", sf.col("name"))
            .withColumn("funding_subtier_agency_code", sf.col("subtier_code"))
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
        self.product_service_code = spark.table("global_temp.psc")

    @property
    @abstractmethod
    def recipient_hash_and_levels(self) -> DataFrame: ...

    @property
    def generated_recipient_hash(self) -> Column:
        return hash_col(
            sf.when(
                sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei).isNotNull(),
                sf.concat(
                    sf.lit("uei-"),
                    sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei),
                ),
            )
            .when(
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
            )
            .otherwise(
                sf.concat(
                    sf.lit("name-"),
                    sf.coalesce(
                        self.transaction_fpds.awardee_or_recipient_legal,
                        self.transaction_fabs.awardee_or_recipient_legal,
                    ),
                )
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
    def rl_state_lookup(self) -> DataFrame:
        return (
            self.state_data.groupBy("code", "name", "fips")
            .agg(sf.max(sf.col("id")))
            .select(
                sf.col("code").alias("rl_state_code"),
                sf.col("fips").alias("recipient_location_state_fips"),
            )
        )

    def join_location_data(self, df: DataFrame) -> DataFrame:
        return (
            df.join(
                self.pop_state_lookup,
                (
                    sf.col("pop_state_code")
                    == sf.coalesce(
                        self.transaction_fpds.place_of_performance_state,
                        self.transaction_fabs.place_of_perfor_state_code,
                    )
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
    def accts_agg(self) -> list[Column]:
        return [
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
            )
            .cast(StringType())
            .alias("program_activities"),
        ]

    @property
    @abstractmethod
    def dataframe(self) -> DataFrame: ...
