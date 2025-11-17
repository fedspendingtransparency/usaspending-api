from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as sf, Column
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
)

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES
from usaspending_api.search.delta_models.dataframes.abstract_search import AbstractSearch, extract_numbers_as_string

ALL_AWARD_TYPES = list(award_type_mapping.keys())


class AwardSearch(AbstractSearch):

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.submission_attributes = spark.table("global_temp.submission_attributes")
        self.dabs_submission_window_schedule = spark.table("global_temp.dabs_submission_window_schedule")
        self.disaster_emergency_fund_code = spark.table("global_temp.disaster_emergency_fund_code")

    @property
    def transaction_cfdas(self):
        return (
            self.transaction_fabs.join(
                self.transaction_normalized,
                self.transaction_fabs.transaction_id == self.transaction_normalized.id,
                "inner",
            )
            .groupby(sf.col("award_id"))
            .agg(
                sf.sort_array(
                    sf.collect_set(
                        sf.to_json(
                            sf.named_struct(
                                sf.lit("cfda_number"),
                                sf.col("cfda_number"),
                                sf.lit("cfda_program_title"),
                                sf.col("cfda_title"),
                            )
                        )
                    )
                ).alias("cfdas")
            )
            .select(
                sf.col("award_id"),
                sf.col("cfdas"),
            )
        )

    @property
    def recipient_hash_and_levels(self) -> DataFrame:
        return (
            self.recipient_profile.filter(sf.col("recipient_level") != sf.lit("P"))
            .groupBy("recipient_hash", "uei")
            .agg(sf.sort_array(sf.collect_set("recipient_level")).alias("recipient_levels"))
            .select(
                sf.col("recipient_hash").alias("recipient_level_hash"),
                sf.col("recipient_levels"),
            )
        )

    @property
    def outlays_and_obligations(self) -> DataFrame:
        return (
            self.faba.join(
                self.submission_attributes, self.submission_attributes.submission_id == self.faba.submission_id, "inner"
            )
            .join(
                self.dabs_submission_window_schedule,
                (self.submission_attributes.submission_window_id == self.dabs_submission_window_schedule.id)
                & (self.dabs_submission_window_schedule.submission_reveal_date <= sf.now()),
                "inner",
            )
            .join(
                self.disaster_emergency_fund_code,
                self.disaster_emergency_fund_code.code == self.faba.disaster_emergency_fund_code,
                "leftouter",
            )
            .filter(
                sf.col("gross_outlay_amount_by_award_cpe").isNotNull()
                | sf.col("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe").isNotNull()
                | sf.col("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe").isNotNull()
                | sf.col("transaction_obligated_amount").isNotNull()
            )
            .groupby(sf.col("award_id"), sf.col("disaster_emergency_fund_code"), sf.col("group_name"))
            .agg(
                sf.coalesce(
                    sf.sum(
                        sf.when(
                            self.submission_attributes.is_final_balances_for_fy,
                            sf.coalesce(self.faba.gross_outlay_amount_by_award_cpe, sf.lit(0))
                            + sf.coalesce(
                                self.faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, sf.lit(0)
                            )
                            + sf.coalesce(
                                self.faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, sf.lit(0)
                            ),
                        ).otherwise(sf.lit(None))
                    ),
                    sf.lit(0),
                ).alias("outlay"),
                sf.coalesce(sf.sum(self.faba.transaction_obligated_amount), sf.lit(0)).alias("obligation"),
            )
            .select(
                sf.col("award_id"),
                sf.col("disaster_emergency_fund_code"),
                sf.col("group_name"),
                sf.col("outlay"),
                sf.col("obligation"),
            )
            .filter(sf.col("award_id").isNotNull())
            .groupby(sf.col("award_id"))
            .agg(
                sf.sort_array(
                    sf.collect_set(
                        sf.when(
                            sf.col("disaster_emergency_fund_code").isNotNull(),
                            sf.to_json(
                                sf.named_struct(
                                    sf.lit("defc"),
                                    sf.col("disaster_emergency_fund_code"),
                                    sf.lit("outlay"),
                                    sf.col("outlay"),
                                    sf.lit("obligation"),
                                    sf.col("obligation"),
                                )
                            ),
                        )
                    )
                )
                .cast(StringType())
                .alias("spending_by_defc"),
                sf.sum(sf.col("outlay")).alias("total_outlays"),
                sf.sum(sf.when(sf.col("group_name") == sf.lit("covid_19"), sf.col("outlay")))
                .cast(DecimalType(23, 2))
                .alias("total_covid_outlay"),
                sf.sum(sf.when(sf.col("group_name") == sf.lit("covid_19"), sf.col("obligation")))
                .cast(DecimalType(23, 2))
                .alias("total_covid_obligation"),
                sf.sum(sf.when(sf.col("group_name") == sf.lit("infrastructure"), sf.col("outlay")))
                .cast(DecimalType(23, 2))
                .alias("total_iija_outlay"),
                sf.sum(sf.when(sf.col("group_name") == sf.lit("infrastructure"), sf.col("obligation")))
                .cast(DecimalType(23, 2))
                .alias("total_iija_obligation"),
            )
            .select(
                sf.col("award_id"),
                sf.col("spending_by_defc"),
                sf.col("total_outlays"),
                sf.col("total_covid_outlay"),
                sf.col("total_covid_obligation"),
                sf.col("total_iija_outlay"),
                sf.col("total_iija_obligation"),
            )
        )

    @property
    def treasury_account(self):
        return (
            self.treasury_appropriation_account.join(
                self.faba,
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
                self.awarding_toptier_agency.toptier_agency_id == self.federal_account.parent_toptier_agency_id,
                "inner",
            )
            .join(self.ref_program_activity, self.ref_program_activity.id == self.faba.program_activity_id, "left")
            .join(
                self.program_activity_park,
                self.program_activity_park.code == self.faba.program_activity_reporting_key,
                "left",
            )
            .filter(self.faba.award_id.isNotNull())
            .groupby(sf.col("award_id"))
            .agg(*self.accts_agg)
        )

    @property
    def total_obligation_bin(self) -> Column:
        total_ob = sf.coalesce(
            sf.when(self.awards.type.isin(["07", "08"]), self.awards.total_subsidy_cost).otherwise(
                self.awards.total_obligation
            ),
            sf.lit(0),
        )
        return (
            sf.when(total_ob == sf.lit(500_000_000), sf.lit("500M"))
            .when(total_ob == sf.lit(100_000_000), sf.lit("100M"))
            .when(total_ob == sf.lit(1_000_000), sf.lit("1M"))
            .when(total_ob == sf.lit(25_000_000), sf.lit("25M"))
            .when(total_ob > sf.lit(500_000_000), sf.lit(">500M"))
            .when(total_ob < sf.lit(1_000_000), sf.lit("<1M"))
            .when(total_ob < sf.lit(25_000_000), sf.lit("1M..25M"))
            .when(total_ob < sf.lit(100_000_000), sf.lit("25M..100M"))
            .when(total_ob < sf.lit(500_000_000), sf.lit("100M..500M"))
            .otherwise(sf.lit(None))
            .alias("total_obl_bin")
        )

    @property
    def dataframe(self) -> DataFrame:
        df = (
            self.awards.join(
                self.transaction_normalized,
                self.transaction_normalized.id == self.awards.latest_transaction_id,
                "inner",
            )
            .join(
                self.transaction_fpds,
                (self.transaction_fpds.transaction_id == self.awards.latest_transaction_id)
                & self.transaction_normalized.is_fpds,
                "leftouter",
            )
            .join(
                self.transaction_fabs,
                (self.transaction_fabs.transaction_id == self.awards.latest_transaction_id)
                & ~self.transaction_normalized.is_fpds,
                "leftouter",
            )
            .join(
                self.recipient_lookup,
                self.recipient_lookup.recipient_hash == self.generated_recipient_hash,
                "leftouter",
            )
            .join(
                self.product_service_code,
                self.transaction_fpds.product_or_service_code == self.product_service_code.code,
            )
            .join(
                self.transaction_cfdas,
                self.transaction_cfdas.award_id == self.awards.id,
                "leftouter",
            )
            .join(self.awarding_agency, self.awarding_agency.id == self.awards.awarding_agency_id, "leftouter")
            .join(
                self.awarding_toptier_agency,
                self.awarding_agency.toptier_agency_id == self.awarding_toptier_agency.toptier_agency_id,
                "leftouter",
            )
            .join(
                self.awarding_subtier_agency,
                self.awarding_agency.subtier_agency_id == self.awarding_agency.subtier_agency_id,
                "leftouter",
            )
            .join(self.funding_agency, self.funding_agency.id == self.awards.funding_agency_id, "leftouter")
            .join(
                self.funding_toptier_agency,
                self.funding_toptier_agency.toptier_agency_id == self.funding_agency.funding_toptier_agency_id,
                "leftouter",
            )
            .join(
                self.funding_subtier_agency,
                self.funding_subtier_agency.subtier_agency_id == self.funding_agency.funding_subtier_agency_id,
                "leftouter",
            )
            .join(
                self.funding_agency_id,
                (self.funding_agency_id.toptier_agency_id == self.funding_toptier_agency.funding_toptier_agency_id)
                & (self.funding_agency_id.row_num == 1),
                "leftouter",
            )
        )
        df_with_location = self.join_location_data(df)

        return (
            df_with_location.join(
                self.current_cd, (self.awards.latest_transaction_id == self.current_cd.transaction_id), "leftouter"
            )
            .join(
                self.recipient_hash_and_levels,
                (sf.col("recipient_hash") == sf.col("recipient_level_hash"))
                & ~(sf.col("legal_business_name").isin(SPECIAL_CASES))
                & sf.col("legal_business_name").isNotNull(),
                "leftouter",
            )
            .join(self.outlays_and_obligations, self.outlays_and_obligations.award_id == self.awards.id, "leftouter")
            .join(self.treasury_account, self.treasury_account.award_id == self.awards.id, "leftouter")
            .select(
                sf.col("treasury_account_identifiers"),
                self.awards.id.alias("award_id"),
                self.awards.data_source,
                self.awards.transaction_unique_id,
                self.awards.latest_transaction_id,
                self.awards.earliest_transaction_id,
                self.awards.latest_transaction_id.alias("latest_transaction_search_id"),
                self.awards.earliest_transaction_id.alias("earliest_transaction_search_id"),
                self.awards.category,
                self.awards.type.alias("type_raw"),
                self.awards.type_description.alias("type_description_raw"),
                sf.when(~self.awards.type.isin(ALL_AWARD_TYPES) | self.awards.type.isNull(), sf.lit("-1"))
                .otherwise(self.awards.type)
                .alias("type"),
                sf.when(~self.awards.type.isin(ALL_AWARD_TYPES) | self.awards.type.isNull(), sf.lit("NOT SPECIFIED"))
                .otherwise(self.awards.type_description)
                .alias("type_description"),
                self.awards.is_fpds,
                self.awards.generated_unique_award_id,
                sf.when(
                    ~self.awards.is_fpds & (self.transaction_fabs.record_type == sf.lit(1)),
                    sf.upper(
                        sf.concat(
                            sf.lit("ASST_AGG_"),
                            sf.coalesce(self.transaction_fabs.uri, sf.lit("-none-")),
                            sf.lit("_"),
                            sf.coalesce(self.awarding_subtier_agency.subtier_code, sf.lit("-none-")),
                        )
                    ),
                )
                .when(
                    ~self.awards.is_fpds,
                    sf.upper(
                        sf.concat(
                            sf.lit("ASST_NON_"),
                            sf.coalesce(self.transaction_fabs.fain, sf.lit("-none-")),
                            sf.lit("_"),
                            sf.coalesce(self.awarding_subtier_agency.subtier_code, sf.lit("-none-")),
                        )
                    ),
                )
                .otherwise(sf.lit(None))
                .alias("generated_unique_award_id_legacy"),
                sf.when(
                    self.awards.type.isin([str(i).zfill(2) for i in range(2, 12)]) & self.awards.fain.isNotNull(),
                    self.awards.fain,
                )
                .when(self.awards.piid.isNotNull(), self.awards.piid)
                .otherwise(self.awards.uri)
                .alias("display_award_id"),
                self.awards.update_date,
                self.awards.certified_date,
                self.awards.create_date,
                self.awards.piid,
                self.awards.fain,
                self.awards.uri,
                self.awards.parent_award_piid,
                sf.coalesce(
                    sf.when(self.awards.type.isin(["07", "08"]), self.awards.total_subsidy_cost).otherwise(
                        self.awards.total_obligation
                    ),
                    sf.lit(0),
                )
                .cast(DecimalType(23, 2))
                .alias("award_amount"),
                sf.coalesce(
                    sf.when(self.awards.type.isin(["07", "08"]), sf.lit(0)).otherwise(self.awards.total_obligation),
                    sf.lit(0),
                )
                .cast(DecimalType(23, 2))
                .alias("total_obligation"),
                self.awards.description,
                self.total_obligation_bin,
                sf.coalesce(
                    sf.when(self.awards.type.isin(["07", "08"]), self.awards.total_subsidy_cost).otherwise(sf.lit(0)),
                    sf.lit(0),
                )
                .cast(DecimalType(23, 2))
                .alias("total_subsidy_cost"),
                sf.coalesce(
                    sf.when(self.awards.type.isin(["07", "08"]), self.awards.total_loan_value).otherwise(sf.lit(0)),
                    sf.lit(0),
                )
                .cast(DecimalType(23, 2))
                .alias("total_loan_value"),
                self.awards.total_funding_amount,
                self.awards.total_indirect_federal_sharing,
                self.awards.base_and_all_options_value,
                self.awards.base_exercised_options_val,
                self.awards.non_federal_funding_amount,
                self.generated_recipient_hash.alias("recipient_hash"),
                sf.col("recipient_levels"),
                sf.upper(
                    sf.coalesce(
                        self.recipient_lookup.legal_business_name,
                        self.transaction_fpds.awardee_or_recipient_legal,
                        self.transaction_fabs.awardee_or_recipient_legal,
                    )
                ).alias("recipient_name"),
                sf.upper(
                    sf.coalesce(
                        self.transaction_fpds.awardee_or_recipient_legal,
                        self.transaction_fabs.awardee_or_recipient_legal,
                    )
                ).alias("raw_recipient_name"),
                sf.coalesce(
                    self.transaction_fpds.awardee_or_recipient_uniqu, self.transaction_fabs.awardee_or_recipient_uniqu
                ).alias("recipient_unique_id"),
                sf.coalesce(
                    self.transaction_fpds.ultimate_parent_unique_ide, self.transaction_fabs.ultimate_parent_unique_ide
                ).alias("parent_recipient_unique_id"),
                sf.coalesce(self.transaction_fpds.awardee_or_recipient_uei, self.transaction_fabs.uei).alias(
                    "recipient_uei"
                ),
                sf.coalesce(self.transaction_fpds.ultimate_parent_uei, self.transaction_fabs.ultimate_parent_uei).alias(
                    "parent_uei"
                ),
                sf.coalesce(
                    self.transaction_fpds.ultimate_parent_legal_enti, self.transaction_fabs.ultimate_parent_legal_enti
                ).alias("parent_recipient_name"),
                self.transaction_normalized.business_categories,
                self.awards.total_subaward_amount,
                self.awards.subaward_count,
                self.transaction_normalized.action_date,
                self.transaction_normalized.fiscal_year,
                self.transaction_normalized.last_modified_date,
                self.awards.period_of_performance_start_date,
                self.awards.period_of_performance_current_end_date,
                self.awards.date_signed,
                self.transaction_fpds.ordering_period_end_date.cast(DateType()),
                sf.coalesce(self.transaction_fabs.original_loan_subsidy_cost, sf.lit(0)).alias(
                    "original_loan_subsidy_cost"
                ),
                sf.coalesce(self.transaction_fabs.face_value_loan_guarantee, sf.lit(0)).alias(
                    "face_value_loan_guarantee"
                ),
                self.transaction_normalized.awarding_agency_id,
                self.transaction_normalized.funding_agency_id,
                self.awarding_toptier_agency.awarding_toptier_agency_name,
                self.funding_toptier_agency.funding_toptier_agency_name,
                self.awarding_subtier_agency.awarding_subtier_agency_name,
                self.funding_subtier_agency.funding_subtier_agency_name,
                sf.coalesce(
                    self.transaction_fabs.awarding_agency_name, self.transaction_fpds.awarding_agency_name
                ).alias("awarding_toptier_agency_name_raw"),
                sf.coalesce(self.transaction_fabs.funding_agency_name, self.transaction_fpds.funding_agency_name).alias(
                    "funding_toptier_agency_name_raw"
                ),
                sf.coalesce(
                    self.transaction_fabs.awarding_sub_tier_agency_n, self.transaction_fpds.awarding_sub_tier_agency_n
                ).alias("awarding_subtier_agency_name_raw"),
                sf.coalesce(
                    self.transaction_fabs.funding_sub_tier_agency_na, self.transaction_fpds.funding_sub_tier_agency_na
                ).alias("funding_subtier_agency_name_raw"),
                self.awarding_toptier_agency.awarding_toptier_agency_code,
                self.funding_toptier_agency.funding_toptier_agency_code,
                self.awarding_subtier_agency.awarding_subtier_agency_code,
                self.funding_subtier_agency.funding_subtier_agency_code,
                sf.coalesce(
                    self.transaction_fabs.awarding_agency_code, self.transaction_fpds.awarding_agency_code
                ).alias("awarding_toptier_agency_code_raw"),
                sf.coalesce(self.transaction_fabs.funding_agency_code, self.transaction_fpds.funding_agency_code).alias(
                    "funding_toptier_agency_code_raw"
                ),
                sf.coalesce(
                    self.transaction_fabs.awarding_sub_tier_agency_c, self.transaction_fpds.awarding_sub_tier_agency_c
                ).alias("awarding_subtier_agency_code_raw"),
                sf.coalesce(
                    self.transaction_fabs.funding_sub_tier_agency_co, self.transaction_fpds.funding_sub_tier_agency_co
                ).alias("funding_subtier_agency_code_raw"),
                self.funding_agency_id.funding_toptier_agency_id,
                self.transaction_normalized.funding_agency_id.alias("funding_subtier_agency_id"),
                self.awards.fpds_agency_id,
                self.awards.fpds_parent_agency_id,
                sf.coalesce(
                    self.transaction_fpds.legal_entity_country_code,
                    self.transaction_fabs.legal_entity_country_code,
                    sf.lit("USA"),
                ).alias("recipient_location_country_code"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_country_name, self.transaction_fabs.legal_entity_country_name
                ).alias("recipient_location_country_name"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_state_code, self.transaction_fabs.legal_entity_state_code
                ).alias("recipient_location_state_code"),
                extract_numbers_as_string(
                    sf.coalesce(
                        self.transaction_fpds.legal_entity_county_code, self.transaction_fabs.legal_entity_county_code
                    ),
                    3,
                ).alias("recipient_location_county_code"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_county_name, self.transaction_fabs.legal_entity_county_name
                ).alias("recipient_location_county_name"),
                extract_numbers_as_string(
                    sf.coalesce(
                        self.transaction_fpds.legal_entity_congressional,
                        self.transaction_fabs.legal_entity_congressional,
                    ),
                    2,
                ).alias("recipient_location_congressional_code"),
                self.current_cd.recipient_location_congressional_code_current,
                sf.coalesce(self.transaction_fpds.legal_entity_zip5, self.transaction_fabs.legal_entity_zip5).alias(
                    "recipient_location_zip5"
                ),
                sf.rtrim(
                    sf.coalesce(
                        self.transaction_fpds.legal_entity_city_name, self.transaction_fabs.legal_entity_city_name
                    )
                ).alias("recipient_location_city_name"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_state_descrip, self.transaction_fabs.legal_entity_state_name
                ).alias("recipient_location_state_name"),
                sf.col("recipient_location_state_fips"),
                self.rl_state_population.recipient_location_state_population,
                self.rl_county_population.recipient_location_county_population,
                self.rl_district_population.recipient_location_congressional_population,
                sf.concat(
                    sf.col("recipient_location_state_fips"),
                    sf.coalesce(
                        self.transaction_fpds.legal_entity_county_code, self.transaction_fabs.legal_entity_county_code
                    ),
                ).alias("recipient_location_county_fips"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_address_line1, self.transaction_fabs.legal_entity_address_line1
                ).alias("recipient_location_address_line1"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_address_line2, self.transaction_fabs.legal_entity_address_line2
                ).alias("recipient_location_address_line2"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_address_line3, self.transaction_fabs.legal_entity_address_line3
                ).alias("recipient_location_address_line3"),
                sf.coalesce(
                    self.transaction_fpds.legal_entity_zip_last4, self.transaction_fabs.legal_entity_zip_last4
                ).alias("recipient_location_zip4"),
                self.transaction_fabs.legal_entity_foreign_posta.alias("recipient_location_foreign_postal_code"),
                self.transaction_fabs.legal_entity_foreign_provi.alias("recipient_location_foreign_province"),
                sf.coalesce(
                    self.transaction_fpds.place_of_perf_country_desc, self.transaction_fabs.place_of_perform_country_n
                ).alias("pop_country_name"),
                sf.coalesce(
                    self.transaction_fpds.place_of_perform_country_c,
                    self.transaction_fabs.place_of_perform_country_c,
                    sf.lit("USA"),
                ).alias("pop_country_code"),
                sf.coalesce(
                    self.transaction_fpds.place_of_performance_state, self.transaction_fabs.place_of_perfor_state_code
                ).alias("pop_state_code"),
                extract_numbers_as_string(
                    sf.coalesce(
                        self.transaction_fpds.place_of_perform_county_co,
                        self.transaction_fabs.place_of_perform_county_co,
                    ),
                    3,
                ).alias("pop_county_code"),
                sf.coalesce(
                    self.transaction_fpds.place_of_perform_county_na, self.transaction_fabs.place_of_perform_county_na
                ).alias("pop_county_name"),
                self.transaction_fabs.place_of_performance_code.alias("pop_city_code"),
                sf.coalesce(
                    self.transaction_fpds.place_of_performance_zip5, self.transaction_fabs.place_of_performance_zip5
                ).alias("pop_zip5"),
                extract_numbers_as_string(
                    sf.coalesce(
                        self.transaction_fpds.place_of_performance_congr,
                        self.transaction_fabs.place_of_performance_congr,
                    ),
                    2,
                ).alias("pop_congressional_code"),
                self.current_cd.pop_congressional_code_current,
                sf.rtrim(
                    sf.coalesce(
                        self.transaction_fpds.place_of_perform_city_name,
                        self.transaction_fabs.place_of_performance_city,
                    )
                ).alias("pop_city_name"),
                sf.coalesce(
                    self.transaction_fpds.place_of_perfor_state_desc, self.transaction_fabs.place_of_perform_state_nam
                ).alias("pop_state_name"),
                sf.col("pop_state_fips"),
                self.pop_state_population.pop_state_population,
                self.pop_county_population.pop_county_population,
                self.pop_district_population.pop_congressional_population,
                sf.concat(
                    sf.col("pop_state_fips"),
                    sf.coalesce(
                        self.transaction_fpds.place_of_perform_county_co,
                        self.transaction_fabs.place_of_perform_county_co,
                    ),
                ).alias("pop_county_fips"),
                sf.coalesce(
                    self.transaction_fpds.place_of_performance_zip4a, self.transaction_fabs.place_of_performance_zip4a
                ).alias("pop_zip4"),
                self.transaction_fabs.cfda_title.alias("cfda_program_title"),
                self.transaction_fabs.cfda_number,
                sf.when(~self.awards.is_fpds, sf.col("cfdas")).otherwise(sf.lit(None)).alias("cfdas"),
                self.transaction_fabs.sai_number,
                self.transaction_fpds.type_of_contract_pricing,
                self.transaction_fpds.extent_competed,
                self.transaction_fpds.type_set_aside,
                self.transaction_fpds.product_or_service_code,
                self.product_service_code.description.alias("product_or_service_description"),
                self.transaction_fpds.naics.alias("naics_code"),
                self.transaction_fpds.naics_description,
                sf.col("tas_paths"),
                sf.col("tas_components"),
                sf.col("federal_accounts"),
                sf.col("disaster_emergency_fund_codes"),
                sf.col("spending_by_defc"),
                sf.col("total_covid_outlay"),
                sf.col("total_covid_obligation"),
                self.awards.officer_1_amount,
                self.awards.officer_1_name,
                self.awards.officer_2_amount,
                self.awards.officer_2_name,
                self.awards.officer_3_amount,
                self.awards.officer_3_name,
                self.awards.officer_4_amount,
                self.awards.officer_4_name,
                self.awards.officer_5_amount,
                self.awards.officer_5_name,
                sf.col("total_iija_outlay"),
                sf.col("total_iija_obligation"),
                sf.col("total_outlays").cast(DecimalType(23, 2)).alias("total_outlays"),
                sf.coalesce(
                    sf.when(self.awards.type.isin(["07", "08"]), self.awards.total_subsidy_cost).otherwise(
                        self.awards.total_obligation
                    ),
                    sf.lit(0),
                )
                .cast(DecimalType(23, 2))
                .alias("generated_pragmatic_obligation"),
                sf.col("program_activities"),
                self.awards.transaction_count,
            )
            .withColumn("merge_hash_key", sf.xxhash64("*"))
        )


def load_award_search(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    df = AwardSearch(spark).dataframe
    df.write.saveAsTable(
        f"{destination_database}.{destination_table_name}",
        mode="overwrite",
        format="delta",
    )


def load_award_search_incremental(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = AwardSearch(spark).dataframe.alias("s")
    (
        target.merge(source, "s.award_id = t.award_id and s.merge_hash_key = t.merge_hash_key")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
