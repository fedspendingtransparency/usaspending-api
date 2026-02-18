from delta import DeltaTable
from django.contrib.postgres.aggregates import ArrayAgg
from django.utils.functional import cached_property
from pyspark.sql import DataFrame, SparkSession, column
from pyspark.sql import functions as sf
from pyspark.sql.types import DecimalType

from usaspending_api.download.helpers.download_annotation_functions import AWARD_URL
from usaspending_api.references.models import DisasterEmergencyFundCode


class TransactionDownload:

    def __init__(self, spark: SparkSession):
        self.award_search = spark.table("rpt.award_search")
        self.disaster_emergency_fund_code = spark.table("global_temp.disaster_emergency_fund_code")
        self.federal_account = spark.table("global_temp.federal_account")
        self.faba = spark.table("int.financial_accounts_by_awards").withColumnRenamed(
            "award_id", "faba_award_id"
        )
        self.object_class = spark.table("global_temp.object_class")
        self.ref_program_activity = spark.table("global_temp.ref_program_activity")
        self.submission_attributes = spark.table("global_temp.submission_attributes")
        self.transaction_search = spark.table("rpt.transaction_search").withColumnRenamed(
            "award_id", "transaction_award_id"
        )
        self.treasury_appropriation_account = spark.table("global_temp.treasury_appropriation_account")

    @cached_property
    def defc_by_group(self) -> dict[str, list[str]]:
        defc_groups = (
            DisasterEmergencyFundCode.objects.filter(group_name__isnull=False)
            .values("group_name")
            .annotate(code_list=ArrayAgg("code"))
            .values("group_name", "code_list")
        )
        return {val["group_name"]: val["code_list"] for val in defc_groups}

    @property
    def iija_defc(self) -> list[str]:
        return self.defc_by_group["infrastructure"]

    @property
    def covid_defc(self) -> list[str]:
        return self.defc_by_group["covid_19"]

    @property
    def faba_aggs_df(self) -> DataFrame:
        faba_aggs = {
            "covid_19_obligated_amount": (
                sf.sum(
                    sf.when(
                        self.faba.disaster_emergency_fund_code.isin(self.covid_defc),
                        self.faba.transaction_obligated_amount,
                    )
                ).cast(DecimalType(23, 2))
            ),
            "covid_19_outlayed_amount": (
                sf.sum(
                    sf.when(
                        self.faba.disaster_emergency_fund_code.isin(self.covid_defc)
                        & self.submission_attributes.is_final_balances_for_fy,
                        self.faba.gross_outlay_amount_by_award_cpe
                        + self.faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                        + self.faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
                    )
                ).cast(DecimalType(23, 2))
            ),
            "defc_for_overall_award": (
                sf.concat_ws(
                    ";",
                    sf.sort_array(
                        sf.collect_set(
                            sf.when(
                                self.faba.disaster_emergency_fund_code.isNotNull(),
                                sf.concat(
                                    self.faba.disaster_emergency_fund_code,
                                    sf.lit(": "),
                                    self.disaster_emergency_fund_code.public_law,
                                ),
                            )
                        )
                    ),
                )
            ),
            "federal_accounts_funding_this_award": (
                sf.concat_ws(";", sf.sort_array(sf.collect_set(self.federal_account.federal_account_code)))
            ),
            "iija_obligated_amount": (
                sf.sum(
                    sf.when(
                        self.faba.disaster_emergency_fund_code.isin(self.iija_defc),
                        self.faba.transaction_obligated_amount,
                    )
                ).cast(DecimalType(23, 2))
            ),
            "iija_outlayed_amount": (
                sf.sum(
                    sf.when(
                        self.faba.disaster_emergency_fund_code.isin(self.iija_defc)
                        & self.submission_attributes.is_final_balances_for_fy,
                        self.faba.gross_outlay_amount_by_award_cpe
                        + self.faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                        + self.faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
                    )
                ).cast(DecimalType(23, 2))
            ),
            "object_classes_funding_this_award": (
                sf.concat_ws(
                    ";",
                    sf.sort_array(
                        sf.collect_set(
                            sf.when(
                                self.submission_attributes.is_final_balances_for_fy
                                & self.faba.object_class_id.isNotNull(),
                                sf.concat(
                                    self.object_class.object_class, sf.lit(": "), self.object_class.object_class_name
                                ),
                            )
                        )
                    ),
                )
            ),
            "program_activities_funding_this_award": (
                sf.concat_ws(
                    ";",
                    sf.sort_array(
                        sf.collect_set(
                            sf.when(
                                self.submission_attributes.is_final_balances_for_fy
                                & self.faba.program_activity_id.isNotNull(),
                                sf.concat(
                                    self.ref_program_activity.program_activity_code,
                                    sf.lit(": "),
                                    self.ref_program_activity.program_activity_name,
                                ),
                            )
                        )
                    ),
                )
            ),
            "total_outlayed_amount_for_overall_award": (
                sf.sum(
                    sf.when(
                        self.submission_attributes.is_final_balances_for_fy
                        & self.faba.gross_outlay_amount_by_award_cpe.isNotNull()
                        & self.faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe.isNotNull()
                        & self.faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe.isNotNull(),
                        self.faba.gross_outlay_amount_by_award_cpe
                        + self.faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                        + self.faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
                    )
                ).cast(DecimalType(23, 2))
            ),
            "treasury_accounts_funding_this_award": (
                sf.concat_ws(
                    ";", sf.sort_array(sf.collect_set(self.treasury_appropriation_account.tas_rendering_label))
                )
            ),
        }

        df = (
            self.faba.join(self.submission_attributes, "submission_id")
            .join(
                self.disaster_emergency_fund_code,
                self.faba.disaster_emergency_fund_code
                == self.disaster_emergency_fund_code.code,
                "left",
            )
            .join(
                self.treasury_appropriation_account,
                self.faba.treasury_account_id
                == self.treasury_appropriation_account.treasury_account_identifier,
            )
            .join(
                self.federal_account, self.treasury_appropriation_account.federal_account_id == self.federal_account.id
            )
            .join(self.object_class, self.faba.object_class_id == self.object_class.id, "left")
            .join(
                self.ref_program_activity,
                self.faba.program_activity_id == self.ref_program_activity.id,
                "left",
            )
            .groupBy(self.faba.faba_award_id)
            .agg(*[agg.alias(name) for name, agg in faba_aggs.items()])
        )
        return df

    @property
    def common_cols(self) -> list[column]:
        return [
            # --- Award ---
            self.award_search.award_id,
            self.award_search.generated_unique_award_id,
            self.award_search.description.alias("award_description"),
            self.award_search.total_obligation,
            # TODO: Update to use url_encode Spark SQL function
            sf.when(
                self.award_search.generated_unique_award_id.isNotNull(),
                sf.concat(
                    sf.lit(AWARD_URL),
                    sf.expr(
                        "java_method('java.net.URLEncoder', 'encode', award_search.generated_unique_award_id, 'UTF-8')"
                    ),
                    sf.lit("/"),
                ),
            )
            .otherwise("")
            .alias("usaspending_permalink"),
            # --- Transaction ---
            self.transaction_search.modification_number,
            self.transaction_search.transaction_id,
            sf.coalesce(
                self.transaction_search.published_fabs_id, self.transaction_search.detached_award_procurement_id
            ).alias("transaction_broker_id"),
            sf.coalesce(
                self.transaction_search.afa_generated_unique, self.transaction_search.detached_award_proc_unique
            ).alias("transaction_unique_key"),
            # Agencies
            self.transaction_search.awarding_agency_code,
            self.transaction_search.awarding_toptier_agency_name_raw,
            self.transaction_search.awarding_sub_tier_agency_c,
            self.transaction_search.awarding_subtier_agency_name_raw,
            self.transaction_search.funding_agency_code,
            self.transaction_search.funding_toptier_agency_name_raw,
            self.transaction_search.funding_sub_tier_agency_co,
            self.transaction_search.funding_subtier_agency_name_raw,
            # Amounts
            self.transaction_search.federal_action_obligation,
            self.transaction_search.generated_pragmatic_obligation,
            # Dates
            self.transaction_search.action_date,
            sf.year(self.transaction_search.fiscal_action_date).alias("action_date_fiscal_year"),
            self.transaction_search.initial_report_date,
            self.transaction_search.last_modified_date,
            self.transaction_search.period_of_performance_current_end_date,
            self.transaction_search.period_of_performance_start_date,
            # Offices
            self.transaction_search.awarding_office_code,
            self.transaction_search.awarding_office_name,
            self.transaction_search.funding_office_code,
            self.transaction_search.funding_office_name,
            # Place of Performance
            sf.when(
                self.transaction_search.pop_state_code.isNotNull()
                & self.transaction_search.pop_congressional_code_current.isNotNull()
                & (self.transaction_search.pop_state_code != ""),
                sf.concat(
                    self.transaction_search.pop_state_code,
                    sf.lit("-"),
                    self.transaction_search.pop_congressional_code_current,
                ),
            ).alias("pop_cd_current"),
            sf.when(
                self.transaction_search.pop_state_code.isNotNull()
                & self.transaction_search.pop_congressional_code.isNotNull()
                & (self.transaction_search.pop_state_code != ""),
                sf.concat(
                    self.transaction_search.pop_state_code,
                    sf.lit("-"),
                    self.transaction_search.pop_congressional_code,
                ),
            ).alias("pop_cd_original"),
            self.transaction_search.pop_city_name,
            self.transaction_search.pop_country_code,
            self.transaction_search.pop_country_name,
            self.transaction_search.pop_county_fips,
            self.transaction_search.pop_county_name,
            self.transaction_search.pop_state_fips,
            self.transaction_search.pop_state_code,
            self.transaction_search.pop_state_name,
            self.transaction_search.place_of_performance_zip4a,
            # Recipient
            self.transaction_search.recipient_unique_id,
            self.transaction_search.recipient_name,
            self.transaction_search.recipient_name_raw,
            self.transaction_search.recipient_uei,
            self.transaction_search.parent_recipient_unique_id,
            self.transaction_search.parent_recipient_name,
            self.transaction_search.parent_recipient_name_raw,
            self.transaction_search.parent_uei,
            # Recipient Location
            self.transaction_search.legal_entity_address_line1,
            self.transaction_search.legal_entity_address_line2,
            sf.when(
                self.transaction_search.recipient_location_state_code.isNotNull()
                & self.transaction_search.recipient_location_congressional_code_current.isNotNull()
                & (self.transaction_search.recipient_location_state_code != ""),
                sf.concat(
                    self.transaction_search.recipient_location_state_code,
                    sf.lit("-"),
                    self.transaction_search.recipient_location_congressional_code_current,
                ),
            ).alias("recipient_location_cd_current"),
            sf.when(
                self.transaction_search.recipient_location_state_code.isNotNull()
                & self.transaction_search.recipient_location_congressional_code.isNotNull()
                & (self.transaction_search.recipient_location_state_code != ""),
                sf.concat(
                    self.transaction_search.recipient_location_state_code,
                    sf.lit("-"),
                    self.transaction_search.recipient_location_congressional_code,
                ),
            ).alias("recipient_location_cd_original"),
            self.transaction_search.recipient_location_city_name,
            self.transaction_search.recipient_location_country_code,
            self.transaction_search.recipient_location_country_name,
            self.transaction_search.recipient_location_county_fips,
            self.transaction_search.recipient_location_county_name,
            self.transaction_search.recipient_location_state_code,
            self.transaction_search.recipient_location_state_fips,
            self.transaction_search.recipient_location_state_name,
            self.transaction_search.recipient_location_zip5,
            self.transaction_search.legal_entity_zip_last4,
            # Typing
            self.transaction_search.action_type,
            self.transaction_search.action_type_description,
            self.transaction_search.is_fpds,
            self.transaction_search.transaction_description,
        ]

    @property
    def fabs_cols(self) -> list[column]:
        return [
            # --- Award ---
            self.award_search.total_loan_value,
            self.award_search.total_subsidy_cost,
            self.transaction_search.non_federal_funding_amount.alias("total_non_federal_funding_amount"),
            # --- self.Transaction ---
            self.transaction_search.fain,
            self.transaction_search.uri,
            # Amounts
            self.transaction_search.face_value_loan_guarantee,
            self.transaction_search.indirect_federal_sharing,
            self.transaction_search.non_federal_funding_amount,
            self.transaction_search.original_loan_subsidy_cost,
            # Place of Performance
            self.transaction_search.place_of_performance_code,
            self.transaction_search.place_of_performance_forei,
            self.transaction_search.place_of_performance_scope,
            # Recipient Location
            self.transaction_search.legal_entity_foreign_city,
            self.transaction_search.legal_entity_foreign_posta,
            self.transaction_search.legal_entity_foreign_provi,
            # Additional FABS Fields
            self.transaction_search.business_funds_indicator,
            self.transaction_search.business_funds_ind_desc,
            self.transaction_search.business_types,
            self.transaction_search.business_types_desc,
            self.transaction_search.cfda_number,
            self.transaction_search.cfda_title,
            self.transaction_search.correction_delete_indicatr,
            self.transaction_search.correction_delete_ind_desc,
            self.transaction_search.funding_opportunity_number,
            self.transaction_search.funding_opportunity_goals,
            self.transaction_search.record_type,
            self.transaction_search.record_type_description,
            self.transaction_search.sai_number,
            self.transaction_search.type,
            self.transaction_search.type_description,
        ]

    @property
    def fpds_cols(self) -> list[column]:
        return [
            # -- self.Transaction ---
            self.transaction_search.piid,
            self.transaction_search.transaction_number,
            # Dates
            self.transaction_search.ordering_period_end_date,
            self.transaction_search.solicitation_date,
            # Officer Amounts
            self.transaction_search.officer_1_name,
            self.transaction_search.officer_1_amount,
            self.transaction_search.officer_2_name,
            self.transaction_search.officer_2_amount,
            self.transaction_search.officer_3_name,
            self.transaction_search.officer_3_amount,
            self.transaction_search.officer_4_name,
            self.transaction_search.officer_4_amount,
            self.transaction_search.officer_5_name,
            self.transaction_search.officer_5_amount,
            # Parent Award (for IDV)
            self.transaction_search.referenced_idv_agency_iden,
            self.transaction_search.referenced_idv_agency_desc,
            self.transaction_search.parent_award_id,
            self.transaction_search.referenced_idv_modificatio,
            self.transaction_search.referenced_mult_or_single,
            self.transaction_search.referenced_mult_or_si_desc,
            self.transaction_search.referenced_idv_type,
            self.transaction_search.referenced_idv_type_desc,
            # Recipient
            self.transaction_search.vendor_phone_number,
            self.transaction_search.vendor_fax_number,
            self.transaction_search.vendor_doing_as_business_n,
            # Recipient Location
            self.transaction_search.legal_entity_zip4,
            # Additional FPDS Fields
            self.transaction_search.a_76_fair_act_action,
            self.transaction_search.a_76_fair_act_action_desc,
            self.transaction_search.airport_authority,
            self.transaction_search.alaskan_native_owned_corpo,
            self.transaction_search.alaskan_native_servicing_i,
            self.transaction_search.american_indian_owned_busi,
            self.transaction_search.asian_pacific_american_own,
            self.transaction_search.pulled_from,
            self.transaction_search.base_and_all_options_value,
            self.transaction_search.base_exercised_options_val,
            self.transaction_search.black_american_owned_busin,
            self.transaction_search.c1862_land_grant_college,
            self.transaction_search.c1890_land_grant_college,
            self.transaction_search.c1994_land_grant_college,
            self.transaction_search.c8a_program_participant,
            self.transaction_search.cage_code,
            self.transaction_search.city_local_government,
            self.transaction_search.clinger_cohen_act_planning,
            self.transaction_search.clinger_cohen_act_pla_desc,
            self.transaction_search.commercial_item_acqui_desc,
            self.transaction_search.commercial_item_acquisitio,
            self.transaction_search.commercial_item_test_progr,
            self.transaction_search.commercial_item_test_desc,
            self.transaction_search.community_developed_corpor,
            self.transaction_search.community_development_corp,
            self.transaction_search.consolidated_contract,
            self.transaction_search.consolidated_contract_desc,
            self.transaction_search.construction_wage_rat_desc,
            self.transaction_search.construction_wage_rate_req,
            self.transaction_search.contingency_humanitar_desc,
            self.transaction_search.contingency_humanitarian_o,
            self.transaction_search.contract_award_type,
            self.transaction_search.contract_award_type_desc,
            self.transaction_search.contract_bundling,
            self.transaction_search.contract_bundling_descrip,
            self.transaction_search.contract_financing,
            self.transaction_search.contract_financing_descrip,
            self.transaction_search.contracting_officers_desc,
            self.transaction_search.contracting_officers_deter,
            self.transaction_search.contracts,
            self.transaction_search.corporate_entity_not_tax_e,
            self.transaction_search.corporate_entity_tax_exemp,
            self.transaction_search.cost_accounting_standards,
            self.transaction_search.cost_accounting_stand_desc,
            self.transaction_search.cost_or_pricing_data,
            self.transaction_search.cost_or_pricing_data_desc,
            self.transaction_search.council_of_governments,
            self.transaction_search.country_of_product_or_serv,
            self.transaction_search.country_of_product_or_desc,
            self.transaction_search.county_local_government,
            self.transaction_search.current_total_value_award,
            self.transaction_search.program_system_or_equipmen,
            self.transaction_search.program_system_or_equ_desc,
            self.transaction_search.dod_claimant_program_code,
            self.transaction_search.dod_claimant_prog_cod_desc,
            self.transaction_search.domestic_or_foreign_entity,
            self.transaction_search.domestic_or_foreign_e_desc,
            self.transaction_search.domestic_shelter,
            self.transaction_search.dot_certified_disadvantage,
            self.transaction_search.economically_disadvantaged,
            self.transaction_search.educational_institution,
            self.transaction_search.emerging_small_business,
            self.transaction_search.epa_designated_product,
            self.transaction_search.epa_designated_produc_desc,
            self.transaction_search.evaluated_preference,
            self.transaction_search.evaluated_preference_desc,
            self.transaction_search.extent_competed,
            self.transaction_search.extent_compete_description,
            self.transaction_search.fair_opportunity_limited_s,
            self.transaction_search.fair_opportunity_limi_desc,
            self.transaction_search.fed_biz_opps,
            self.transaction_search.fed_biz_opps_description,
            self.transaction_search.federal_agency,
            self.transaction_search.federally_funded_research,
            self.transaction_search.for_profit_organization,
            self.transaction_search.foreign_funding,
            self.transaction_search.foreign_funding_desc,
            self.transaction_search.foreign_government,
            self.transaction_search.foreign_owned_and_located,
            self.transaction_search.foundation,
            self.transaction_search.government_furnished_prope,
            self.transaction_search.government_furnished_desc,
            self.transaction_search.grants,
            self.transaction_search.hispanic_american_owned_bu,
            self.transaction_search.hispanic_servicing_institu,
            self.transaction_search.historically_black_college,
            self.transaction_search.historically_underutilized,
            self.transaction_search.hospital_flag,
            self.transaction_search.housing_authorities_public,
            self.transaction_search.idv_type,
            self.transaction_search.idv_type_description,
            self.transaction_search.information_technology_com,
            self.transaction_search.information_technolog_desc,
            self.transaction_search.indian_tribe_federally_rec,
            self.transaction_search.inherently_government_func,
            self.transaction_search.inherently_government_desc,
            self.transaction_search.inter_municipal_local_gove,
            self.transaction_search.interagency_contracting_au,
            self.transaction_search.interagency_contract_desc,
            self.transaction_search.international_organization,
            self.transaction_search.interstate_entity,
            self.transaction_search.joint_venture_economically,
            self.transaction_search.joint_venture_women_owned,
            self.transaction_search.labor_standards,
            self.transaction_search.labor_standards_descrip,
            self.transaction_search.labor_surplus_area_firm,
            self.transaction_search.limited_liability_corporat,
            self.transaction_search.local_area_set_aside,
            self.transaction_search.local_area_set_aside_desc,
            self.transaction_search.local_government_owned,
            self.transaction_search.major_program,
            self.transaction_search.manufacturer_of_goods,
            self.transaction_search.materials_supplies_article,
            self.transaction_search.materials_supplies_descrip,
            self.transaction_search.minority_institution,
            self.transaction_search.minority_owned_business,
            self.transaction_search.multi_year_contract,
            self.transaction_search.multi_year_contract_desc,
            self.transaction_search.multiple_or_single_award_i,
            self.transaction_search.multiple_or_single_aw_desc,
            self.transaction_search.municipality_local_governm,
            self.transaction_search.naics_code,
            self.transaction_search.naics_description,
            self.transaction_search.national_interest_action,
            self.transaction_search.national_interest_desc,
            self.transaction_search.native_american_owned_busi,
            self.transaction_search.native_hawaiian_owned_busi,
            self.transaction_search.native_hawaiian_servicing,
            self.transaction_search.nonprofit_organization,
            self.transaction_search.number_of_actions,
            self.transaction_search.number_of_offers_received,
            self.transaction_search.organizational_type,
            self.transaction_search.other_minority_owned_busin,
            self.transaction_search.other_not_for_profit_organ,
            self.transaction_search.other_statutory_authority,
            self.transaction_search.other_than_full_and_open_c,
            self.transaction_search.other_than_full_and_o_desc,
            self.transaction_search.partnership_or_limited_lia,
            self.transaction_search.performance_based_service,
            self.transaction_search.performance_based_se_desc,
            self.transaction_search.place_of_manufacture,
            self.transaction_search.place_of_manufacture_desc,
            self.transaction_search.planning_commission,
            self.transaction_search.port_authority,
            self.transaction_search.potential_total_value_awar,
            self.transaction_search.price_evaluation_adjustmen,
            self.transaction_search.private_university_or_coll,
            self.transaction_search.program_acronym,
            self.transaction_search.product_or_service_code,
            self.transaction_search.product_or_service_description,
            self.transaction_search.purchase_card_as_payment_m,
            self.transaction_search.purchase_card_as_paym_desc,
            self.transaction_search.recovered_materials_sustai,
            self.transaction_search.recovered_materials_s_desc,
            self.transaction_search.research,
            self.transaction_search.research_description,
            self.transaction_search.sam_exception,
            self.transaction_search.sam_exception_description,
            self.transaction_search.sba_certified_8_a_joint_ve,
            self.transaction_search.school_district_local_gove,
            self.transaction_search.school_of_forestry,
            self.transaction_search.sea_transportation,
            self.transaction_search.sea_transportation_desc,
            self.transaction_search.self_certified_small_disad,
            self.transaction_search.service_disabled_veteran_o,
            self.transaction_search.small_agricultural_coopera,
            self.transaction_search.small_business_competitive,
            self.transaction_search.small_disadvantaged_busine,
            self.transaction_search.sole_proprietorship,
            self.transaction_search.solicitation_identifier,
            self.transaction_search.state_controlled_instituti,
            self.transaction_search.subchapter_s_corporation,
            self.transaction_search.subcontinent_asian_asian_i,
            self.transaction_search.subcontracting_plan,
            self.transaction_search.subcontracting_plan_desc,
            self.transaction_search.the_ability_one_program,
            self.transaction_search.township_local_government,
            self.transaction_search.transit_authority,
            self.transaction_search.tribal_college,
            self.transaction_search.tribally_owned_business,
            self.transaction_search.type_of_contract_pricing,
            self.transaction_search.type_of_contract_pric_desc,
            self.transaction_search.type_of_idc,
            self.transaction_search.type_of_idc_description,
            self.transaction_search.type_set_aside,
            self.transaction_search.type_set_aside_description,
            self.transaction_search.undefinitized_action,
            self.transaction_search.undefinitized_action_desc,
            self.transaction_search.us_federal_government,
            self.transaction_search.us_government_entity,
            self.transaction_search.us_local_government,
            self.transaction_search.us_state_government,
            self.transaction_search.us_tribal_government,
            self.transaction_search.veteran_owned_business,
            self.transaction_search.veterinary_college,
            self.transaction_search.veterinary_hospital,
            self.transaction_search.woman_owned_business,
            self.transaction_search.women_owned_small_business,
        ]

    @property
    def dataframe(self) -> DataFrame:
        faba_cols = self.faba_aggs_df.columns
        faba_cols.remove("faba_award_id")
        df = (
            self.transaction_search.join(
                self.award_search, self.transaction_search.transaction_award_id == self.award_search.award_id
            )
            .join(
                self.faba_aggs_df,
                self.award_search.award_id == self.faba_aggs_df.faba_award_id,
                "left",
            )
            .select(*self.common_cols, *faba_cols, *self.fabs_cols, *self.fpds_cols)
            .withColumn("merge_hash_key", sf.xxhash64("*"))
            .repartition()
        )
        return df


def load_transaction_download(spark: SparkSession, destination_database: str, destination_table_name: str) -> None:
    df = TransactionDownload(spark).dataframe
    df.write.saveAsTable(
        f"{destination_database}.{destination_table_name}",
        mode="overwrite",
        format="delta",
    )


def load_transaction_download_incremental(
    spark: SparkSession, destination_database: str, destination_table_name: str
) -> None:
    target = DeltaTable.forName(spark, f"{destination_database}.{destination_table_name}").alias("t")
    source = TransactionDownload(spark).dataframe.alias("s")
    (
        target.merge(source, "s.transaction_id = t.transaction_id and s.merge_hash_key = t.merge_hash_key")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
