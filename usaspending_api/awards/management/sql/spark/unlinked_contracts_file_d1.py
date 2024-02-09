file_d1_sql_string = """
    select
      ts.detached_award_proc_unique as contract_transaction_unique_key,
      ts.generated_unique_award_id as contract_award_unique_key,
      ts.piid as award_id_piid,
      ts.modification_number as modification_number,
      ts.transaction_number as transaction_number,
      ts.referenced_idv_agency_iden as parent_award_agency_id,
      ts.referenced_idv_agency_desc as parent_award_agency_name,
      ts.parent_award_id as parent_award_id_piid,
      ts.referenced_idv_modificatio as parent_award_modification_number,
      ts.federal_action_obligation as federal_action_obligation,
      ts.total_obligated_amount as total_dollars_obligated,
      (
      select
        ((coalesce(SUM(U0.gross_outlay_amount_by_award_cpe), 0) + coalesce(SUM(U0.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe), 0)) + coalesce(SUM(U0.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe), 0)) as sum
      from
        int.financial_accounts_by_awards U0
      inner join global_temp.submission_attributes U1 on
        (U0.submission_id = U1.submission_id)
      where
        (U1.is_final_balances_for_fy
          and U0.award_id = ts.award_id)
      group by
        U0.award_id
      having
        (SUM(U0.gross_outlay_amount_by_award_cpe) is not null
          or SUM(U0.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe) is not null
            or SUM(U0.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe) is not null)) as total_outlayed_amount_for_overall_award,
      ts.base_exercised_options_val as base_and_exercised_options_value,
      ts.current_total_value_award as current_total_value_of_award,
      ts.base_and_all_options_value as base_and_all_options_value,
      ts.potential_total_value_awar as potential_total_value_of_award,
      (
      select
        ((coalesce(SUM(U0.gross_outlay_amount_by_award_cpe), 0) + coalesce(SUM(U0.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe), 0)) + coalesce(SUM(U0.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe), 0)) as sum
      from
        int.financial_accounts_by_awards U0
      inner join global_temp.submission_attributes U1 on
        (U0.submission_id = U1.submission_id)
      inner join global_temp.disaster_emergency_fund_code U3 on
        (U0.disaster_emergency_fund_code = U3.code)
      where
        (U1.is_final_balances_for_fy
          and U0.award_id = ts.award_id
          and U3.group_name = 'covid_19')
      group by
        U0.award_id) as `outlayed_amount_from_COVID-19_supplementals_for_overall_award`,
      (
      select
        SUM(U0.transaction_obligated_amount) as sum
      from
        int.financial_accounts_by_awards U0
      inner join global_temp.submission_attributes U1 on
        (U0.submission_id = U1.submission_id)
      inner join global_temp.disaster_emergency_fund_code U3 on
        (U0.disaster_emergency_fund_code = U3.code)
      where
        (((U1.reporting_fiscal_year = 2021
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2022
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2023
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 11)
        or (U1.reporting_fiscal_year = 2020
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2020
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2017
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2023
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 9)
        or (U1.reporting_fiscal_year = 2022
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2018
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2019
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2021
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12))
        and U0.award_id = ts.award_id
        and U3.group_name = 'covid_19')
      group by
        U0.award_id) as `obligated_amount_from_COVID-19_supplementals_for_overall_award`,
      (
      select
        ((coalesce(SUM(U0.gross_outlay_amount_by_award_cpe), 0) + coalesce(SUM(U0.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe), 0)) + coalesce(SUM(U0.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe), 0)) as sum
      from
        int.financial_accounts_by_awards U0
      inner join global_temp.submission_attributes U1 on
        (U0.submission_id = U1.submission_id)
      inner join global_temp.disaster_emergency_fund_code U3 on
        (U0.disaster_emergency_fund_code = U3.code)
      where
        (U1.is_final_balances_for_fy
          and U0.award_id = ts.award_id
          and U3.group_name = 'infrastructure')
      group by
        U0.award_id) as outlayed_amount_from_IIJA_supplemental_for_overall_award,
      (
      select
        SUM(U0.transaction_obligated_amount) as sum
      from
        int.financial_accounts_by_awards U0
      inner join global_temp.submission_attributes U1 on
        (U0.submission_id = U1.submission_id)
      inner join global_temp.disaster_emergency_fund_code U3 on
        (U0.disaster_emergency_fund_code = U3.code)
      where
        (((U1.reporting_fiscal_year = 2021
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2022
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2023
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 11)
        or (U1.reporting_fiscal_year = 2020
          and not U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2020
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2017
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2023
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 9)
        or (U1.reporting_fiscal_year = 2022
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2018
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2019
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12)
        or (U1.reporting_fiscal_year = 2021
          and U1.quarter_format_flag
          and U1.reporting_fiscal_period <= 12))
        and U0.award_id = ts.award_id
        and U3.group_name = 'infrastructure')
      group by
        U0.award_id) as obligated_amount_from_IIJA_supplemental_for_overall_award,
      ts.action_date as action_date,
      extract(year
    from
      (ts.action_date) + interval '3 months') as action_date_fiscal_year,
      ts.period_of_performance_start_date as period_of_performance_start_date,
      ts.period_of_performance_current_end_date as period_of_performance_current_end_date,
      ts.period_of_perf_potential_e as period_of_performance_potential_end_date,
      ts.ordering_period_end_date as ordering_period_end_date,
      ts.solicitation_date as solicitation_date,
      ts.awarding_agency_code as awarding_agency_code,
      ts.awarding_toptier_agency_name as awarding_agency_name,
      ts.awarding_sub_tier_agency_c as awarding_sub_agency_code,
      ts.awarding_subtier_agency_name as awarding_sub_agency_name,
      ts.awarding_office_code as awarding_office_code,
      ts.awarding_office_name as awarding_office_name,
      ts.funding_agency_code as funding_agency_code,
      ts.funding_toptier_agency_name as funding_agency_name,
      ts.funding_sub_tier_agency_co as funding_sub_agency_code,
      ts.funding_subtier_agency_name as funding_sub_agency_name,
      ts.funding_office_code as funding_office_code,
      ts.funding_office_name as funding_office_name,
      U2.tas_rendering_label as treasury_accounts_funding_this_award,
      ts.foreign_funding as foreign_funding,
      ts.foreign_funding_desc as foreign_funding_description,
      ts.sam_exception as sam_exception,
      ts.sam_exception_description as sam_exception_description,
      ts.recipient_uei as recipient_uei,
      ts.recipient_unique_id as recipient_duns,
      ts.recipient_name as recipient_name,
      ts.recipient_name_raw as recipient_name_raw,
      ts.vendor_doing_as_business_n as recipient_doing_business_as_name,
      ts.cage_code as cage_code,
      ts.parent_uei as recipient_parent_uei,
      ts.parent_recipient_unique_id as recipient_parent_duns,
      ts.parent_recipient_name as recipient_parent_name,
      ts.parent_recipient_name_raw as recipient_parent_name_raw,
      ts.recipient_location_country_code as recipient_country_code,
      ts.recipient_location_country_name as recipient_country_name,
      ts.legal_entity_address_line1 as recipient_address_line_1,
      ts.legal_entity_address_line2 as recipient_address_line_2,
      ts.recipient_location_city_name as recipient_city_name,
      ts.recipient_location_county_fips as prime_award_transaction_recipient_county_fips_code,
      ts.recipient_location_county_name as recipient_county_name,
      ts.recipient_location_state_fips as prime_award_transaction_recipient_state_fips_code,
      ts.recipient_location_state_code as recipient_state_code,
      ts.recipient_location_state_name as recipient_state_name,
      ts.legal_entity_zip4 as recipient_zip_4_code,
      case
        when (ts.recipient_location_state_code is not null
          and ts.recipient_location_congressional_code is not null
          and not (ts.recipient_location_state_code = ''
            and ts.recipient_location_state_code is not null)) then CONCAT(ts.recipient_location_state_code, '-', ts.recipient_location_congressional_code)
        else ts.recipient_location_congressional_code
      end as prime_award_transaction_recipient_cd_original,
      case
        when (ts.recipient_location_state_code is not null
          and ts.recipient_location_congressional_code_current is not null
          and not (ts.recipient_location_state_code = ''
            and ts.recipient_location_state_code is not null)) then CONCAT(ts.recipient_location_state_code, '-', ts.recipient_location_congressional_code_current)
        else ts.recipient_location_congressional_code_current
      end as prime_award_transaction_recipient_cd_current,
      ts.vendor_phone_number as recipient_phone_number,
      ts.vendor_fax_number as recipient_fax_number,
      ts.pop_country_code as primary_place_of_performance_country_code,
      ts.pop_country_name as primary_place_of_performance_country_name,
      ts.pop_city_name as primary_place_of_performance_city_name,
      ts.pop_county_fips as prime_award_transaction_place_of_performance_county_fips_code,
      ts.pop_county_name as primary_place_of_performance_county_name,
      ts.pop_state_fips as prime_award_transaction_place_of_performance_state_fips_code,
      ts.pop_state_code as primary_place_of_performance_state_code,
      ts.pop_state_name as primary_place_of_performance_state_name,
      ts.place_of_performance_zip4a as primary_place_of_performance_zip_4,
      case
        when (ts.pop_state_code is not null
          and ts.pop_congressional_code is not null
          and not (ts.pop_state_code = ''
            and ts.pop_state_code is not null)) then CONCAT(ts.pop_state_code, '-', ts.pop_congressional_code)
        else ts.pop_congressional_code
      end as prime_award_transaction_place_of_performance_cd_original,
      case
        when (ts.pop_state_code is not null
          and ts.pop_congressional_code_current is not null
          and not (ts.pop_state_code = ''
            and ts.pop_state_code is not null)) then CONCAT(ts.pop_state_code, '-', ts.pop_congressional_code_current)
        else ts.pop_congressional_code_current
      end as prime_award_transaction_place_of_performance_cd_current,
      ts.pulled_from as award_or_idv_flag,
      ts.contract_award_type as award_type_code,
      ts.contract_award_type_desc as award_type,
      ts.idv_type as idv_type_code,
      ts.idv_type_description as idv_type,
      ts.multiple_or_single_award_i as multiple_or_single_award_idv_code,
      ts.multiple_or_single_aw_desc as multiple_or_single_award_idv,
      ts.type_of_idc as type_of_idc_code,
      ts.type_of_idc_description as type_of_idc,
      ts.type_of_contract_pricing as type_of_contract_pricing_code,
      ts.type_of_contract_pric_desc as type_of_contract_pricing,
      ts.transaction_description as transaction_description,
      ras.description as prime_award_base_transaction_description,
      ts.action_type as action_type_code,
      ts.action_type_description as action_type,
      ts.solicitation_identifier as solicitation_identifier,
      ts.number_of_actions as number_of_actions,
      ts.inherently_government_func as inherently_governmental_functions,
      ts.inherently_government_desc as inherently_governmental_functions_description,
      ts.product_or_service_code as product_or_service_code,
      ts.product_or_service_description as product_or_service_code_description,
      ts.contract_bundling as contract_bundling_code,
      ts.contract_bundling_descrip as contract_bundling,
      ts.dod_claimant_program_code as dod_claimant_program_code,
      ts.dod_claimant_prog_cod_desc as dod_claimant_program_description,
      ts.naics_code as naics_code,
      ts.naics_description as naics_description,
      ts.recovered_materials_sustai as recovered_materials_sustainability_code,
      ts.recovered_materials_s_desc as recovered_materials_sustainability,
      ts.domestic_or_foreign_entity as domestic_or_foreign_entity_code,
      ts.domestic_or_foreign_e_desc as domestic_or_foreign_entity,
      ts.program_system_or_equipmen as dod_acquisition_program_code,
      ts.program_system_or_equ_desc as dod_acquisition_program_description,
      ts.information_technology_com as information_technology_commercial_item_category_code,
      ts.information_technolog_desc as information_technology_commercial_item_category,
      ts.epa_designated_product as epa_designated_product_code,
      ts.epa_designated_produc_desc as epa_designated_product,
      ts.country_of_product_or_serv as country_of_product_or_service_origin_code,
      ts.country_of_product_or_desc as country_of_product_or_service_origin,
      ts.place_of_manufacture as place_of_manufacture_code,
      ts.place_of_manufacture_desc as place_of_manufacture,
      ts.subcontracting_plan as subcontracting_plan_code,
      ts.subcontracting_plan_desc as subcontracting_plan,
      ts.extent_competed as extent_competed_code,
      ts.extent_compete_description as extent_competed,
      ts.solicitation_procedures as solicitation_procedures_code,
      ts.solicitation_procedur_desc as solicitation_procedures,
      ts.type_set_aside as type_of_set_aside_code,
      ts.type_set_aside_description as type_of_set_aside,
      ts.evaluated_preference as evaluated_preference_code,
      ts.evaluated_preference_desc as evaluated_preference,
      ts.research as research_code,
      ts.research_description as research,
      ts.fair_opportunity_limited_s as fair_opportunity_limited_sources_code,
      ts.fair_opportunity_limi_desc as fair_opportunity_limited_sources,
      ts.other_than_full_and_open_c as other_than_full_and_open_competition_code,
      ts.other_than_full_and_o_desc as other_than_full_and_open_competition,
      ts.number_of_offers_received as number_of_offers_received,
      ts.commercial_item_acquisitio as commercial_item_acquisition_procedures_code,
      ts.commercial_item_acqui_desc as commercial_item_acquisition_procedures,
      ts.small_business_competitive as small_business_competitiveness_demonstration_program,
      ts.commercial_item_test_progr as simplified_procedures_for_certain_commercial_items_code,
      ts.commercial_item_test_desc as simplified_procedures_for_certain_commercial_items,
      ts.a_76_fair_act_action as a76_fair_act_action_code,
      ts.a_76_fair_act_action_desc as a76_fair_act_action,
      ts.fed_biz_opps as fed_biz_opps_code,
      ts.fed_biz_opps_description as fed_biz_opps,
      ts.local_area_set_aside as local_area_set_aside_code,
      ts.local_area_set_aside_desc as local_area_set_aside,
      ts.price_evaluation_adjustmen as price_evaluation_adjustment_preference_percent_difference,
      ts.clinger_cohen_act_planning as clinger_cohen_act_planning_code,
      ts.clinger_cohen_act_pla_desc as clinger_cohen_act_planning,
      ts.materials_supplies_article as materials_supplies_articles_equipment_code,
      ts.materials_supplies_descrip as materials_supplies_articles_equipment,
      ts.labor_standards as labor_standards_code,
      ts.labor_standards_descrip as labor_standards,
      ts.construction_wage_rate_req as construction_wage_rate_requirements_code,
      ts.construction_wage_rat_desc as construction_wage_rate_requirements,
      ts.interagency_contracting_au as interagency_contracting_authority_code,
      ts.interagency_contract_desc as interagency_contracting_authority,
      ts.other_statutory_authority as other_statutory_authority,
      ts.program_acronym as program_acronym,
      ts.referenced_idv_type as parent_award_type_code,
      ts.referenced_idv_type_desc as parent_award_type,
      ts.referenced_mult_or_single as parent_award_single_or_multiple_code,
      ts.referenced_mult_or_si_desc as parent_award_single_or_multiple,
      ts.major_program as major_program,
      ts.national_interest_action as national_interest_action_code,
      ts.national_interest_desc as national_interest_action,
      ts.cost_or_pricing_data as cost_or_pricing_data_code,
      ts.cost_or_pricing_data_desc as cost_or_pricing_data,
      ts.cost_accounting_standards as cost_accounting_standards_clause_code,
      ts.cost_accounting_stand_desc as cost_accounting_standards_clause,
      ts.government_furnished_prope as government_furnished_property_code,
      ts.government_furnished_desc as government_furnished_property,
      ts.sea_transportation as sea_transportation_code,
      ts.sea_transportation_desc as sea_transportation,
      ts.undefinitized_action as undefinitized_action_code,
      ts.undefinitized_action_desc as undefinitized_action,
      ts.consolidated_contract as consolidated_contract_code,
      ts.consolidated_contract_desc as consolidated_contract,
      ts.performance_based_service as performance_based_service_acquisition_code,
      ts.performance_based_se_desc as performance_based_service_acquisition,
      ts.multi_year_contract as multi_year_contract_code,
      ts.multi_year_contract_desc as multi_year_contract,
      ts.contract_financing as contract_financing_code,
      ts.contract_financing_descrip as contract_financing,
      ts.purchase_card_as_payment_m as purchase_card_as_payment_method_code,
      ts.purchase_card_as_paym_desc as purchase_card_as_payment_method,
      ts.contingency_humanitarian_o as contingency_humanitarian_or_peacekeeping_operation_code,
      ts.contingency_humanitar_desc as contingency_humanitarian_or_peacekeeping_operation,
      ts.alaskan_native_owned_corpo as alaskan_native_corporation_owned_firm,
      ts.american_indian_owned_busi as american_indian_owned_business,
      ts.indian_tribe_federally_rec as indian_tribe_federally_recognized,
      ts.native_hawaiian_owned_busi as native_hawaiian_organization_owned_firm,
      ts.tribally_owned_business as tribally_owned_firm,
      ts.veteran_owned_business as veteran_owned_business,
      ts.service_disabled_veteran_o as service_disabled_veteran_owned_business,
      ts.woman_owned_business as woman_owned_business,
      ts.women_owned_small_business as women_owned_small_business,
      ts.economically_disadvantaged as economically_disadvantaged_women_owned_small_business,
      ts.joint_venture_women_owned as joint_venture_women_owned_small_business,
      ts.joint_venture_economically as joint_venture_economic_disadvantaged_women_owned_small_bus,
      ts.minority_owned_business as minority_owned_business,
      ts.subcontinent_asian_asian_i as subcontinent_asian_asian_indian_american_owned_business,
      ts.asian_pacific_american_own as asian_pacific_american_owned_business,
      ts.black_american_owned_busin as black_american_owned_business,
      ts.hispanic_american_owned_bu as hispanic_american_owned_business,
      ts.native_american_owned_busi as native_american_owned_business,
      ts.other_minority_owned_busin as other_minority_owned_business,
      ts.contracting_officers_desc as contracting_officers_determination_of_business_size,
      ts.contracting_officers_deter as contracting_officers_determination_of_business_size_code,
      ts.emerging_small_business as emerging_small_business,
      ts.community_developed_corpor as community_developed_corporation_owned_firm,
      ts.labor_surplus_area_firm as labor_surplus_area_firm,
      ts.us_federal_government as us_federal_government,
      ts.federally_funded_research as federally_funded_research_and_development_corp,
      ts.federal_agency as federal_agency,
      ts.us_state_government as us_state_government,
      ts.us_local_government as us_local_government,
      ts.city_local_government as city_local_government,
      ts.county_local_government as county_local_government,
      ts.inter_municipal_local_gove as inter_municipal_local_government,
      ts.local_government_owned as local_government_owned,
      ts.municipality_local_governm as municipality_local_government,
      ts.school_district_local_gove as school_district_local_government,
      ts.township_local_government as township_local_government,
      ts.us_tribal_government as us_tribal_government,
      ts.foreign_government as foreign_government,
      ts.organizational_type as organizational_type,
      ts.corporate_entity_not_tax_e as corporate_entity_not_tax_exempt,
      ts.corporate_entity_tax_exemp as corporate_entity_tax_exempt,
      ts.partnership_or_limited_lia as partnership_or_limited_liability_partnership,
      ts.sole_proprietorship as sole_proprietorship,
      ts.small_agricultural_coopera as small_agricultural_cooperative,
      ts.international_organization as international_organization,
      ts.us_government_entity as us_government_entity,
      ts.community_development_corp as community_development_corporation,
      ts.domestic_shelter as domestic_shelter,
      ts.educational_institution as educational_institution,
      ts.foundation as foundation,
      ts.hospital_flag as hospital_flag,
      ts.manufacturer_of_goods as manufacturer_of_goods,
      ts.veterinary_hospital as veterinary_hospital,
      ts.hispanic_servicing_institu as hispanic_servicing_institution,
      ts.contracts as receives_contracts,
      ts.grants as receives_financial_assistance,
      ts.receives_contracts_and_gra as receives_contracts_and_financial_assistance,
      ts.airport_authority as airport_authority,
      ts.council_of_governments as council_of_governments,
      ts.housing_authorities_public as housing_authorities_public_tribal,
      ts.interstate_entity as interstate_entity,
      ts.planning_commission as planning_commission,
      ts.port_authority as port_authority,
      ts.transit_authority as transit_authority,
      ts.subchapter_s_corporation as subchapter_scorporation,
      ts.limited_liability_corporat as limited_liability_corporation,
      ts.foreign_owned_and_located as foreign_owned,
      ts.for_profit_organization as for_profit_organization,
      ts.nonprofit_organization as nonprofit_organization,
      ts.other_not_for_profit_organ as other_not_for_profit_organization,
      ts.the_ability_one_program as the_ability_one_program,
      ts.private_university_or_coll as private_university_or_college,
      ts.state_controlled_instituti as state_controlled_institution_of_higher_learning,
      ts.c1862_land_grant_college as 1862_land_grant_college,
      ts.c1890_land_grant_college as 1890_land_grant_college,
      ts.c1994_land_grant_college as 1994_land_grant_college,
      ts.minority_institution as minority_institution,
      ts.historically_black_college as historically_black_college,
      ts.tribal_college as tribal_college,
      ts.alaskan_native_servicing_i as alaskan_native_servicing_institution,
      ts.native_hawaiian_servicing as native_hawaiian_servicing_institution,
      ts.school_of_forestry as school_of_forestry,
      ts.veterinary_college as veterinary_college,
      ts.dot_certified_disadvantage as dot_certified_disadvantage,
      ts.self_certified_small_disad as self_certified_small_disadvantaged_business,
      ts.small_disadvantaged_busine as small_disadvantaged_business,
      ts.c8a_program_participant as c8a_program_participant,
      ts.historically_underutilized as historically_underutilized_business_zone_hubzone_firm,
      ts.sba_certified_8_a_joint_ve as sba_certified_8a_joint_venture,
      ts.officer_1_name as highly_compensated_officer_1_name,
      ts.officer_1_amount as highly_compensated_officer_1_amount,
      ts.officer_2_name as highly_compensated_officer_2_name,
      ts.officer_2_amount as highly_compensated_officer_2_amount,
      ts.officer_3_name as highly_compensated_officer_3_name,
      ts.officer_3_amount as highly_compensated_officer_3_amount,
      ts.officer_4_name as highly_compensated_officer_4_name,
      ts.officer_4_amount as highly_compensated_officer_4_amount,
      ts.officer_5_name as highly_compensated_officer_5_name,
      ts.officer_5_amount as highly_compensated_officer_5_amount,
      ts.initial_report_date as initial_report_date,
      ts.last_modified_date as last_modified_date,
      U1.reporting_fiscal_period as period,
      U1.reporting_fiscal_year as fiscal_year
    from
      rpt.transaction_search ts
    left outer join rpt.award_search ras on
      (ts.award_id = ras.award_id)
    left join global_temp.financial_accounts_by_awards U0
      on U0.award_id = ts.award_id
    left outer join global_temp.treasury_appropriation_account U2 on
      U0.treasury_account_id = U2.treasury_account_identifier
    left join global_temp.submission_attributes U1 on
        (U0.submission_id = U1.submission_id)
    where
      ts.awarding_toptier_agency_name = {agency_name}
        and ts.type in ('A', 'B', 'C', 'D', 'IDV_A', 'IDV_B', 'IDV_B_A', 'IDV_B_B', 'IDV_B_C', 'IDV_C', 'IDV_D', 'IDV_E')
        and ts.is_fpds = true
        and treasury_accounts_funding_this_award is null
"""
