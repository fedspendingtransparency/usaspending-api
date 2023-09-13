d1_awards_sql_string = """
SELECT
    award_search.generated_unique_award_id AS contract_award_unique_key,
    award_search.piid AS award_id_piid,
    latest_transaction.referenced_idv_agency_iden AS parent_award_agency_id,
    latest_transaction.referenced_idv_agency_desc AS parent_award_agency_name,
    award_search.parent_award_piid AS parent_award_id_piid,
    COVID_DEFC.disaster_emergency_funds AS disaster_emergency_fund_codes,
    COVID_DEFC.gross_outlay_amount_by_award_cpe + COVID_DEFC.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe + COVID_DEFC.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS `outlayed_amount_from_COVID-19_supplementals`,
    COVID_DEFC.transaction_obligated_amount AS `obligated_amount_from_COVID-19_supplementals`,
    IIJA_DEFC.gross_outlay_amount_by_award_cpe + IIJA_DEFC.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe + IIJA_DEFC.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS outlayed_amount_from_IIJA_supplemental,
    IIJA_DEFC.transaction_obligated_amount AS obligated_amount_from_IIJA_supplemental,
    award_search.total_obligation AS total_obligated_amount,
    award_search.total_outlays AS total_outlayed_amount,
    latest_transaction.current_total_value_award AS current_total_value_of_award,
    latest_transaction.potential_total_value_awar AS potential_total_value_of_award,
    award_search.date_signed AS award_base_action_date,
    EXTRACT (YEAR FROM (award_search.date_signed) + INTERVAL 3 months) AS award_base_action_date_fiscal_year,
    latest_transaction.action_date AS award_latest_action_date,
    EXTRACT (YEAR FROM (TO_DATE(latest_transaction.action_date)) + INTERVAL 3 months) AS award_latest_action_date_fiscal_year,
    award_search.period_of_performance_start_date AS period_of_performance_start_date,
    latest_transaction.period_of_performance_current_end_date AS period_of_performance_current_end_date,
    latest_transaction.period_of_perf_potential_e AS period_of_performance_potential_end_date,
    latest_transaction.ordering_period_end_date AS ordering_period_end_date,
    earliest_transaction.solicitation_date AS solicitation_date,
    latest_transaction.awarding_agency_code AS awarding_agency_code,
    latest_transaction.awarding_toptier_agency_name AS awarding_agency_name,
    latest_transaction.awarding_sub_tier_agency_c AS awarding_sub_agency_code,
    latest_transaction.awarding_subtier_agency_name AS awarding_sub_agency_name,
    latest_transaction.awarding_office_code AS awarding_office_code,
    latest_transaction.awarding_office_name AS awarding_office_name,
    latest_transaction.funding_agency_code AS funding_agency_code,
    latest_transaction.funding_toptier_agency_name AS funding_agency_name,
    latest_transaction.funding_sub_tier_agency_co AS funding_sub_agency_code,
    latest_transaction.funding_subtier_agency_name AS funding_sub_agency_name,
    latest_transaction.funding_office_code AS funding_office_code,
    latest_transaction.funding_office_name AS funding_office_name,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(U2.tas_rendering_label))) AS value FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) LEFT OUTER JOIN global_temp.treasury_appropriation_account U2 ON (U1.treasury_account_id = U2.treasury_account_identifier) WHERE U0.award_id = (award_search.award_id) GROUP BY U0.award_id) AS treasury_accounts_funding_this_award,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(U3.federal_account_code))) AS value FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) LEFT OUTER JOIN global_temp.treasury_appropriation_account U2 ON (U1.treasury_account_id = U2.treasury_account_identifier) LEFT OUTER JOIN global_temp.federal_account U3 ON (U2.federal_account_id = U3.id) WHERE U0.award_id = (award_search.award_id) GROUP BY U0.award_id) AS federal_accounts_funding_this_award,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(CONCAT(U2.object_class, ':', U2.object_class_name)))) FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) INNER JOIN global_temp.object_class U2 ON (U1.object_class_id = U2.id) WHERE U0.award_id = (award_search.award_id) and U1.object_class_id IS NOT NULL GROUP BY U0.award_id) AS object_classes_funding_this_award,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(CONCAT(U2.program_activity_code, ':', U2.program_activity_name)))) FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) INNER JOIN global_temp.ref_program_activity U2 ON (U1.program_activity_id = U2.id) WHERE U0.award_id = (award_search.award_id) and U1.program_activity_id IS NOT NULL GROUP BY U0.award_id) AS program_activities_funding_this_award,
    latest_transaction.foreign_funding AS foreign_funding,
    latest_transaction.foreign_funding_desc AS foreign_funding_description,
    latest_transaction.sam_exception AS sam_exception,
    latest_transaction.sam_exception_description AS sam_exception_description,
    latest_transaction.recipient_unique_id AS recipient_duns,
    latest_transaction.recipient_uei AS recipient_uei,
    latest_transaction.recipient_name_raw AS recipient_name,
    latest_transaction.vendor_doing_as_business_n AS recipient_doing_business_as_name,
    latest_transaction.cage_code AS cage_code,
    latest_transaction.parent_recipient_unique_id AS recipient_parent_duns,
    latest_transaction.parent_uei AS recipient_parent_uei,
    latest_transaction.parent_recipient_name AS recipient_parent_name,
    latest_transaction.recipient_location_country_code AS recipient_country_code,
    latest_transaction.recipient_location_country_name AS recipient_country_name,
    latest_transaction.legal_entity_address_line1 AS recipient_address_line_1,
    latest_transaction.legal_entity_address_line2 AS recipient_address_line_2,
    latest_transaction.recipient_location_city_name AS recipient_city_name,
    latest_transaction.recipient_location_county_name AS recipient_county_name,
    latest_transaction.recipient_location_state_code AS recipient_state_code,
    latest_transaction.recipient_location_state_name AS recipient_state_name,
    latest_transaction.legal_entity_zip4 AS recipient_zip_4_code,
    CASE
        WHEN latest_transaction.recipient_location_state_code IS NOT NULL
            AND latest_transaction.recipient_location_congressional_code IS NOT NULL
            AND latest_transaction.recipient_location_state_code != ''
        THEN CONCAT(latest_transaction.recipient_location_state_code, '-', latest_transaction.recipient_location_congressional_code)
        ELSE latest_transaction.recipient_location_congressional_code
    END AS prime_award_summary_recipient_cd_original,
    CASE
        WHEN latest_transaction.recipient_location_state_code IS NOT NULL
            AND latest_transaction.recipient_location_congressional_code_current IS NOT NULL
            AND latest_transaction.recipient_location_state_code != ''
        THEN CONCAT(latest_transaction.recipient_location_state_code, '-', latest_transaction.recipient_location_congressional_code_current)
        ELSE latest_transaction.recipient_location_congressional_code_current
    END AS prime_award_summary_recipient_cd_current,
    latest_transaction.vendor_phone_number AS recipient_phone_number,
    latest_transaction.vendor_fax_number AS recipient_fax_number,
    latest_transaction.pop_country_code AS primary_place_of_performance_country_code,
    latest_transaction.pop_country_name AS primary_place_of_performance_country_name,
    latest_transaction.pop_city_name AS primary_place_of_performance_city_name,
    latest_transaction.pop_county_name AS primary_place_of_performance_county_name,
    latest_transaction.pop_state_code AS primary_place_of_performance_state_code,
    latest_transaction.pop_state_name AS primary_place_of_performance_state_name,
    latest_transaction.place_of_performance_zip4a AS primary_place_of_performance_zip_4,
    CASE
        WHEN latest_transaction.pop_state_code IS NOT NULL
            AND latest_transaction.pop_congressional_code IS NOT NULL
            AND latest_transaction.pop_state_code != ''
        THEN CONCAT(latest_transaction.pop_state_code, '-', latest_transaction.pop_congressional_code)
        ELSE latest_transaction.pop_congressional_code
    END AS prime_award_summary_place_of_performance_cd_original,
    CASE
        WHEN latest_transaction.pop_state_code IS NOT NULL
            AND latest_transaction.pop_congressional_code_current IS NOT NULL
            AND latest_transaction.pop_state_code != ''
        THEN CONCAT(latest_transaction.pop_state_code, '-', latest_transaction.pop_congressional_code_current)
        ELSE latest_transaction.pop_congressional_code_current
    END AS prime_award_summary_place_of_performance_cd_current,
    latest_transaction.pulled_from AS award_or_idv_flag,
    latest_transaction.contract_award_type AS award_type_code,
    latest_transaction.contract_award_type_desc AS award_type,
    latest_transaction.idv_type AS idv_type_code,
    latest_transaction.idv_type_description AS idv_type,
    latest_transaction.multiple_or_single_award_i AS multiple_or_single_award_idv_code,
    latest_transaction.multiple_or_single_aw_desc AS multiple_or_single_award_idv,
    latest_transaction.type_of_idc AS type_of_idc_code,
    latest_transaction.type_of_idc_description AS type_of_idc,
    latest_transaction.type_of_contract_pricing AS type_of_contract_pricing_code,
    latest_transaction.type_of_contract_pric_desc AS type_of_contract_pricing,
    award_search.description AS prime_award_base_transaction_description,
    latest_transaction.solicitation_identifier AS solicitation_identifier,
    latest_transaction.number_of_actions AS number_of_actions,
    latest_transaction.inherently_government_func AS inherently_governmental_functions,
    latest_transaction.inherently_government_desc AS inherently_governmental_functions_description,
    latest_transaction.product_or_service_code AS product_or_service_code,
    latest_transaction.product_or_service_description AS product_or_service_code_description,
    latest_transaction.contract_bundling AS contract_bundling_code,
    latest_transaction.contract_bundling_descrip AS contract_bundling,
    latest_transaction.dod_claimant_program_code AS dod_claimant_program_code,
    latest_transaction.dod_claimant_prog_cod_desc AS dod_claimant_program_description,
    latest_transaction.naics_code AS naics_code,
    latest_transaction.naics_description AS naics_description,
    latest_transaction.recovered_materials_sustai AS recovered_materials_sustainability_code,
    latest_transaction.recovered_materials_s_desc AS recovered_materials_sustainability,
    latest_transaction.domestic_or_foreign_entity AS domestic_or_foreign_entity_code,
    latest_transaction.domestic_or_foreign_e_desc AS domestic_or_foreign_entity,
    latest_transaction.program_system_or_equipmen AS dod_acquisition_program_code,
    latest_transaction.program_system_or_equ_desc AS dod_acquisition_program_description,
    latest_transaction.information_technology_com AS information_technology_commercial_item_category_code,
    latest_transaction.information_technolog_desc AS information_technology_commercial_item_category,
    latest_transaction.epa_designated_product AS epa_designated_product_code,
    latest_transaction.epa_designated_produc_desc AS epa_designated_product,
    latest_transaction.country_of_product_or_serv AS country_of_product_or_service_origin_code,
    latest_transaction.country_of_product_or_desc AS country_of_product_or_service_origin,
    latest_transaction.place_of_manufacture AS place_of_manufacture_code,
    latest_transaction.place_of_manufacture_desc AS place_of_manufacture,
    latest_transaction.subcontracting_plan AS subcontracting_plan_code,
    latest_transaction.subcontracting_plan_desc AS subcontracting_plan,
    latest_transaction.extent_competed AS extent_competed_code,
    latest_transaction.extent_compete_description AS extent_competed,
    latest_transaction.solicitation_procedures AS solicitation_procedures_code,
    latest_transaction.solicitation_procedur_desc AS solicitation_procedures,
    latest_transaction.type_set_aside AS type_of_set_aside_code,
    latest_transaction.type_set_aside_description AS type_of_set_aside,
    latest_transaction.evaluated_preference AS evaluated_preference_code,
    latest_transaction.evaluated_preference_desc AS evaluated_preference,
    latest_transaction.research AS research_code,
    latest_transaction.research_description AS research,
    latest_transaction.fair_opportunity_limited_s AS fair_opportunity_limited_sources_code,
    latest_transaction.fair_opportunity_limi_desc AS fair_opportunity_limited_sources,
    latest_transaction.other_than_full_and_open_c AS other_than_full_and_open_competition_code,
    latest_transaction.other_than_full_and_o_desc AS other_than_full_and_open_competition,
    latest_transaction.number_of_offers_received AS number_of_offers_received,
    latest_transaction.commercial_item_acquisitio AS commercial_item_acquisition_procedures_code,
    latest_transaction.commercial_item_acqui_desc AS commercial_item_acquisition_procedures,
    latest_transaction.small_business_competitive AS small_business_competitiveness_demonstration_program,
    latest_transaction.commercial_item_test_progr AS simplified_procedures_for_certain_commercial_items_code,
    latest_transaction.commercial_item_test_desc AS simplified_procedures_for_certain_commercial_items,
    latest_transaction.a_76_fair_act_action AS a76_fair_act_action_code,
    latest_transaction.a_76_fair_act_action_desc AS a76_fair_act_action,
    latest_transaction.fed_biz_opps AS fed_biz_opps_code,
    latest_transaction.fed_biz_opps_description AS fed_biz_opps,
    latest_transaction.local_area_set_aside AS local_area_set_aside_code,
    latest_transaction.local_area_set_aside_desc AS local_area_set_aside,
    latest_transaction.price_evaluation_adjustmen AS price_evaluation_adjustment_preference_percent_difference,
    latest_transaction.clinger_cohen_act_planning AS clinger_cohen_act_planning_code,
    latest_transaction.clinger_cohen_act_pla_desc AS clinger_cohen_act_planning,
    latest_transaction.materials_supplies_article AS materials_supplies_articles_equipment_code,
    latest_transaction.materials_supplies_descrip AS materials_supplies_articles_equipment,
    latest_transaction.labor_standards AS labor_standards_code,
    latest_transaction.labor_standards_descrip AS labor_standards,
    latest_transaction.construction_wage_rate_req AS construction_wage_rate_requirements_code,
    latest_transaction.construction_wage_rat_desc AS construction_wage_rate_requirements,
    latest_transaction.interagency_contracting_au AS interagency_contracting_authority_code,
    latest_transaction.interagency_contract_desc AS interagency_contracting_authority,
    latest_transaction.other_statutory_authority AS other_statutory_authority,
    latest_transaction.program_acronym AS program_acronym,
    latest_transaction.referenced_idv_type AS parent_award_type_code,
    latest_transaction.referenced_idv_type_desc AS parent_award_type,
    latest_transaction.referenced_mult_or_single AS parent_award_single_or_multiple_code,
    latest_transaction.referenced_mult_or_si_desc AS parent_award_single_or_multiple,
    latest_transaction.major_program AS major_program,
    latest_transaction.national_interest_action AS national_interest_action_code,
    latest_transaction.national_interest_desc AS national_interest_action,
    latest_transaction.cost_or_pricing_data AS cost_or_pricing_data_code,
    latest_transaction.cost_or_pricing_data_desc AS cost_or_pricing_data,
    latest_transaction.cost_accounting_standards AS cost_accounting_standards_clause_code,
    latest_transaction.cost_accounting_stand_desc AS cost_accounting_standards_clause,
    latest_transaction.government_furnished_prope AS government_furnished_property_code,
    latest_transaction.government_furnished_prope AS government_furnished_property,
    latest_transaction.sea_transportation AS sea_transportation_code,
    latest_transaction.sea_transportation_desc AS sea_transportation,
    latest_transaction.consolidated_contract AS consolidated_contract_code,
    latest_transaction.consolidated_contract_desc AS consolidated_contract,
    latest_transaction.performance_based_service AS performance_based_service_acquisition_code,
    latest_transaction.performance_based_se_desc AS performance_based_service_acquisition,
    latest_transaction.multi_year_contract AS multi_year_contract_code,
    latest_transaction.multi_year_contract_desc AS multi_year_contract,
    latest_transaction.contract_financing AS contract_financing_code,
    latest_transaction.contract_financing_descrip AS contract_financing,
    latest_transaction.purchase_card_as_payment_m AS purchase_card_as_payment_method_code,
    latest_transaction.purchase_card_as_paym_desc AS purchase_card_as_payment_method,
    latest_transaction.contingency_humanitarian_o AS contingency_humanitarian_or_peacekeeping_operation_code,
    latest_transaction.contingency_humanitar_desc AS contingency_humanitarian_or_peacekeeping_operation,
    latest_transaction.alaskan_native_owned_corpo AS alaskan_native_corporation_owned_firm,
    latest_transaction.american_indian_owned_busi AS american_indian_owned_business,
    latest_transaction.indian_tribe_federally_rec AS indian_tribe_federally_recognized,
    latest_transaction.native_hawaiian_owned_busi AS native_hawaiian_organization_owned_firm,
    latest_transaction.tribally_owned_business AS tribally_owned_firm,
    latest_transaction.veteran_owned_business AS veteran_owned_business,
    latest_transaction.service_disabled_veteran_o AS service_disabled_veteran_owned_business,
    latest_transaction.woman_owned_business AS woman_owned_business,
    latest_transaction.women_owned_small_business AS women_owned_small_business,
    latest_transaction.economically_disadvantaged AS economically_disadvantaged_women_owned_small_business,
    latest_transaction.joint_venture_women_owned AS joint_venture_women_owned_small_business,
    latest_transaction.joint_venture_economically AS joint_venture_economic_disadvantaged_women_owned_small_bus,
    latest_transaction.minority_owned_business AS minority_owned_business,
    latest_transaction.subcontinent_asian_asian_i AS subcontinent_asian_asian_indian_american_owned_business,
    latest_transaction.asian_pacific_american_own AS asian_pacific_american_owned_business,
    latest_transaction.black_american_owned_busin AS black_american_owned_business,
    latest_transaction.hispanic_american_owned_bu AS hispanic_american_owned_business,
    latest_transaction.native_american_owned_busi AS native_american_owned_business,
    latest_transaction.other_minority_owned_busin AS other_minority_owned_business,
    latest_transaction.contracting_officers_desc AS contracting_officers_determination_of_business_size,
    latest_transaction.contracting_officers_deter AS contracting_officers_determination_of_business_size_code,
    latest_transaction.emerging_small_business AS emerging_small_business,
    latest_transaction.community_developed_corpor AS community_developed_corporation_owned_firm,
    latest_transaction.labor_surplus_area_firm AS labor_surplus_area_firm,
    latest_transaction.us_federal_government AS us_federal_government,
    latest_transaction.federally_funded_research AS federally_funded_research_and_development_corp,
    latest_transaction.federal_agency AS federal_agency,
    latest_transaction.us_state_government AS us_state_government,
    latest_transaction.us_local_government AS us_local_government,
    latest_transaction.city_local_government AS city_local_government,
    latest_transaction.county_local_government AS county_local_government,
    latest_transaction.inter_municipal_local_gove AS inter_municipal_local_government,
    latest_transaction.local_government_owned AS local_government_owned,
    latest_transaction.municipality_local_governm AS municipality_local_government,
    latest_transaction.school_district_local_gove AS school_district_local_government,
    latest_transaction.township_local_government AS township_local_government,
    latest_transaction.us_tribal_government AS us_tribal_government,
    latest_transaction.foreign_government AS foreign_government,
    latest_transaction.organizational_type AS organizational_type,
    latest_transaction.corporate_entity_not_tax_e AS corporate_entity_not_tax_exempt,
    latest_transaction.corporate_entity_tax_exemp AS corporate_entity_tax_exempt,
    latest_transaction.partnership_or_limited_lia AS partnership_or_limited_liability_partnership,
    latest_transaction.sole_proprietorship AS sole_proprietorship,
    latest_transaction.small_agricultural_coopera AS small_agricultural_cooperative,
    latest_transaction.international_organization AS international_organization,
    latest_transaction.us_government_entity AS us_government_entity,
    latest_transaction.community_development_corp AS community_development_corporation,
    latest_transaction.domestic_shelter AS domestic_shelter,
    latest_transaction.educational_institution AS educational_institution,
    latest_transaction.foundation AS foundation,
    latest_transaction.hospital_flag AS hospital_flag,
    latest_transaction.manufacturer_of_goods AS manufacturer_of_goods,
    latest_transaction.veterinary_hospital AS veterinary_hospital,
    latest_transaction.hispanic_servicing_institu AS hispanic_servicing_institution,
    latest_transaction.contracts AS receives_contracts,
    latest_transaction.grants AS receives_financial_assistance,
    latest_transaction.receives_contracts_and_gra AS receives_contracts_and_financial_assistance,
    latest_transaction.airport_authority AS airport_authority,
    latest_transaction.council_of_governments AS council_of_governments,
    latest_transaction.housing_authorities_public AS housing_authorities_public_tribal,
    latest_transaction.interstate_entity AS interstate_entity,
    latest_transaction.planning_commission AS planning_commission,
    latest_transaction.port_authority AS port_authority,
    latest_transaction.transit_authority AS transit_authority,
    latest_transaction.subchapter_s_corporation AS subchapter_scorporation,
    latest_transaction.limited_liability_corporat AS limited_liability_corporation,
    latest_transaction.foreign_owned_and_located AS foreign_owned,
    latest_transaction.for_profit_organization AS for_profit_organization,
    latest_transaction.nonprofit_organization AS nonprofit_organization,
    latest_transaction.other_not_for_profit_organ AS other_not_for_profit_organization,
    latest_transaction.the_ability_one_program AS the_ability_one_program,
    latest_transaction.private_university_or_coll AS private_university_or_college,
    latest_transaction.state_controlled_instituti AS state_controlled_institution_of_higher_learning,
    latest_transaction.c1862_land_grant_college AS 1862_land_grant_college,
    latest_transaction.c1890_land_grant_college AS 1890_land_grant_college,
    latest_transaction.c1994_land_grant_college AS 1994_land_grant_college,
    latest_transaction.minority_institution AS minority_institution,
    latest_transaction.historically_black_college AS historically_black_college,
    latest_transaction.tribal_college AS tribal_college,
    latest_transaction.alaskan_native_servicing_i AS alaskan_native_servicing_institution,
    latest_transaction.native_hawaiian_servicing AS native_hawaiian_servicing_institution,
    latest_transaction.school_of_forestry AS school_of_forestry,
    latest_transaction.veterinary_college AS veterinary_college,
    latest_transaction.dot_certified_disadvantage AS dot_certified_disadvantage,
    latest_transaction.self_certified_small_disad AS self_certified_small_disadvantaged_business,
    latest_transaction.small_disadvantaged_busine AS small_disadvantaged_business,
    latest_transaction.c8a_program_participant AS c8a_program_participant,
    latest_transaction.historically_underutilized AS historically_underutilized_business_zone_hubzone_firm,
    latest_transaction.sba_certified_8_a_joint_ve AS sba_certified_8a_joint_venture,
    award_search.officer_1_name AS highly_compensated_officer_1_name,
    award_search.officer_1_amount AS highly_compensated_officer_1_amount,
    award_search.officer_2_name AS highly_compensated_officer_2_name,
    award_search.officer_2_amount AS highly_compensated_officer_2_amount,
    award_search.officer_3_name AS highly_compensated_officer_3_name,
    award_search.officer_3_amount AS highly_compensated_officer_3_amount,
    award_search.officer_4_name AS highly_compensated_officer_4_name,
    award_search.officer_4_amount AS highly_compensated_officer_4_amount,
    award_search.officer_5_name AS highly_compensated_officer_5_name,
    award_search.officer_5_amount AS highly_compensated_officer_5_amount,
    /*
        Use REPLACE for the asterisk (*) because it is considered a reserved character in
        the Java URLEncoder and therefore won't be replaced by the encode() function.
        This is to match the behavior of our custom urlencode() function that was written
        in PostgresSQL which did not consider the asterisk (*) to be a reserved character.
    */
    CONCAT('https://www.usaspending.gov/award/', REPLACE(REFLECT('java.net.URLEncoder','encode', award_search.generated_unique_award_id, 'UTF-8'), '*', '%2A'), '/') AS usaspending_permalink,
    STRING(latest_transaction.last_modified_date) AS last_modified_date
FROM rpt.award_search
INNER JOIN rpt.transaction_search AS latest_transaction ON (latest_transaction.is_fpds = TRUE AND award_search.latest_transaction_id = latest_transaction.transaction_id)
INNER JOIN rpt.transaction_search AS earliest_transaction ON (earliest_transaction.is_fpds = TRUE AND award_search.earliest_transaction_id = earliest_transaction.transaction_id)
INNER JOIN (
    SELECT
        faba.award_id,
        CONCAT_WS('; ', SORT_ARRAY(COLLECT_SET(CONCAT(disaster_emergency_fund_code, ': ', public_law)))) AS disaster_emergency_funds,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe END), 0) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe END), 0) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM int.financial_accounts_by_awards faba
    INNER JOIN global_temp.disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN global_temp.submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2020-04-01'
    INNER JOIN global_temp.dabs_submission_window_schedule ON (
        sa.submission_window_id = global_temp.dabs_submission_window_schedule.id
        AND global_temp.dabs_submission_window_schedule.submission_reveal_date <= now()
    )
    WHERE
        faba.award_id IS NOT NULL
    GROUP BY
        faba.award_id
    HAVING
        COALESCE(
            SUM(
                CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                    THEN
                        COALESCE(faba.gross_outlay_amount_by_award_cpe, 0)
                        + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                        + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                END
            ),
            0
        ) != 0
        OR COALESCE(SUM(faba.transaction_obligated_amount), 0) != 0
) COVID_DEFC ON (COVID_DEFC.award_id = award_search.award_id)
LEFT OUTER JOIN (
    SELECT
        faba.award_id,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe END), 0) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe END), 0) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM int.financial_accounts_by_awards faba
    INNER JOIN global_temp.disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'infrastructure'
    INNER JOIN global_temp.submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2021-11-15'
    INNER JOIN global_temp.dabs_submission_window_schedule ON (
        sa.submission_window_id = global_temp.dabs_submission_window_schedule.id
        AND global_temp.dabs_submission_window_schedule.submission_reveal_date <= now()
    )
    WHERE faba.award_id IS NOT NULL
    GROUP BY
        faba.award_id
    HAVING
        COALESCE(
            SUM(
                CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                    THEN
                        COALESCE(faba.gross_outlay_amount_by_award_cpe, 0)
                        + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                        + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                END
            ),
            0
        ) != 0
        OR COALESCE(SUM(faba.transaction_obligated_amount), 0) != 0
) IIJA_DEFC
ON IIJA_DEFC.award_id = rpt.award_search.award_id
"""
