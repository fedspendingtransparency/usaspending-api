-- Please replace the second conditional to whatever you need to use
-- to limit which rows are updated for performance

-- FPDS
UPDATE transaction_normalized AS tn
SET business_categories = compile_fpds_business_categories(
    tf.contracting_officers_deter,
    tf.corporate_entity_tax_exemp,
    tf.corporate_entity_not_tax_e,
    tf.partnership_or_limited_lia,
    tf.sole_proprietorship,
    tf.manufacturer_of_goods,
    tf.subchapter_s_corporation,
    tf.limited_liability_corporat,
    tf.for_profit_organization,
    tf.alaskan_native_owned_corpo,
    tf.american_indian_owned_busi,
    tf.asian_pacific_american_own,
    tf.black_american_owned_busin,
    tf.hispanic_american_owned_bu,
    tf.native_american_owned_busi,
    tf.native_hawaiian_owned_busi,
    tf.subcontinent_asian_asian_i,
    tf.tribally_owned_business,
    tf.other_minority_owned_busin,
    tf.minority_owned_business,
    tf.women_owned_small_business,
    tf.economically_disadvantaged,
    tf.joint_venture_women_owned,
    tf.joint_venture_economically,
    tf.woman_owned_business,
    tf.service_disabled_veteran_o,
    tf.veteran_owned_business,
    tf.c8a_program_participant,
    tf.the_ability_one_program,
    tf.dot_certified_disadvantage,
    tf.emerging_small_business,
    tf.federally_funded_research,
    tf.historically_underutilized,
    tf.labor_surplus_area_firm,
    tf.sba_certified_8_a_joint_ve,
    tf.self_certified_small_disad,
    tf.small_agricultural_coopera,
    tf.small_disadvantaged_busine,
    tf.community_developed_corpor,
    tf.domestic_or_foreign_entity,
    tf.foreign_owned_and_located,
    tf.foreign_government,
    tf.international_organization,
    tf.domestic_shelter,
    tf.hospital_flag,
    tf.veterinary_hospital,
    tf.foundation,
    tf.community_development_corp,
    tf.nonprofit_organization,
    tf.educational_institution,
    tf.other_not_for_profit_organ,
    tf.state_controlled_instituti,
    tf.c1862_land_grant_college,
    tf.c1890_land_grant_college,
    tf.c1994_land_grant_college,
    tf.private_university_or_coll,
    tf.minority_institution,
    tf.historically_black_college,
    tf.tribal_college,
    tf.alaskan_native_servicing_i,
    tf.native_hawaiian_servicing,
    tf.hispanic_servicing_institu,
    tf.school_of_forestry,
    tf.veterinary_college,
    tf.us_federal_government,
    tf.federal_agency,
    tf.us_government_entity,
    tf.interstate_entity,
    tf.us_state_government,
    tf.council_of_governments,
    tf.city_local_government,
    tf.county_local_government,
    tf.inter_municipal_local_gove,
    tf.municipality_local_governm,
    tf.township_local_government,
    tf.us_local_government,
    tf.local_government_owned,
    tf.school_district_local_gove,
    tf.us_tribal_government,
    tf.indian_tribe_federally_rec,
    tf.housing_authorities_public,
    tf.airport_authority,
    tf.port_authority,
    tf.transit_authority,
    tf.planning_commission
)
FROM transaction_fpds AS tf
WHERE tn.is_fpds IS TRUE AND
      tn.id = tf.transaction_id;

-- FABS
UPDATE transaction_normalized AS tn
SET business_categories = compile_fabs_business_categories(
    tf.business_types
)
FROM transaction_fabs AS tf
WHERE tn.is_fpds IS FALSE AND
      tn.id = tf.transaction_id;
