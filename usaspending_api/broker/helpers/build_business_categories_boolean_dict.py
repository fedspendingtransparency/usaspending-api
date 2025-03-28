fpds_boolean_columns = [
    "small_business_competitive",
    "city_local_government",
    "county_local_government",
    "inter_municipal_local_gove",
    "local_government_owned",
    "municipality_local_governm",
    "school_district_local_gove",
    "township_local_government",
    "us_state_government",
    "us_federal_government",
    "federal_agency",
    "federally_funded_research",
    "us_tribal_government",
    "foreign_government",
    "community_developed_corpor",
    "labor_surplus_area_firm",
    "corporate_entity_not_tax_e",
    "corporate_entity_tax_exemp",
    "partnership_or_limited_lia",
    "sole_proprietorship",
    "small_agricultural_coopera",
    "international_organization",
    "us_government_entity",
    "emerging_small_business",
    "c8a_program_participant",
    "sba_certified_8_a_joint_ve",
    "dot_certified_disadvantage",
    "self_certified_small_disad",
    "historically_underutilized",
    "small_disadvantaged_busine",
    "the_ability_one_program",
    "historically_black_college",
    "c1862_land_grant_college",
    "c1890_land_grant_college",
    "c1994_land_grant_college",
    "minority_institution",
    "private_university_or_coll",
    "school_of_forestry",
    "state_controlled_instituti",
    "tribal_college",
    "veterinary_college",
    "educational_institution",
    "alaskan_native_servicing_i",
    "community_development_corp",
    "native_hawaiian_servicing",
    "domestic_shelter",
    "manufacturer_of_goods",
    "hospital_flag",
    "veterinary_hospital",
    "hispanic_servicing_institu",
    "foundation",
    "woman_owned_business",
    "minority_owned_business",
    "women_owned_small_business",
    "economically_disadvantaged",
    "joint_venture_women_owned",
    "joint_venture_economically",
    "veteran_owned_business",
    "service_disabled_veteran_o",
    "contracts",
    "grants",
    "receives_contracts_and_gra",
    "airport_authority",
    "council_of_governments",
    "housing_authorities_public",
    "interstate_entity",
    "planning_commission",
    "port_authority",
    "transit_authority",
    "subchapter_s_corporation",
    "limited_liability_corporat",
    "foreign_owned_and_located",
    "american_indian_owned_busi",
    "alaskan_native_owned_corpo",
    "indian_tribe_federally_rec",
    "native_hawaiian_owned_busi",
    "tribally_owned_business",
    "asian_pacific_american_own",
    "black_american_owned_busin",
    "hispanic_american_owned_bu",
    "native_american_owned_busi",
    "subcontinent_asian_asian_i",
    "other_minority_owned_busin",
    "for_profit_organization",
    "nonprofit_organization",
    "other_not_for_profit_organ",
    "us_local_government",
]


def build_business_categories_boolean_dict(row):
    return {column: row.get(column, False) for column in fpds_boolean_columns}
