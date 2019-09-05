from datetime import datetime

from usaspending_api.common.helpers.generic_helper import fy
from usaspending_api.data_load.data_load_helpers import subtier_agency_list


def calculate_fiscal_year(broker_input):
    return fy(broker_input["action_date"])


def calculate_awarding_agency(broker_input):
    awarding_agency = subtier_agency_list.get(broker_input["awarding_sub_tier_agency_c"], None)
    if awarding_agency is not None:
        return awarding_agency["id"]
    else:
        return None


def calculate_funding_agency(broker_input):
    funding_agency = subtier_agency_list.get(broker_input["funding_sub_tier_agency_co"], None)
    if funding_agency is not None:
        return funding_agency["id"]
    else:
        return None


def unique_transaction_id(broker_input):
    return broker_input["detached_award_proc_unique"]


def now(broker_input):
    return datetime.now()


def business_categories(broker_input):
    retval = []

    if broker_input["contracting_officers_deter"] == "S" or\
    broker_input["women_owned_small_business"] or\
    broker_input["economically_disadvantaged"] or\
    broker_input["joint_venture_women_owned"] or\
    broker_input["emerging_small_business"] or\
    broker_input["self_certified_small_disad"] or\
    broker_input["small_agricultural_coopera"] or\
    broker_input["small_disadvantaged_busine"]:
        retval.append("small_business")

    if broker_input["contracting_officers_deter"] == "O":
        retval.append("other_than_small_business")

    if broker_input["corporate_entity_tax_exemp"] == "Y":
        retval.append("corporate_entity_tax_exempt")

    if broker_input["corporate_entity_not_tax_e"] == "Y":
        retval.append("corporate_entity_not_tax_exempt")

    if broker_input["partnership_or_limited_lia"] == "Y":
        retval.append("partnership_or_limited_liability_partnership")

    if broker_input["sole_proprietorship"] == "Y":
        retval.append("sole_proprietorship")

    if broker_input["manufacturer_of_goods"] == "Y":
        retval.append("manufacturer_of_goods")

    if broker_input["subchapter_s_corporation"] == "Y":
        retval.append("subchapter_s_corporation")

    if broker_input["limited_liability_corporat"] == "Y":
        retval.append("limited_liability_corporation")

    if broker_input["for_profit_organization"] == "Y" or\
        any(x in retval for x in ["small_business",
        "other_than_small_business",
        "corporate_entity_tax_exempt",
        "corporate_entity_not_tax_exempt",
        "partnership_or_limited_liability_partnership",
        "sole_proprietorship",
        "manufacturer_of_goods",
        "subchapter_s_corporation",
        "limited_liability_corporation"]):
        retval.append("category_business")

    if broker_input["alaskan_native_owned_corpo"] == "Y":
        retval.append("alaskan_native_owned_business")

    if broker_input["american_indian_owned_busi"] == "Y":
        retval.append("american_indian_owned_business")

    if broker_input["asian_pacific_american_own"] == "Y":
        retval.append("asian_pacific_american_owned_business")

    if broker_input["black_american_owned_busin"] == "Y":
        retval.append("black_american_owned_business")

    if broker_input["hispanic_american_owned_bu"] == "Y":
        retval.append("hispanic_american_owned_business")

    if broker_input["native_american_owned_busi"] == "Y":
        retval.append("native_american_owned_business")

    if broker_input["native_hawaiian_owned_busi"] == "Y":
        retval.append("native_hawaiian_owned_business")

    if broker_input["subcontinent_asian_asian_i"] == "Y":
        retval.append("subcontinent_asian_indian_american_owned_business")

    if broker_input["tribally_owned_business"] == "Y":
        retval.append("tribally_owned_business")

    if broker_input["other_minority_owned_busin"] == "Y":
        retval.append("other_minority_owned_business")

    if broker_input["minority_owned_business"] == "Y" or\
        any(x in retval for x in ["alaskan_native_owned_business",
        "american_indian_owned_business",
        "asian_pacific_american_owned_business",
        "black_american_owned_business",
        "hispanic_american_owned_business",
        "native_american_owned_business",
        "native_hawaiian_owned_business",
        "subcontinent_asian_indian_american_owned_business",
        "tribally_owned_business",
        "other_minority_owned_business"]):
        retval.append("minority_owned_business")

    if broker_input["women_owned_small_business"] == "Y":
        retval.append("women_owned_small_business")

    if broker_input["economically_disadvantaged"] == "Y":
        retval.append("economically_disadvantaged_women_owned_small_business")

    if broker_input["joint_venture_women_owned"] == "Y":
        retval.append("joint_venture_women_owned_small_business")

    if broker_input["joint_venture_economically"] == "Y":
        retval.append("joint_venture_economically_disadvantaged_women_owned_small_business")

    if broker_input["woman_owned_business"] == "Y" or\
        any(x in retval for x in ["women_owned_small_business",
        "economically_disadvantaged_women_owned_small_business",
        "joint_venture_women_owned_small_business",
        "joint_venture_economically_disadvantaged_women_owned_small_business"]):
        retval.append("woman_owned_business")

    if broker_input["service_disabled_veteran_o"] == "Y":
        retval.append("service_disabled_veteran_owned_business")
        retval.append("veteran_owned_business")
    elif broker_input["veteran_owned_business"] == "Y":
        retval.append("veteran_owned_business")

    if broker_input["c8a_program_participant"] == "Y":
        retval.append("8a_program_participant")

    if broker_input["the_ability_one_program"] == "Y":
        retval.append("ability_one_program")

    if broker_input["dot_certified_disadvantage"] == "Y":
        retval.append("dot_certified_disadvantaged_business_enterprise")

    if broker_input["federally_funded_research"] == "Y":
        retval.append("federally_funded_research_and_development_corp")

    if broker_input["historically_underutilized"] == "Y":
        retval.append("historically_underutilized_business_firm")

    if broker_input["labor_surplus_area_firm"] == "Y":
        retval.append("labor_surplus_area_firm")

    if broker_input["sba_certified_8_a_joint_ve"] == "Y":
        retval.append("sba_certified_8_a_joint_ve")

    if broker_input["self_certified_small_disad"] == "Y":
        retval.append("self_certified_small_disadvanted_business")

    if broker_input["small_agricultural_coopera"] == "Y":
        retval.append("small_agricultural_cooperative")

    if broker_input["small_disadvantaged_busine"] == "Y":
        retval.append("small_disadvantaged_business")

    if broker_input["community_developed_corpor"] == "Y":
        retval.append("community_developed_corporation_owned_firm")

    if broker_input["domestic_or_foreign_entity"] == "A":
        retval.append("us_owned_business")

    if broker_input["domestic_or_foreign_entity"] == "C":
        retval.append("foreign_owned_and_us_located_business")

    if broker_input["domestic_or_foreign_entity"] == "D" or\
            broker_input["foreign_owned_and_located"] == "Y":
        retval.append("foreign_owned_and_located_business")

    if broker_input["foreign_government"] == "Y":
        retval.append("foreign_government")

    if broker_input["international_organization"] == "Y":
        retval.append("international_organization")

    if broker_input["domestic_shelter"] == "Y":
        retval.append("domestic_shelter")

    if broker_input["hospital_flag"] == "Y":
        retval.append("hospital")

    if broker_input["veterinary_hospital"] == "Y":
        retval.append("veterinary_hospital")

    if any(x in retval for x in ["8a_program_participant",
        "ability_one_program",
        "dot_certified_disadvantaged_business_enterprise",
        "emerging_small_business",
        "federally_funded_research_and_development_corp",
        "historically_underutilized_business_firm",
        "labor_surplus_area_firm",
        "sba_certified_8a_joint_venture",
        "self_certified_small_disadvanted_business",
        "small_agricultural_cooperative",
        "small_disadvantaged_business",
        "community_developed_corporation_owned_firm",
        "us_owned_business",
        "foreign_owned_and_us_located_business",
        "foreign_owned_and_located_business",
        "foreign_government",
        "international_organization",
        "domestic_shelter",
        "hospital",
        "veterinary_hospital"]):
        retval.append("special_designations")

    if broker_input["foundation"] == "Y":
        retval.append("foundation")

    if broker_input["community_development_corp"] == "Y":
        retval.append("community_development_corporations")

    if broker_input["nonprofit_organization"] == "Y" or \
            broker_input["other_not_for_profit_organ"] == "Y" or \
            any(x in retval for x in ["foundation",
            "community_development_corporations"]):
        retval.append("nonprofit")

    if broker_input["educational_institution"] == "Y":
        retval.append("educational_institution")

    if broker_input["state_controlled_instituti"] == "Y" or broker_input["c1862_land_grant_college"] == "Y" \
            or broker_input["c1890_land_grant_college"] == "Y" or broker_input["c1994_land_grant_college"] == "Y":
        retval.append("public_institution_of_higher_education")

    if broker_input["private_university_or_coll"] == "Y":
        retval.append("private_institution_of_higher_education")

    if broker_input["minority_institution"] == "Y" or broker_input["historically_black_college"] == "Y" \
            or broker_input["tribal_college"] == "Y" or broker_input["alaskan_native_servicing_i"] == "Y"\
            or broker_input["native_hawaiian_servicing"] == "Y" or broker_input["hispanic_servicing_institu"] == "Y":
        retval.append("minority_serving_institution_of_higher_education")

    if broker_input["school_of_forestry"] == "Y":
        retval.append("school_of_forestry")

    if broker_input["veterinary_college"] == "Y":
        retval.append("veterinary_college")
            
    if any(x in retval for x in ["8a_program_participant",
        "educational_institution",
        "public_institution_of_higher_education",
        "private_institution_of_higher_education",
        "minority_serving_institution_of_higher_education",
        "school_of_forestry",
        "veterinary_college"]):
        retval.append("higher_education")

    if broker_input["us_federal_government"] == "Y" or broker_input["federal_agency"] == "Y" \
            or broker_input["us_government_entity"] == "Y":
        retval.append("national_government")

    if broker_input["interstate_entity"] == "Y":
        retval.append("interstate_entity")

    if broker_input["us_state_government"] == "Y":
        retval.append("regional_and_state_government")

    if broker_input["council_of_governments"] == "Y":
        retval.append("council_of_governments")

    if broker_input["city_local_government"] == "Y" or broker_input["county_local_government"] == "Y" \
            or broker_input["inter_municipal_local_gove"] == "Y" or broker_input["municipality_local_governm"] == "Y"\
            or broker_input["township_local_government"] == "Y" or broker_input["us_local_government"] == "Y"\
            or broker_input["local_government_owned"] == "Y" or broker_input["school_district_local_gove"] == "Y":
        retval.append("local_government")

    if broker_input["us_tribal_government"] == "Y" or broker_input["indian_tribe_federally_rec"] == "Y":
        retval.append("indian_native_american_tribal_government")
        
    if broker_input["housing_authorities_public"] == "Y" or broker_input["airport_authority"] == "Y" \
            or broker_input["port_authority"] == "Y"\
            or broker_input["transit_authority"] == "Y" or broker_input["planning_commission"] == "Y":
        retval.append("authorities_and_commissions")

    if any(x in retval for x in ["national_government",
        "regional_and_state_government",
        "local_government",
        "indian_native_american_tribal_government",
        "authorities_and_commissions",
        "interstate_entity",
        "council_of_governments"]):
        retval.append("government")

    return retval
