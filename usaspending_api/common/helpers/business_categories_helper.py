# Dictionary of Business Categories that pair them with their human readable name
BUSINESS_CATEGORIES_LOOKUP_DICT = {
    # Category Business
    "category_business": "Category Business",
    "small_business": "Small Business",
    "other_than_small_business": "Not Designated a Small Business",
    "corporate_entity_tax_exempt": "Corporate Entity Tax Exempt",
    "corporate_entity_not_tax_exempt": "Corporate Entity Not Tax Exempt",
    "partnership_or_limited_liability_partnership": "Partnership or Limited Liability Partnership",
    "sole_proprietorship": "Sole Proprietorship",
    "manufacturer_of_goods": "Manufacturer of Goods",
    "subchapter_s_corporation": "Subchapter S Corporation",
    "limited_liability_corporation": "Limited Liability Corporation",
    # Minority Owned Business
    "minority_owned_business": "Minority Owned Business",
    "alaskan_native_corporation_owned_firm": "Alaskan Native Corporation Owned Firm",
    "american_indian_owned_business": "American Indian Owned Business",
    "asian_pacific_american_owned_business": "Asian Pacific American Owned Business",
    "black_american_owned_business": "Black American Owned Business",
    "hispanic_american_owned_business": "Hispanic American Owned Business",
    "native_american_owned_business": "Native American Owned Business",
    "native_hawaiian_organization_owned_firm": "Native Hawaiian Organization Owned Firm",
    "subcontinent_asian_indian_american_owned_business": "Indian (Subcontinent) American Owned Business",
    "tribally_owned_firm": "Tribally Owned Firm",
    "other_minority_owned_business": "Other Minority Owned Business",
    # Women Owned Business
    "woman_owned_business": "Woman Owned Business",
    "women_owned_small_business": "Women Owned Small Business",
    "economically_disadvantaged_women_owned_small_business": "Economically Disadvantaged Women Owned Small Business",
    "joint_venture_women_owned_small_business": "Joint Venture Women Owned Small Business",
    "joint_venture_economically_disadvantaged_women_owned_small_business": "Joint Venture Economically Disadvantaged Women Owned Small Business",
    # Veteran Owned Business
    "veteran_owned_business": "Veteran Owned Business",
    "service_disabled_veteran_owned_business": "Service Disabled Veteran Owned Business",
    # Special Designations
    "special_designations": "Special Designations",
    "8a_program_participant": "8(a) Program Participant",
    "ability_one_program": "AbilityOne Program Participant",
    "dot_certified_disadvantaged_business_enterprise": "DoT Certified Disadvantaged Business Enterprise",
    "emerging_small_business": "Emerging Small Business",
    "federally_funded_research_and_development_corp": "Federally Funded Research and Development Corp",
    "historically_underutilized_business_firm": "HUBZone Firm",
    "labor_surplus_area_firm": "Labor Surplus Area Firm",
    "sba_certified_8a_joint_venture": "SBA Certified 8 a Joint Venture",
    "self_certified_small_disadvanted_business": "Self-Certified Small Disadvantaged Business",
    "small_agricultural_cooperative": "Small Agricultural Cooperative",
    "small_disadvantaged_business": "Small Disadvantaged Business",
    "community_developed_corporation_owned_firm": "Community Developed Corporation Owned Firm",
    "us_owned_business": "U.S.-Owned Business",
    "foreign_owned_and_us_located_business": "Foreign-Owned and U.S.-Incorporated Business",
    "foreign_owned": "Foreign Owned",
    "foreign_government": "Foreign Government",
    "international_organization": "International Organization",
    "domestic_shelter": "Domestic Shelter",
    "hospital": "Hospital",
    "veterinary_hospital": "Veterinary Hospital",
    # Nonprofit
    "nonprofit": "Nonprofit Organization",
    "foundation": "Foundation",
    "community_development_corporations": "Community Development Corporation",
    # Higher education
    "higher_education": "Higher Education",
    "public_institution_of_higher_education": "Higher Education (Public)",
    "private_institution_of_higher_education": "Higher Education (Private)",
    "minority_serving_institution_of_higher_education": "Higher Education (Minority Serving)",
    "educational_institution": "Educational Institution",
    "school_of_forestry": "School of Forestry",
    "veterinary_college": "Veterinary College",
    # Government
    "government": "Government",
    "national_government": "U.S. National Government",
    "regional_and_state_government": "U.S. Regional/State Government",
    "regional_organization": "U.S. Regional Government Organization",
    "interstate_entity": "U.S. Interstate Government Entity",
    "us_territory_or_possession": "U.S. Territory Government",
    "local_government": "U.S. Local Government",
    "indian_native_american_tribal_government": "Native American Tribal Government",
    "authorities_and_commissions": "U.S. Government Authorities",
    "council_of_governments": "Council of Governments",
    # Individuals
    "individuals": "Individuals",
}


def get_business_category_display_names(business_category_list):
    business_category_display_name_list = []
    for business_category in business_category_list:
        display_name = BUSINESS_CATEGORIES_LOOKUP_DICT.get(business_category)
        if display_name:
            business_category_display_name_list.append(display_name)
    return business_category_display_name_list
