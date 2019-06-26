# Dictionary of Business Categories that pair them with their human readable name
BUSINESS_CATEGORIES_LOOKUP = [
    # Category Business
    {"display_name": "Category Business", "field_name": "category_business"},
    {"display_name": "Small Business", "field_name": "small_business"},
    {"display_name": "Not Designated a Small Business", "field_name": "other_than_small_business"},
    {"display_name": "Corporate Entity Tax Exempt", "field_name": "corporate_entity_tax_exempt"},
    {"display_name": "Corporate Entity Not Tax Exempt", "field_name": "corporate_entity_not_tax_exempt"},
    {
        "display_name": "Partnership or Limited Liability Partnership",
        "field_name": "partnership_or_limited_liability_partnership"
    },
    {"display_name": "Sole Proprietorship", "field_name": "sole_proprietorship"},
    {"display_name": "Manufacturer of Goods", "field_name": "manufacturer_of_goods"},
    {"display_name": "Subchapter S Corporation", "field_name": "subchapter_s_corporation"},
    {"display_name": "Limited Liability Corporation", "field_name": "limited_liability_corporation"},

    # Minority Owned Business
    {"display_name": "Minority Owned Business", "field_name": "minority_owned_business"},
    {"display_name": "Alaskan Native Corporation Owned Firm", "field_name": "alaskan_native_owned_business"},
    {"display_name": "American Indian Owned Business", "field_name": "american_indian_owned_business"},
    {"display_name": "Asian Pacific American Owned Business", "field_name": "asian_pacific_american_owned_business"},
    {"display_name": "Black American Owned Business", "field_name": "black_american_owned_business"},
    {"display_name": "Hispanic American Owned Business", "field_name": "hispanic_american_owned_business"},
    {"display_name": "Native American Owned Business", "field_name": "native_american_owned_business"},
    {"display_name": "Native Hawaiian Owned Business", "field_name": "native_hawaiian_owned_business"},
    {
        "display_name": "Indian (Subcontinent) American Owned Business",
        "field_name": "subcontinent_asian_indian_american_owned_business"
    },
    {"display_name": "Tribally Owned Business", "field_name": "tribally_owned_business"},
    {"display_name": "Other Minority Owned Business", "field_name": "other_minority_owned_business"},

    # Women Owned Business
    {"display_name": "Woman Owned Business", "field_name": "woman_owned_business"},
    {"display_name": "Women Owned Small Business", "field_name": "women_owned_small_business"},
    {
        "display_name": "Economically Disadvantaged Women Owned Small Business",
        "field_name": "economically_disadvantaged_women_owned_small_business"
    },
    {
        "display_name": "Joint Venture Women Owned Small Business",
        "field_name": "joint_venture_women_owned_small_business"
    },
    {
        "display_name": "Joint Venture Economically Disadvantaged Women Owned Small Business",
        "field_name": "joint_venture_economically_disadvantaged_women_owned_small_business"
    },

    # Veteran Owned Business
    {"display_name": "Veteran Owned Business", "field_name": "veteran_owned_business"},
    {
        "display_name": "Service Disabled Veteran Owned Business",
        "field_name": "service_disabled_veteran_owned_business"
    },

    # Special Designations
    {"display_name": "Special Designations", "field_name": "special_designations"},
    {"display_name": "8(a) Program Participant", "field_name": "8a_program_participant"},
    {"display_name": "AbilityOne Program Participant", "field_name": "ability_one_program"},
    {
        "display_name": "DoT Certified Disadvantaged Business Enterprise",
        "field_name": "dot_certified_disadvantaged_business_enterprise"
    },
    {"display_name": "Emerging Small Business", "field_name": "emerging_small_business"},
    {
        "display_name": "Federally Funded Research and Development Corp",
        "field_name": "federally_funded_research_and_development_corp"
    },
    {"display_name": "HUBZone Firm", "field_name": "historically_underutilized_business_firm"},
    {"display_name": "Labor Surplus Area Firm", "field_name": "labor_surplus_area_firm"},
    {"display_name": "SBA Certified 8 a Joint Venture", "field_name": "sba_certified_8a_joint_venture"},
    {
        "display_name": "Self-Certified Small Disadvantaged Business",
        "field_name": "self_certified_small_disadvanted_business"
    },
    {"display_name": "Small Agricultural Cooperative", "field_name": "small_agricultural_cooperative"},
    {"display_name": "Small Disadvantaged Business", "field_name": "small_disadvantaged_business"},
    {
        "display_name": "Community Developed Corporation Owned Firm",
        "field_name": "community_developed_corporation_owned_firm"
    },
    {"display_name": "U.S.-Owned Business", "field_name": "us_owned_business"},
    {
        "display_name": "Foreign-Owned and U.S.-Incorporated Business",
        "field_name": "foreign_owned_and_us_located_business"
    },
    {"display_name": "Foreign Owned and Located", "field_name": "foreign_owned_and_located_business"},
    {"display_name": "Foreign Government", "field_name": "foreign_government"},
    {"display_name": "International Organization", "field_name": "international_organization"},
    {"display_name": "Domestic Shelter", "field_name": "domestic_shelter"},
    {"display_name": "Hospital", "field_name": "hospital"},
    {"display_name": "Veterinary Hospital", "field_name": "veterinary_hospital"},

    # Nonprofit
    {"display_name": "Nonprofit Organization", "field_name": "nonprofit"},
    {"display_name": "Foundation", "field_name": "foundation"},
    {"display_name": "Community Development Corporation", "field_name": "community_development_corporations"},

    # Higher education
    {"display_name": "Higher Education", "field_name": "higher_education"},
    {
        "display_name": "Higher Education (Public)",
        "field_name": "public_institution_of_higher_education"
    },
    {
        "display_name": "Higher Education (Private)",
        "field_name": "private_institution_of_higher_education"
    },
    {
        "display_name": "Higher Education (Minority Serving)",
        "field_name": "minority_serving_institution_of_higher_education"
    },
    {"display_name": "Educational Institution", "field_name": "educational_institution"},
    {"display_name": "School of Forestry", "field_name": "school_of_forestry"},
    {"display_name": "Veterinary College", "field_name": "veterinary_college"},

    # Government
    {"display_name": "Government", "field_name": "government"},
    {"display_name": "U.S. National Government", "field_name": "national_government"},
    {"display_name": "U.S. Regional/State Government", "field_name": "regional_and_state_government"},
    {"display_name": "U.S. Regional Government Organization", "field_name": "regional_organization"},
    {"display_name": "U.S. Interstate Government Entity", "field_name": "interstate_entity"},
    {"display_name": "U.S. Territory Government", "field_name": "us_territory_or_possession"},
    {"display_name": "U.S. Local Government", "field_name": "local_government"},
    {"display_name": "Native American Tribal Government", "field_name": "indian_native_american_tribal_government"},
    {"display_name": "U.S. Government Authorities", "field_name": "authorities_and_commissions"},
    {"display_name": "Interstate Entity", "field_name": "interstate_entity"},
    {"display_name": "Council of Governments", "field_name": "council_of_governments"},

    # Individuals
    {"display_name": "Individuals", "field_name": "individuals"},
]


def get_business_category_display_names(business_category_list):
    filtered_business_category_list = list(
        filter(
            lambda bus_cat_dict: bus_cat_dict.get("field_name") in business_category_list,
            BUSINESS_CATEGORIES_LOOKUP
        )
    )
    return list(map(lambda bus_cat_dict: bus_cat_dict.get("display_name"), filtered_business_category_list))
