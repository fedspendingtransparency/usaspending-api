from usaspending_api.llm.models.py_models import AITool, AIToolDescription

RECIPIENT_TYPES: dict[str, list[dict[str, str]]] = {
    "General business": [
        {"value": "business", "label": "Any business entity"},
        {"value": "small_business", "label": "Small business (SBA size standards)"},
        {"value": "other_than_small_business", "label": "Large businesses"},
        {"value": "corporate_entity_tax_exempt", "label": "Tax-exempt corporations"},
        {"value": "corporate_entity_not_tax_exempt", "label": "Taxable corporations"},
        {"value": "partnership_or_limited_liability_partnership", "label": "Partnerships/LLPs"},
        {"value": "sole_proprietorship", "label": "Individual-owned businesses"},
        {"value": "manufacturer_of_goods", "label": "Manufacturing companies"},
        {"value": "subchapter_s_corporation", "label": "S-Corps (pass-through taxation)"},
        {"value": "limited_liability_corporation", "label": "LLCs"},
    ],
    "Minority owned business": [
        {"value": "minority_owned_business", "label": "Any minority-owned business"},
        {"value": "alaskan_native_corporation_owned_firm", "label": "Alaska Native corporations"},
        {"value": "american_indian_owned_business", "label": "American Indian owned"},
        {"value": "asian_pacific_american_owned_business", "label": "Asian Pacific American owned"},
        {"value": "black_american_owned_business", "label": "Black/African American owned"},
        {"value": "hispanic_american_owned_business", "label": "Hispanic/Latino owned"},
        {"value": "native_american_owned_business", "label": "Native American owned"},
        {"value": "native_hawaiian_organization_owned_firm", "label": "Native Hawaiian organizations"},
        {"value": "subcontinent_asian_indian_american_owned_business", "label": "South Asian owned"},
        {"value": "tribally_owned_firm", "label": "Tribal government-owned"},
        {"value": "other_minority_owned_business", "label": "Other minority categories"},
    ],
    "Women owned business": [
        {"value": "woman_owned_business", "label": "Any women-owned business"},
        {"value": "women_owned_small_business", "label": "Women-owned small business (WOSB)"},
        {
            "value": "economically_disadvantaged_women_owned_small_business",
            "label": "Economically disadvantaged WOSB (EDWOSB)",
        },
        {"value": "joint_venture_women_owned_small_business", "label": "WOSB joint ventures"},
        {
            "value": "joint_venture_economically_disadvantaged_women_owned_small_business",
            "label": "EDWOSB joint ventures",
        },
    ],
    "Veteran owned business": [
        {"value": "veteran_owned_business", "label": "Veteran-owned business (VOB)"},
        {"value": "service_disabled_veteran_owned_business", "label": "Service-disabled veteran-owned (SDVOB)"},
    ],
    "Special designations": [
        {"value": "special_designations", "label": "Any special designation"},
        {"value": "8a_program_participant", "label": "SBA 8(a) Business Development program"},
        {"value": "ability_one_program", "label": "AbilityOne (employs people with disabilities)"},
        {"value": "dot_certified_disadvantaged_business_enterprise", "label": "DoT DBE certified"},
        {"value": "emerging_small_business", "label": "Emerging small business"},
        {"value": "federally_funded_research_and_development_corp", "label": "FFRDCs"},
        {"value": "historically_underutilized_business_firm", "label": "HUBZone certified"},
        {"value": "labor_surplus_area_firm", "label": "Located in labor surplus areas"},
        {"value": "sba_certified_8a_joint_venture", "label": "SBA-certified 8(a) joint ventures"},
        {"value": "self_certified_small_disadvanted_business", "label": "Self-certified small disadvantaged business"},
        {"value": "small_agricultural_cooperative", "label": "Agricultural cooperatives"},
        {"value": "community_developed_corporation_owned_firm", "label": "Community development corporations"},
        {"value": "us_owned_business", "label": "U.S.-owned businesses"},
        {"value": "foreign_owned_and_us_located_business", "label": "Foreign-owned, U.S.-based"},
        {"value": "foreign_owned", "label": "Foreign-owned entities"},
        {"value": "foreign_government", "label": "Foreign government entities"},
        {"value": "international_organization", "label": "International organizations (UN, World Bank, etc.)"},
        {"value": "domestic_shelter", "label": "Domestic violence shelters"},
        {"value": "hospital", "label": "Hospital facilities"},
        {"value": "veterinary_hospital", "label": "Veterinary hospitals"},
    ],
    "Nonprofit": [
        {"value": "nonprofit", "label": "Any nonprofit organization (501(c) entities)"},
        {"value": "foundation", "label": "Private/public foundations"},
        {"value": "community_development_corporations", "label": "Community development nonprofits"},
    ],
    "Higher education": [
        {"value": "higher_education", "label": "Any higher education institution"},
        {"value": "public_institution_of_higher_education", "label": "Public colleges/universities"},
        {"value": "private_institution_of_higher_education", "label": "Private colleges/universities"},
        {
            "value": "minority_serving_institution_of_higher_education",
            "label": "MSIs (HBCUs, HSIs, TCUs, etc.)",
        },
        {"value": "school_of_forestry", "label": "Forestry schools"},
        {"value": "veterinary_college", "label": "Veterinary medicine schools"},
    ],
    "Government": [
        {"value": "government", "label": "Any government entity"},
        {"value": "national_government", "label": "Federal government agencies"},
        {"value": "interstate_entity", "label": "Multi-state compacts/authorities"},
        {"value": "regional_and_state_government", "label": "State governments"},
        {"value": "regional_organization", "label": "Regional planning organizations"},
        {"value": "us_territory_or_possession", "label": "Puerto Rico, Guam, USVI, etc."},
        {"value": "council_of_governments", "label": "Regional councils (COGs)"},
        {"value": "local_government", "label": "Cities, counties, municipalities"},
        {"value": "indian_native_american_tribal_government", "label": "Federally recognized tribes"},
        {"value": "authorities_and_commissions", "label": "Public authorities/commissions"},
    ],
    "Individuals": [
        {"value": "individuals", "label": "Individual persons (grants, scholarships, etc.)"},
    ],
}


def list_recipient_types() -> dict[str, list[dict[str, str]]]:
    return RECIPIENT_TYPES


list_recipient_types_tool = AITool(
    function=list_recipient_types,
    logging=lambda _: "Listing recipient types.",
    description=AIToolDescription(
        name="list_recipient_types",
        description=(
            "List every valid recipient-type filter value, grouped by category (General business, "
            "Minority owned business, Women owned business, Veteran owned business, Special "
            "designations, Nonprofit, Higher education, Government, Individuals). Takes no input. "
            "Each entry has a 'value' (the exact string to put in the execute_filter 'recipientType' "
            "field) and a short 'label' describing it. Call this before setting 'recipientType' to "
            "choose the correct value(s) for what the user described."
        ),
        input_schema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
)
