# Third-party app imports
import pytest

from usaspending_api.common.helpers.business_categories_helper import get_business_category_display_names


def test_get_few_valid_business_category_display_names():
    business_category_names_list = [
        "corporate_entity_tax_exempt",
        "minority_owned_business",
        "economically_disadvantaged_women_owned_small_business",
        "service_disabled_veteran_owned_business",
        "dot_certified_disadvantaged_business_enterprise",
        "federally_funded_research_and_development_corp",
        "self_certified_small_disadvanted_business",
        "community_development_corporations",
        "educational_institution",
        "council_of_governments",
        "interstate_entity",
        "individuals"
    ]
    business_category_display_names_list = [
        "Corporate Entity Tax Exempt",
        "Minority Owned Business",
        "Economically Disadvantaged Women Owned Small Business",
        "Service Disabled Veteran Owned Business",
        "DOT Certified Disadvantaged Business Enterprise",
        "Federally Funded Research and Development Corp",
        "Self-Certified Small Disadvantaged Business",
        "Community Development Corporations",
        "Educational Institution",
        "Council of Governments",
        "Interstate Entity",
        "Individuals"
    ]
    assert get_business_category_display_names(business_category_names_list) == business_category_display_names_list
