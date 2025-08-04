import pytest

from django.db import connection
from usaspending_api.broker.helpers.get_business_categories import get_business_categories
from usaspending_api.common.helpers.business_categories_helper import get_business_category_display_names


@pytest.mark.django_db
def test_dev_2318_business_category_changes():
    """
    The following changes were made to business categories as part of DEV-2318.  Let's make sure they took.
        - alaskan_native_owned_business         => alaskan_native_corporation_owned_firm
        - foreign_owned_and_located_business    => foreign_owned
        - native_hawaiian_owned_business        => native_hawaiian_organization_owned_firm
        - tribally_owned_business               => tribally_owned_firm
    """

    expected_business_categories = [
        "alaskan_native_corporation_owned_firm",
        "foreign_owned",
        "minority_owned_business",
        "native_hawaiian_organization_owned_firm",
        "special_designations",
        "tribally_owned_firm",
    ]

    business_categories = get_business_categories(
        {
            "alaskan_native_owned_corpo": True,
            "native_hawaiian_owned_busi": True,
            "tribally_owned_business": True,
            "foreign_owned_and_located": True,
        },
        "fpds",
    )
    assert business_categories == expected_business_categories

    # Same test using the database function.
    with connection.cursor() as cursor:
        cursor.execute(
            """
                select
                    compile_fpds_business_categories(
                        '',     -- contracting_officers_deter
                        false,  -- corporate_entity_tax_exemp
                        false,  -- corporate_entity_not_tax_e
                        false,  -- partnership_or_limited_lia
                        false,  -- sole_proprietorship
                        false,  -- manufacturer_of_goods
                        false,  -- subchapter_s_corporation
                        false,  -- limited_liability_corporat
                        false,  -- for_profit_organization
                        TRUE,   -- alaskan_native_owned_corpo
                        false,  -- american_indian_owned_busi
                        false,  -- asian_pacific_american_own
                        false,  -- black_american_owned_busin
                        false,  -- hispanic_american_owned_bu
                        false,  -- native_american_owned_busi
                        TRUE,   -- native_hawaiian_owned_busi
                        false,  -- subcontinent_asian_asian_i
                        TRUE,   -- tribally_owned_business
                        false,  -- other_minority_owned_busin
                        false,  -- minority_owned_business
                        false,  -- women_owned_small_business
                        false,  -- economically_disadvantaged
                        false,  -- joint_venture_women_owned
                        false,  -- joint_venture_economically
                        false,  -- woman_owned_business
                        false,  -- service_disabled_veteran_o
                        false,  -- veteran_owned_business
                        false,  -- c8a_program_participant
                        false,  -- the_ability_one_program
                        false,  -- dot_certified_disadvantage
                        false,  -- emerging_small_business
                        false,  -- federally_funded_research
                        false,  -- historically_underutilized
                        false,  -- labor_surplus_area_firm
                        false,  -- sba_certified_8_a_joint_ve
                        false,  -- self_certified_small_disad
                        false,  -- small_agricultural_coopera
                        false,  -- small_disadvantaged_busine
                        false,  -- community_developed_corpor
                        '',     -- domestic_or_foreign_entity
                        TRUE,   -- foreign_owned_and_located
                        false,  -- foreign_government
                        false,  -- international_organization
                        false,  -- domestic_shelter
                        false,  -- hospital_flag
                        false,  -- veterinary_hospital
                        false,  -- foundation
                        false,  -- community_development_corp
                        false,  -- nonprofit_organization
                        false,  -- educational_institution
                        false,  -- other_not_for_profit_organ
                        false,  -- state_controlled_instituti
                        false,  -- c1862_land_grant_college
                        false,  -- c1890_land_grant_college
                        false,  -- c1994_land_grant_college
                        false,  -- private_university_or_coll
                        false,  -- minority_institution
                        false,  -- historically_black_college
                        false,  -- tribal_college
                        false,  -- alaskan_native_servicing_i
                        false,  -- native_hawaiian_servicing
                        false,  -- hispanic_servicing_institu
                        false,  -- school_of_forestry
                        false,  -- veterinary_college
                        false,  -- us_federal_government
                        false,  -- federal_agency
                        false,  -- us_government_entity
                        false,  -- interstate_entity
                        false,  -- us_state_government
                        false,  -- council_of_governments
                        false,  -- city_local_government
                        false,  -- county_local_government
                        false,  -- inter_municipal_local_gove
                        false,  -- municipality_local_governm
                        false,  -- township_local_government
                        false,  -- us_local_government
                        false,  -- local_government_owned
                        false,  -- school_district_local_gove
                        false,  -- us_tribal_government
                        false,  -- indian_tribe_federally_rec
                        false,  -- housing_authorities_public
                        false,  -- airport_authority
                        false,  -- port_authority
                        false,  -- transit_authority
                        false   -- planning_commission
                    )
            """
        )
        assert cursor.fetchall()[0][0] == expected_business_categories

        display_names = get_business_category_display_names(business_categories)
        assert display_names == [
            "Alaskan Native Corporation Owned Firm",
            "Foreign Owned",
            "Minority Owned Business",
            "Native Hawaiian Organization Owned Firm",
            "Special Designations",
            "Tribally Owned Firm",
        ]
