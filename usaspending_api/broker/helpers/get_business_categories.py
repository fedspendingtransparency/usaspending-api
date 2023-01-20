from usaspending_api.broker.helpers.build_business_categories_boolean_dict import (
    build_business_categories_boolean_dict,
    fpds_boolean_columns,
)


def get_business_categories(row, data_type):
    business_category_set = set()

    if data_type == "fabs":
        # BUSINESS (FOR-PROFIT)
        business_types = row.get("business_types")
        if business_types in ("R", "23"):
            business_category_set.add("small_business")

        if business_types in ("Q", "22"):
            business_category_set.add("other_than_small_business")

        if business_category_set & {"small_business", "other_than_small_business"}:
            business_category_set.add("category_business")

        # NON-PROFIT
        if business_types in ("M", "N", "12"):
            business_category_set.add("nonprofit")

        # HIGHER EDUCATION
        if business_types in ("H", "06"):
            business_category_set.add("public_institution_of_higher_education")

        if business_types in ("O", "20"):
            business_category_set.add("private_institution_of_higher_education")

        if business_types in ("T", "U", "V", "S"):
            business_category_set.add("minority_serving_institution_of_higher_education")

        if business_category_set & {
            "public_institution_of_higher_education",
            "private_institution_of_higher_education",
            "minority_serving_institution_of_higher_education",
        }:
            business_category_set.add("higher_education")

        # GOVERNMENT
        if business_types in ("A", "00"):
            business_category_set.add("regional_and_state_government")

        if business_types == "E":
            business_category_set.add("regional_organization")

        if business_types == "F":
            business_category_set.add("us_territory_or_possession")

        if business_types in ("B", "C", "D", "G", "01", "02", "04", "05"):
            business_category_set.add("local_government")

        if business_types in ("I", "J", "K", "11"):
            business_category_set.add("indian_native_american_tribal_government")

        if business_types == "L":
            business_category_set.add("authorities_and_commissions")

        if business_category_set & {
            "regional_and_state_government",
            "us_territory_or_possession",
            "local_government",
            "indian_native_american_tribal_government",
            "authorities_and_commissions",
            "regional_organization",
        }:
            business_category_set.add("government")

        # INDIVIDUALS
        if business_types in ("P", "21"):
            business_category_set.add("individuals")

        return sorted(business_category_set)

    elif data_type == "fpds":
        business_categories_boolean_dict = build_business_categories_boolean_dict(row)

        # BUSINESS (FOR-PROFIT)
        if (
            row.get("contracting_officers_deter") == "S"
            or business_categories_boolean_dict["women_owned_small_business"] is True
            or business_categories_boolean_dict["economically_disadvantaged"] is True
            or business_categories_boolean_dict["joint_venture_women_owned"] is True
            or business_categories_boolean_dict["emerging_small_business"] is True
            or business_categories_boolean_dict["self_certified_small_disad"] is True
            or business_categories_boolean_dict["small_agricultural_coopera"] is True
            or business_categories_boolean_dict["small_disadvantaged_busine"] is True
        ):
            business_category_set.add("small_business")

        if row.get("contracting_officers_deter") == "O":
            business_category_set.add("other_than_small_business")

        if business_categories_boolean_dict["corporate_entity_tax_exemp"] is True:
            business_category_set.add("corporate_entity_tax_exempt")

        if business_categories_boolean_dict["corporate_entity_not_tax_e"] is True:
            business_category_set.add("corporate_entity_not_tax_exempt")

        if business_categories_boolean_dict["partnership_or_limited_lia"] is True:
            business_category_set.add("partnership_or_limited_liability_partnership")

        if business_categories_boolean_dict["sole_proprietorship"] is True:
            business_category_set.add("sole_proprietorship")

        if business_categories_boolean_dict["manufacturer_of_goods"] is True:
            business_category_set.add("manufacturer_of_goods")

        if business_categories_boolean_dict["subchapter_s_corporation"] is True:
            business_category_set.add("subchapter_s_corporation")

        if business_categories_boolean_dict["limited_liability_corporat"] is True:
            business_category_set.add("limited_liability_corporation")

        if business_categories_boolean_dict["for_profit_organization"] is True or (
            business_category_set
            & {
                "small_business",
                "other_than_small_business",
                "corporate_entity_tax_exempt",
                "corporate_entity_not_tax_exempt",
                "partnership_or_limited_liability_partnership",
                "sole_proprietorship",
                "manufacturer_of_goods",
                "subchapter_s_corporation",
                "limited_liability_corporation",
            }
        ):
            business_category_set.add("category_business")

        # MINORITY BUSINESS
        if business_categories_boolean_dict["alaskan_native_owned_corpo"] is True:
            business_category_set.add("alaskan_native_corporation_owned_firm")

        if business_categories_boolean_dict["american_indian_owned_busi"] is True:
            business_category_set.add("american_indian_owned_business")

        if business_categories_boolean_dict["asian_pacific_american_own"] is True:
            business_category_set.add("asian_pacific_american_owned_business")

        if business_categories_boolean_dict["black_american_owned_busin"] is True:
            business_category_set.add("black_american_owned_business")

        if business_categories_boolean_dict["hispanic_american_owned_bu"] is True:
            business_category_set.add("hispanic_american_owned_business")

        if business_categories_boolean_dict["native_american_owned_busi"] is True:
            business_category_set.add("native_american_owned_business")

        if business_categories_boolean_dict["native_hawaiian_owned_busi"] is True:
            business_category_set.add("native_hawaiian_organization_owned_firm")

        if business_categories_boolean_dict["subcontinent_asian_asian_i"] is True:
            business_category_set.add("subcontinent_asian_indian_american_owned_business")

        if business_categories_boolean_dict["tribally_owned_business"] is True:
            business_category_set.add("tribally_owned_firm")

        if business_categories_boolean_dict["other_minority_owned_busin"] is True:
            business_category_set.add("other_minority_owned_business")

        if business_categories_boolean_dict["minority_owned_business"] is True or (
            business_category_set
            & {
                "alaskan_native_corporation_owned_firm",
                "american_indian_owned_business",
                "asian_pacific_american_owned_business",
                "black_american_owned_business",
                "hispanic_american_owned_business",
                "native_american_owned_business",
                "native_hawaiian_organization_owned_firm",
                "subcontinent_asian_indian_american_owned_business",
                "tribally_owned_firm",
                "other_minority_owned_business",
            }
        ):
            business_category_set.add("minority_owned_business")

        # WOMEN OWNED BUSINESS
        if business_categories_boolean_dict["women_owned_small_business"] is True:
            business_category_set.add("women_owned_small_business")

        if business_categories_boolean_dict["economically_disadvantaged"] is True:
            business_category_set.add("economically_disadvantaged_women_owned_small_business")

        if business_categories_boolean_dict["joint_venture_women_owned"] is True:
            business_category_set.add("joint_venture_women_owned_small_business")

        if business_categories_boolean_dict["joint_venture_economically"] is True:
            business_category_set.add("joint_venture_economically_disadvantaged_women_owned_small_business")

        if business_categories_boolean_dict["woman_owned_business"] is True or (
            business_category_set
            & {
                "women_owned_small_business",
                "economically_disadvantaged_women_owned_small_business",
                "joint_venture_women_owned_small_business",
                "joint_venture_economically_disadvantaged_women_owned_small_business",
            }
        ):
            business_category_set.add("woman_owned_business")

        # VETERAN OWNED BUSINESS
        if business_categories_boolean_dict["service_disabled_veteran_o"] is True:
            business_category_set.add("service_disabled_veteran_owned_business")

        if business_categories_boolean_dict["veteran_owned_business"] is True or (
            business_category_set & {"service_disabled_veteran_owned_business"}
        ):
            business_category_set.add("veteran_owned_business")

        # SPECIAL DESIGNATIONS
        if business_categories_boolean_dict["c8a_program_participant"] is True:
            business_category_set.add("8a_program_participant")

        if business_categories_boolean_dict["the_ability_one_program"] is True:
            business_category_set.add("ability_one_program")

        if business_categories_boolean_dict["dot_certified_disadvantage"] is True:
            business_category_set.add("dot_certified_disadvantaged_business_enterprise")

        if business_categories_boolean_dict["emerging_small_business"] is True:
            business_category_set.add("emerging_small_business")

        if business_categories_boolean_dict["federally_funded_research"] is True:
            business_category_set.add("federally_funded_research_and_development_corp")

        if business_categories_boolean_dict["historically_underutilized"] is True:
            business_category_set.add("historically_underutilized_business_firm")

        if business_categories_boolean_dict["labor_surplus_area_firm"] is True:
            business_category_set.add("labor_surplus_area_firm")

        if business_categories_boolean_dict["sba_certified_8_a_joint_ve"] is True:
            business_category_set.add("sba_certified_8a_joint_venture")

        if business_categories_boolean_dict["self_certified_small_disad"] is True:
            business_category_set.add("self_certified_small_disadvanted_business")

        if business_categories_boolean_dict["small_agricultural_coopera"] is True:
            business_category_set.add("small_agricultural_cooperative")

        if business_categories_boolean_dict["small_disadvantaged_busine"] is True:
            business_category_set.add("small_disadvantaged_business")

        if business_categories_boolean_dict["community_developed_corpor"] is True:
            business_category_set.add("community_developed_corporation_owned_firm")

        # U.S. Owned Business
        if row.get("domestic_or_foreign_entity") == "A":
            business_category_set.add("us_owned_business")

        # Foreign-Owned Business Incorporated in the U.S.
        if row.get("domestic_or_foreign_entity") == "C":
            business_category_set.add("foreign_owned_and_us_located_business")

        # Foreign-Owned Business Not Incorporated in the U.S.
        if (
            row.get("domestic_or_foreign_entity") == "D"
            or business_categories_boolean_dict["foreign_owned_and_located"] is True
        ):
            business_category_set.add("foreign_owned")

        if business_categories_boolean_dict["foreign_government"] is True:
            business_category_set.add("foreign_government")

        if business_categories_boolean_dict["international_organization"] is True:
            business_category_set.add("international_organization")

        if business_categories_boolean_dict["domestic_shelter"] is True:
            business_category_set.add("domestic_shelter")

        if business_categories_boolean_dict["hospital_flag"] is True:
            business_category_set.add("hospital")

        if business_categories_boolean_dict["veterinary_hospital"] is True:
            business_category_set.add("veterinary_hospital")

        if business_category_set & {
            "8a_program_participant",
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
            "foreign_owned",
            "foreign_government",
            "international_organization",
            "domestic_shelter",
            "hospital",
            "veterinary_hospital",
        }:
            business_category_set.add("special_designations")

        # NON-PROFIT
        if business_categories_boolean_dict["foundation"] is True:
            business_category_set.add("foundation")

        if business_categories_boolean_dict["community_development_corp"] is True:
            business_category_set.add("community_development_corporations")

        if (
            business_categories_boolean_dict["nonprofit_organization"] is True
            or business_categories_boolean_dict["other_not_for_profit_organ"] is True
            or (business_category_set & {"foundation", "community_development_corporations"})
        ):
            business_category_set.add("nonprofit")

        # HIGHER EDUCATION
        if business_categories_boolean_dict["educational_institution"] is True:
            business_category_set.add("educational_institution")

        if (
            business_categories_boolean_dict["state_controlled_instituti"] is True
            or business_categories_boolean_dict["c1862_land_grant_college"] is True
            or business_categories_boolean_dict["c1890_land_grant_college"] is True
            or business_categories_boolean_dict["c1994_land_grant_college"] is True
        ):
            business_category_set.add("public_institution_of_higher_education")

        if business_categories_boolean_dict["private_university_or_coll"] is True:
            business_category_set.add("private_institution_of_higher_education")

        if (
            business_categories_boolean_dict["minority_institution"] is True
            or business_categories_boolean_dict["historically_black_college"] is True
            or business_categories_boolean_dict["tribal_college"] is True
            or business_categories_boolean_dict["alaskan_native_servicing_i"] is True
            or business_categories_boolean_dict["native_hawaiian_servicing"] is True
            or business_categories_boolean_dict["hispanic_servicing_institu"] is True
        ):
            business_category_set.add("minority_serving_institution_of_higher_education")

        if business_categories_boolean_dict["school_of_forestry"] is True:
            business_category_set.add("school_of_forestry")

        if business_categories_boolean_dict["veterinary_college"] is True:
            business_category_set.add("veterinary_college")

        if business_category_set & {
            "educational_institution",
            "public_institution_of_higher_education",
            "private_institution_of_higher_education",
            "school_of_forestry",
            "minority_serving_institution_of_higher_education",
            "veterinary_college",
        }:
            business_category_set.add("higher_education")

        # GOVERNMENT
        if (
            business_categories_boolean_dict["us_federal_government"] is True
            or business_categories_boolean_dict["federal_agency"] is True
            or business_categories_boolean_dict["us_government_entity"] is True
        ):
            business_category_set.add("national_government")

        if business_categories_boolean_dict["interstate_entity"] is True:
            business_category_set.add("interstate_entity")

        if business_categories_boolean_dict["us_state_government"] is True:
            business_category_set.add("regional_and_state_government")

        if business_categories_boolean_dict["council_of_governments"] is True:
            business_category_set.add("council_of_governments")

        if (
            business_categories_boolean_dict["city_local_government"] is True
            or business_categories_boolean_dict["county_local_government"] is True
            or business_categories_boolean_dict["inter_municipal_local_gove"] is True
            or business_categories_boolean_dict["municipality_local_governm"] is True
            or business_categories_boolean_dict["township_local_government"] is True
            or business_categories_boolean_dict["us_local_government"] is True
            or business_categories_boolean_dict["local_government_owned"] is True
            or business_categories_boolean_dict["school_district_local_gove"] is True
        ):
            business_category_set.add("local_government")

        if (
            business_categories_boolean_dict["us_tribal_government"] is True
            or business_categories_boolean_dict["indian_tribe_federally_rec"] is True
        ):
            business_category_set.add("indian_native_american_tribal_government")

        if (
            business_categories_boolean_dict["housing_authorities_public"] is True
            or business_categories_boolean_dict["airport_authority"] is True
            or business_categories_boolean_dict["port_authority"] is True
            or business_categories_boolean_dict["transit_authority"] is True
            or business_categories_boolean_dict["planning_commission"] is True
        ):
            business_category_set.add("authorities_and_commissions")

        if business_category_set & {
            "national_government",
            "regional_and_state_government",
            "local_government",
            "indian_native_american_tribal_government",
            "authorities_and_commissions",
            "interstate_entity",
            "council_of_governments",
        }:
            business_category_set.add("government")

        return sorted(business_category_set)
    else:
        raise ValueError(
            "Invalid object type provided to update_business_categories. "
            "Must be one of the following types: TransactionFPDS, TransactionFABS"
        )


def get_business_categories_fabs(business_types):
    # This function is supposed to be invoked as a Spark UDF with the business_types field passed to it.

    business_category_set = set()

    # BUSINESS (FOR-PROFIT)
    if business_types in ("R", "23"):
        business_category_set.add("small_business")

    if business_types in ("Q", "22"):
        business_category_set.add("other_than_small_business")

    if business_category_set & {"small_business", "other_than_small_business"}:
        business_category_set.add("category_business")

    # NON-PROFIT
    if business_types in ("M", "N", "12"):
        business_category_set.add("nonprofit")

    # HIGHER EDUCATION
    if business_types in ("H", "06"):
        business_category_set.add("public_institution_of_higher_education")

    if business_types in ("O", "20"):
        business_category_set.add("private_institution_of_higher_education")

    if business_types in ("T", "U", "V", "S"):
        business_category_set.add("minority_serving_institution_of_higher_education")

    if business_category_set & {
        "public_institution_of_higher_education",
        "private_institution_of_higher_education",
        "minority_serving_institution_of_higher_education",
    }:
        business_category_set.add("higher_education")

    # GOVERNMENT
    if business_types in ("A", "00"):
        business_category_set.add("regional_and_state_government")

    if business_types == "E":
        business_category_set.add("regional_organization")

    if business_types == "F":
        business_category_set.add("us_territory_or_possession")

    if business_types in ("B", "C", "D", "G", "01", "02", "04", "05"):
        business_category_set.add("local_government")

    if business_types in ("I", "J", "K", "11"):
        business_category_set.add("indian_native_american_tribal_government")

    if business_types == "L":
        business_category_set.add("authorities_and_commissions")

    if business_category_set & {
        "regional_and_state_government",
        "us_territory_or_possession",
        "local_government",
        "indian_native_american_tribal_government",
        "authorities_and_commissions",
        "regional_organization",
    }:
        business_category_set.add("government")

    # INDIVIDUALS
    if business_types in ("P", "21"):
        business_category_set.add("individuals")

    return sorted(business_category_set)


def get_business_categories_fpds(row):
    # This function is supposed to be invoked as a Spark UDF with a named_struct containing the necessary
    # fields passed to it.

    def row_get(row, col_name):
        "pyspark.sql.types.Row is basically a named tuple, not a dict, so make a dict-like get function"
        if col_name in fpds_boolean_columns:
            except_val = False
        else:
            except_val = None

        try:
            retval = getattr(row, col_name)
        except AttributeError:
            retval = except_val

        return retval

    business_category_set = set()

    # BUSINESS (FOR-PROFIT)
    if (
        row_get(row, "contracting_officers_deter") == "S"
        or row_get(row, "women_owned_small_business") is True
        or row_get(row, "economically_disadvantaged") is True
        or row_get(row, "joint_venture_women_owned") is True
        or row_get(row, "emerging_small_business") is True
        or row_get(row, "self_certified_small_disad") is True
        or row_get(row, "small_agricultural_coopera") is True
        or row_get(row, "small_disadvantaged_busine") is True
    ):
        business_category_set.add("small_business")

    if row_get(row, "contracting_officers_deter") == "O":
        business_category_set.add("other_than_small_business")

    if row_get(row, "corporate_entity_tax_exemp") is True:
        business_category_set.add("corporate_entity_tax_exempt")

    if row_get(row, "corporate_entity_not_tax_e") is True:
        business_category_set.add("corporate_entity_not_tax_exempt")

    if row_get(row, "partnership_or_limited_lia") is True:
        business_category_set.add("partnership_or_limited_liability_partnership")

    if row_get(row, "sole_proprietorship") is True:
        business_category_set.add("sole_proprietorship")

    if row_get(row, "manufacturer_of_goods") is True:
        business_category_set.add("manufacturer_of_goods")

    if row_get(row, "subchapter_s_corporation") is True:
        business_category_set.add("subchapter_s_corporation")

    if row_get(row, "limited_liability_corporat") is True:
        business_category_set.add("limited_liability_corporation")

    if row_get(row, "for_profit_organization") is True or (
        business_category_set
        & {
            "small_business",
            "other_than_small_business",
            "corporate_entity_tax_exempt",
            "corporate_entity_not_tax_exempt",
            "partnership_or_limited_liability_partnership",
            "sole_proprietorship",
            "manufacturer_of_goods",
            "subchapter_s_corporation",
            "limited_liability_corporation",
        }
    ):
        business_category_set.add("category_business")

    # MINORITY BUSINESS
    if row_get(row, "alaskan_native_owned_corpo") is True:
        business_category_set.add("alaskan_native_corporation_owned_firm")

    if row_get(row, "american_indian_owned_busi") is True:
        business_category_set.add("american_indian_owned_business")

    if row_get(row, "asian_pacific_american_own") is True:
        business_category_set.add("asian_pacific_american_owned_business")

    if row_get(row, "black_american_owned_busin") is True:
        business_category_set.add("black_american_owned_business")

    if row_get(row, "hispanic_american_owned_bu") is True:
        business_category_set.add("hispanic_american_owned_business")

    if row_get(row, "native_american_owned_busi") is True:
        business_category_set.add("native_american_owned_business")

    if row_get(row, "native_hawaiian_owned_busi") is True:
        business_category_set.add("native_hawaiian_organization_owned_firm")

    if row_get(row, "subcontinent_asian_asian_i") is True:
        business_category_set.add("subcontinent_asian_indian_american_owned_business")

    if row_get(row, "tribally_owned_business") is True:
        business_category_set.add("tribally_owned_firm")

    if row_get(row, "other_minority_owned_busin") is True:
        business_category_set.add("other_minority_owned_business")

    if row_get(row, "minority_owned_business") is True or (
        business_category_set
        & {
            "alaskan_native_corporation_owned_firm",
            "american_indian_owned_business",
            "asian_pacific_american_owned_business",
            "black_american_owned_business",
            "hispanic_american_owned_business",
            "native_american_owned_business",
            "native_hawaiian_organization_owned_firm",
            "subcontinent_asian_indian_american_owned_business",
            "tribally_owned_firm",
            "other_minority_owned_business",
        }
    ):
        business_category_set.add("minority_owned_business")

    # WOMEN OWNED BUSINESS
    if row_get(row, "women_owned_small_business") is True:
        business_category_set.add("women_owned_small_business")

    if row_get(row, "economically_disadvantaged") is True:
        business_category_set.add("economically_disadvantaged_women_owned_small_business")

    if row_get(row, "joint_venture_women_owned") is True:
        business_category_set.add("joint_venture_women_owned_small_business")

    if row_get(row, "joint_venture_economically") is True:
        business_category_set.add("joint_venture_economically_disadvantaged_women_owned_small_business")

    if row_get(row, "woman_owned_business") is True or (
        business_category_set
        & {
            "women_owned_small_business",
            "economically_disadvantaged_women_owned_small_business",
            "joint_venture_women_owned_small_business",
            "joint_venture_economically_disadvantaged_women_owned_small_business",
        }
    ):
        business_category_set.add("woman_owned_business")

    # VETERAN OWNED BUSINESS
    if row_get(row, "service_disabled_veteran_o") is True:
        business_category_set.add("service_disabled_veteran_owned_business")

    if row_get(row, "veteran_owned_business") is True or (
        business_category_set & {"service_disabled_veteran_owned_business"}
    ):
        business_category_set.add("veteran_owned_business")

    # SPECIAL DESIGNATIONS
    if row_get(row, "c8a_program_participant") is True:
        business_category_set.add("8a_program_participant")

    if row_get(row, "the_ability_one_program") is True:
        business_category_set.add("ability_one_program")

    if row_get(row, "dot_certified_disadvantage") is True:
        business_category_set.add("dot_certified_disadvantaged_business_enterprise")

    if row_get(row, "emerging_small_business") is True:
        business_category_set.add("emerging_small_business")

    if row_get(row, "federally_funded_research") is True:
        business_category_set.add("federally_funded_research_and_development_corp")

    if row_get(row, "historically_underutilized") is True:
        business_category_set.add("historically_underutilized_business_firm")

    if row_get(row, "labor_surplus_area_firm") is True:
        business_category_set.add("labor_surplus_area_firm")

    if row_get(row, "sba_certified_8_a_joint_ve") is True:
        business_category_set.add("sba_certified_8a_joint_venture")

    if row_get(row, "self_certified_small_disad") is True:
        business_category_set.add("self_certified_small_disadvanted_business")

    if row_get(row, "small_agricultural_coopera") is True:
        business_category_set.add("small_agricultural_cooperative")

    if row_get(row, "small_disadvantaged_busine") is True:
        business_category_set.add("small_disadvantaged_business")

    if row_get(row, "community_developed_corpor") is True:
        business_category_set.add("community_developed_corporation_owned_firm")

    # U.S. Owned Business
    if row_get(row, "domestic_or_foreign_entity") == "A":
        business_category_set.add("us_owned_business")

    # Foreign-Owned Business Incorporated in the U.S.
    if row_get(row, "domestic_or_foreign_entity") == "C":
        business_category_set.add("foreign_owned_and_us_located_business")

    # Foreign-Owned Business Not Incorporated in the U.S.
    if row_get(row, "domestic_or_foreign_entity") == "D" or row_get(row, "foreign_owned_and_located") is True:
        business_category_set.add("foreign_owned")

    if row_get(row, "foreign_government") is True:
        business_category_set.add("foreign_government")

    if row_get(row, "international_organization") is True:
        business_category_set.add("international_organization")

    if row_get(row, "domestic_shelter") is True:
        business_category_set.add("domestic_shelter")

    if row_get(row, "hospital_flag") is True:
        business_category_set.add("hospital")

    if row_get(row, "veterinary_hospital") is True:
        business_category_set.add("veterinary_hospital")

    if business_category_set & {
        "8a_program_participant",
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
        "foreign_owned",
        "foreign_government",
        "international_organization",
        "domestic_shelter",
        "hospital",
        "veterinary_hospital",
    }:
        business_category_set.add("special_designations")

    # NON-PROFIT
    if row_get(row, "foundation") is True:
        business_category_set.add("foundation")

    if row_get(row, "community_development_corp") is True:
        business_category_set.add("community_development_corporations")

    if (
        row_get(row, "nonprofit_organization") is True
        or row_get(row, "other_not_for_profit_organ") is True
        or (business_category_set & {"foundation", "community_development_corporations"})
    ):
        business_category_set.add("nonprofit")

    # HIGHER EDUCATION
    if row_get(row, "educational_institution") is True:
        business_category_set.add("educational_institution")

    if (
        row_get(row, "state_controlled_instituti") is True
        or row_get(row, "c1862_land_grant_college") is True
        or row_get(row, "c1890_land_grant_college") is True
        or row_get(row, "c1994_land_grant_college") is True
    ):
        business_category_set.add("public_institution_of_higher_education")

    if row_get(row, "private_university_or_coll") is True:
        business_category_set.add("private_institution_of_higher_education")

    if (
        row_get(row, "minority_institution") is True
        or row_get(row, "historically_black_college") is True
        or row_get(row, "tribal_college") is True
        or row_get(row, "alaskan_native_servicing_i") is True
        or row_get(row, "native_hawaiian_servicing") is True
        or row_get(row, "hispanic_servicing_institu") is True
    ):
        business_category_set.add("minority_serving_institution_of_higher_education")

    if row_get(row, "school_of_forestry") is True:
        business_category_set.add("school_of_forestry")

    if row_get(row, "veterinary_college") is True:
        business_category_set.add("veterinary_college")

    if business_category_set & {
        "educational_institution",
        "public_institution_of_higher_education",
        "private_institution_of_higher_education",
        "school_of_forestry",
        "minority_serving_institution_of_higher_education",
        "veterinary_college",
    }:
        business_category_set.add("higher_education")

    # GOVERNMENT
    if (
        row_get(row, "us_federal_government") is True
        or row_get(row, "federal_agency") is True
        or row_get(row, "us_government_entity") is True
    ):
        business_category_set.add("national_government")

    if row_get(row, "interstate_entity") is True:
        business_category_set.add("interstate_entity")

    if row_get(row, "us_state_government") is True:
        business_category_set.add("regional_and_state_government")

    if row_get(row, "council_of_governments") is True:
        business_category_set.add("council_of_governments")

    if (
        row_get(row, "city_local_government") is True
        or row_get(row, "county_local_government") is True
        or row_get(row, "inter_municipal_local_gove") is True
        or row_get(row, "municipality_local_governm") is True
        or row_get(row, "township_local_government") is True
        or row_get(row, "us_local_government") is True
        or row_get(row, "local_government_owned") is True
        or row_get(row, "school_district_local_gove") is True
    ):
        business_category_set.add("local_government")

    if row_get(row, "us_tribal_government") is True or row_get(row, "indian_tribe_federally_rec") is True:
        business_category_set.add("indian_native_american_tribal_government")

    if (
        row_get(row, "housing_authorities_public") is True
        or row_get(row, "airport_authority") is True
        or row_get(row, "port_authority") is True
        or row_get(row, "transit_authority") is True
        or row_get(row, "planning_commission") is True
    ):
        business_category_set.add("authorities_and_commissions")

    if business_category_set & {
        "national_government",
        "regional_and_state_government",
        "local_government",
        "indian_native_american_tribal_government",
        "authorities_and_commissions",
        "interstate_entity",
        "council_of_governments",
    }:
        business_category_set.add("government")

    return sorted(business_category_set)
