DROP TABLE IF EXISTS legal_entity_new;

CREATE TABLE legal_entity_new AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY 1) AS legal_entity_id,
        *
    FROM
    (
        (
            -- RECIPIENTS: FPDS
            SELECT
                TRUE AS is_fpds,
                transaction_fpds_new.detached_award_proc_unique AS transaction_unique_id,
                references_location_new.location_id AS location_id,
                 'DBR'::TEXT AS data_source,
                ultimate_parent_unique_ide AS parent_recipient_unique_id,
                awardee_or_recipient_legal AS recipient_name,
                vendor_doing_as_business_n AS vendor_doing_as_business_name,
                vendor_phone_number,
                vendor_fax_number,
                NULL AS business_types,
                NULL AS business_types_description,
                compile_fpds_business_categories(small_business_competitive, for_profit_organization, alaskan_native_owned_corpo, american_indian_owned_busi, asian_pacific_american_own, black_american_owned_busin, hispanic_american_owned_bu, native_american_owned_busi, native_hawaiian_owned_busi, subcontinent_asian_asian_i, tribally_owned_business, other_minority_owned_busin, minority_owned_business, women_owned_small_business, economically_disadvantaged, joint_venture_women_owned, joint_venture_economically, woman_owned_business, service_disabled_veteran_o, veteran_owned_business, c8a_program_participant, the_ability_one_program, dot_certified_disadvantage, emerging_small_business, federally_funded_research, historically_underutilized, labor_surplus_area_firm, sba_certified_8_a_joint_ve, self_certified_small_disad, small_agricultural_coopera, small_disadvantaged_busine, community_developed_corpor, domestic_or_foreign_entity, foreign_owned_and_located, foreign_government, international_organization, foundation, community_development_corp, nonprofit_organization, other_not_for_profit_organ, state_controlled_instituti, c1862_land_grant_college, c1890_land_grant_college, c1994_land_grant_college, private_university_or_coll, minority_institution, historically_black_college, tribal_college, alaskan_native_servicing_i, native_hawaiian_servicing, hispanic_servicing_institu, us_federal_government, federal_agency, us_government_entity, interstate_entity, us_state_government, council_of_governments, city_local_government, county_local_government, inter_municipal_local_gove, municipality_local_governm, township_local_government, us_local_government, local_government_owned, school_district_local_gove, us_tribal_government, indian_tribe_federally_rec, housing_authorities_public, airport_authority, port_authority, transit_authority, planning_commission) AS business_categories,
                awardee_or_recipient_uniqu AS recipient_unique_id,

                limited_liability_corporat AS limited_liability_corporation,
                sole_proprietorship,
                partnership_or_limited_lia AS partnership_or_limited_liability_partnership,
                subchapter_s_corporation AS subchapter_scorporation,
                foundation,
                for_profit_organization,
                nonprofit_organization,
                corporate_entity_tax_exemp AS corporate_entity_tax_exempt,
                corporate_entity_not_tax_e AS corporate_entity_not_tax_exempt,
                other_not_for_profit_organ AS other_not_for_profit_organization,
                sam_exception,
                city_local_government,
                county_local_government,
                inter_municipal_local_gove AS inter_municipal_local_government,
                local_government_owned,
                municipality_local_governm AS municipality_local_government,
                school_district_local_gove AS school_district_local_government,
                township_local_government,
                us_state_government,
                us_federal_government,
                federal_agency,
                federally_funded_research AS federally_funded_research_and_development_corp,
                us_tribal_government,
                foreign_government,
                community_developed_corpor AS community_developed_corporation_owned_firm,
                labor_surplus_area_firm,
                small_agricultural_coopera AS small_agricultural_cooperative,
                international_organization,
                us_government_entity,
                emerging_small_business,
                c8a_program_participant AS "8a_program_participant",
                sba_certified_8_a_joint_ve AS sba_certified_8a_joint_venture,
                dot_certified_disadvantage,
                self_certified_small_disad AS self_certified_small_disadvantaged_business,
                historically_underutilized AS historically_underutilized_business_zone,
                small_disadvantaged_busine AS small_disadvantaged_business,
                the_ability_one_program,
                historically_black_college,
                c1862_land_grant_college AS "1862_land_grant_college",
                c1890_land_grant_college AS "1890_land_grant_college",
                c1994_land_grant_college AS "1994_land_grant_college",
                minority_institution,
                private_university_or_coll AS private_university_or_college,
                school_of_forestry,
                state_controlled_instituti AS state_controlled_institution_of_higher_learning,
                tribal_college,
                veterinary_college,
                educational_institution,
                alaskan_native_servicing_i AS alaskan_native_servicing_institution,
                community_development_corp AS community_development_corporation,
                native_hawaiian_servicing AS native_hawaiian_servicing_institution,
                domestic_shelter,
                manufacturer_of_goods,
                hospital_flag,
                veterinary_hospital,
                hispanic_servicing_institu AS hispanic_servicing_institution,
                woman_owned_business,
                minority_owned_business,
                women_owned_small_business,
                economically_disadvantaged AS economically_disadvantaged_women_owned_small_business,
                joint_venture_women_owned AS joint_venture_women_owned_small_business,
                joint_venture_economically AS joint_venture_economic_disadvantaged_women_owned_small_bus,
                veteran_owned_business,
                service_disabled_veteran_o AS service_disabled_veteran_owned_business,
                contracts,
                grants,
                receives_contracts_and_gra AS receives_contracts_and_grants,
                airport_authority,
                council_of_governments,
                housing_authorities_public AS housing_authorities_public_tribal,
                interstate_entity,
                planning_commission,
                port_authority,
                transit_authority,
                foreign_owned_and_located,
                american_indian_owned_busi AS american_indian_owned_business,
                alaskan_native_owned_corpo AS alaskan_native_owned_corporation_or_firm,
                indian_tribe_federally_rec AS indian_tribe_federally_recognized,
                native_hawaiian_owned_busi AS native_hawaiian_owned_business,
                tribally_owned_business,
                asian_pacific_american_own AS asian_pacific_american_owned_business,
                black_american_owned_busin AS black_american_owned_business,
                hispanic_american_owned_bu AS hispanic_american_owned_business,
                native_american_owned_busi AS native_american_owned_business,
                subcontinent_asian_asian_i AS subcontinent_asian_asian_indian_american_owned_business,
                other_minority_owned_busin AS other_minority_owned_business,
                us_local_government,
                undefinitized_action,
                domestic_or_foreign_entity,
                domestic_or_foreign_e_desc AS domestic_or_foreign_entity_description,
                division_name,
                division_number_or_office AS division_number,
                last_modified::DATE AS last_modified_date,
                action_date::DATE AS certified_date,
                NULL AS reporting_period_start,
                NULL AS reporting_period_end,
                CURRENT_TIMESTAMP AS create_date,
                CURRENT_TIMESTAMP AS update_date,

                NULL AS city_township_government,
                NULL AS special_district_government,
                NULL AS small_business,
                NULL AS small_business_description,
                NULL AS individual
            FROM
                transaction_fpds_new
                INNER JOIN
                references_location_new ON transaction_fpds_new.detached_award_proc_unique = references_location_new.transaction_unique_id AND references_location_new.is_fpds = TRUE AND references_location_new.recipient_flag = TRUE
        )

        UNION ALL

        (
            -- RECIPIENTS: FABS
            SELECT
                FALSE as is_fpds,
                transaction_fabs_new.afa_generated_unique AS transaction_unique_id,
                references_location_new.location_id AS location_id,
                 'DBR'::TEXT AS data_source,
                NULL AS parent_recipient_unique_id,
                awardee_or_recipient_legal AS recipient_name,
                NULL AS vendor_doing_as_business_name,
                NULL AS vendor_phone_number,
                NULL AS vendor_fax_number,
                business_types,
                CASE
                    WHEN UPPER(business_types) IN ('A', '00') THEN 'STATE GOVERNMENT'
                    WHEN UPPER(business_types) IN ('B', '01') THEN 'COUNTY GOVERNMENT'
                    WHEN UPPER(business_types) IN ('C', '02') THEN 'CITY OR TOWNSHIP GOVERNMENT'
                    WHEN UPPER(business_types) IN ('D', '04') THEN 'SPECIAL DISTRICT GOVERNMENT'
                    WHEN UPPER(business_types) = 'E' THEN 'REGIONAL ORGANIZATION'
                    WHEN UPPER(business_types) = 'F' THEN 'U.S. TERRITORY OR POSSESSION'
                    WHEN UPPER(business_types) IN ('G', '05') THEN 'INDEPENDENT SCHOOL DISTRICT'
                    WHEN UPPER(business_types) IN ('H', '06') THEN 'PUBLIC/STATE CONTROLLED INSTITUTION OF HIGHER EDUCATION'
                    WHEN UPPER(business_types) IN ('I', '11') THEN 'INDIAN/NATIVE AMERICAN TRIBAL GOVERNMENT (FEDERALLY RECOGNIZED)'
                    WHEN UPPER(business_types) = 'J' THEN 'INDIAN/NATIVE AMERICAN TRIBAL GOVERNMENT (OTHER THAN FEDERALLY RECOGNIZED)'
                    WHEN UPPER(business_types) = 'K' THEN 'INDIAN/NATIVE AMERICAN TRIBAL DESIGNATED ORGANIZATION'
                    WHEN UPPER(business_types) = 'L' THEN 'PUBLIC/INDIAN HOUSING AUTHORITY'
                    WHEN UPPER(business_types) = 'M' THEN 'NONPROFIT WITH 501(C)(3) IRS STATUS (OTHER THAN INSTITUTION OF HIGHER EDUCATION)'
                    WHEN UPPER(business_types) = 'N' THEN 'NONPROFIT WITHOUT 501(C)(3) IRS STATUS (OTHER THAN INSTITUTION OF HIGHER EDUCATION)'
                    WHEN UPPER(business_types) IN ('O', '20') THEN 'PRIVATE INSTITUTION OF HIGHER EDUCATION'
                    WHEN UPPER(business_types) IN ('P', '21') THEN 'INDIVIDUAL'
                    WHEN UPPER(business_types) IN ('Q', '22') THEN 'FOR-PROFIT ORGANIZATION (OTHER THAN SMALL BUSINESS)'
                    WHEN UPPER(business_types) IN ('R', '23') THEN 'SMALL BUSINESS'
                    WHEN UPPER(business_types) = 'S' THEN 'HISPANIC-SERVING INSTITUTION'
                    WHEN UPPER(business_types) = 'T' THEN 'HISTORICALLY BLACK COLLEGES AND UNIVERSITIES (HBCUS)'
                    WHEN UPPER(business_types) = 'U' THEN 'TRIBALLY CONTROLLED COLLEGES AND UNIVERSITIES (TCCUS)'
                    WHEN UPPER(business_types) = 'V' THEN 'ALASKA NATIVE AND NATIVE HAWAIIAN SERVING INSTITUTIONS'
                    WHEN UPPER(business_types) = 'W' THEN 'NON-DOMESTIC (NON-US) ENTITY'
                    WHEN UPPER(business_types) IN ('X', '24') THEN 'OTHER'
                    ELSE 'UNKNOWN TYPES'
                END AS business_types_description,
                compile_fabs_business_categories(business_types) AS business_categories,
                awardee_or_recipient_uniqu AS recipient_unique_id,

                NULL AS limited_liability_corporation,
                NULL AS sole_proprietorship,
                NULL AS partnership_or_limited_liability_partnership,
                NULL AS subchapter_scorporation,
                NULL AS foundation,
                NULL AS for_profit_organization,
                NULL AS nonprofit_organization,
                NULL AS corporate_entity_tax_exempt,
                NULL AS corporate_entity_not_tax_exempt,
                NULL AS other_not_for_profit_organization,
                NULL AS sam_exception,
                NULL AS city_local_government,
                NULL AS county_local_government,
                NULL AS inter_municipal_local_government,
                NULL AS local_government_owned,
                NULL AS municipality_local_government,
                NULL AS school_district_local_government,
                NULL AS township_local_government,
                NULL AS us_state_government,
                NULL AS us_federal_government,
                NULL AS federal_agency,
                NULL AS federally_funded_research_and_development_corp,
                NULL AS us_tribal_government,
                NULL AS foreign_government,
                NULL AS community_developed_corporation_owned_firm,
                NULL AS labor_surplus_area_firm,
                NULL AS small_agricultural_cooperative,
                NULL AS international_organization,
                NULL AS us_government_entity,
                NULL AS emerging_small_business,
                NULL AS "8a_program_participant",
                NULL AS sba_certified_8a_joint_venture,
                NULL AS dot_certified_disadvantage,
                NULL AS self_certified_small_disadvantaged_business,
                NULL AS historically_underutilized_business_zone,
                NULL AS small_disadvantaged_business,
                NULL AS the_ability_one_program,
                NULL AS historically_black_college,
                NULL AS "1862_land_grant_college",
                NULL AS "1890_land_grant_college",
                NULL AS "1994_land_grant_college",
                NULL AS minority_institution,
                NULL AS private_university_or_college,
                NULL AS school_of_forestry,
                NULL AS state_controlled_institution_of_higher_learning,
                NULL AS tribal_college,
                NULL AS veterinary_college,
                NULL AS educational_institution,
                NULL AS alaskan_native_servicing_institution,
                NULL AS community_development_corporation,
                NULL AS native_hawaiian_servicing_institution,
                NULL AS domestic_shelter,
                NULL AS manufacturer_of_goods,
                NULL AS hospital_flag,
                NULL AS veterinary_hospital,
                NULL AS hispanic_servicing_institution,
                NULL AS woman_owned_business,
                NULL AS minority_owned_business,
                NULL AS women_owned_small_business,
                NULL AS economically_disadvantaged_women_owned_small_business,
                NULL AS joint_venture_women_owned_small_business,
                NULL AS joint_venture_economic_disadvantaged_women_owned_small_bus,
                NULL AS veteran_owned_business,
                NULL AS service_disabled_veteran_owned_business,
                NULL AS contracts,
                NULL AS grants,
                NULL AS receives_contracts_and_grants,
                NULL AS airport_authority,
                NULL AS council_of_governments,
                NULL AS housing_authorities_public_tribal,
                NULL AS interstate_entity,
                NULL AS planning_commission,
                NULL AS port_authority,
                NULL AS transit_authority,
                NULL AS foreign_owned_and_located,
                NULL AS american_indian_owned_business,
                NULL AS alaskan_native_owned_corporation_or_firm,
                NULL AS indian_tribe_federally_recognized,
                NULL AS native_hawaiian_owned_business,
                NULL AS tribally_owned_business,
                NULL AS asian_pacific_american_owned_business,
                NULL AS black_american_owned_business,
                NULL AS hispanic_american_owned_business,
                NULL AS native_american_owned_business,
                NULL AS subcontinent_asian_asian_indian_american_owned_business,
                NULL AS other_minority_owned_business,
                NULL AS us_local_government,
                NULL AS undefinitized_action,
                NULL AS domestic_or_foreign_entity,
                NULL AS domestic_or_foreign_entity_description,
                NULL AS division_name,
                NULL AS division_number,
                modified_at::DATE AS last_modified_date,
                action_date::DATE AS certified_date,
                NULL AS reporting_period_start,
                NULL AS reporting_period_end,
                CURRENT_TIMESTAMP AS create_date,
                CURRENT_TIMESTAMP AS update_date,

                NULL AS city_township_government,
                NULL AS special_district_government,
                NULL AS small_business,
                NULL AS small_business_description,
                NULL AS individual
            FROM
                transaction_fabs_new
                INNER JOIN
                references_location_new ON transaction_fabs_new.afa_generated_unique = references_location_new.transaction_unique_id AND references_location_new.is_fpds = FALSE AND references_location_new.recipient_flag = TRUE
        )
    ) AS recipients
);