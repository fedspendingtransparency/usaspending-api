FORMAT: 1A
HOST: https://api.usaspending.gov

# Awards [/api/v2/awards/{award_id}/]

This endpoint is used to power USAspending.gov's award profile pages. This data can be used to view details about a specific award.

## GET

This endpoint returns a list of data that is associated with the Award.

+ Request (application/json)
    A request with a contract id; accepts the v2 generated award hash or internal database id.
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `award_id`: `CONT_AWD_H907_9700_SPE2DX16D1500_9700` (required, string)

+ Response 200 (application/json)
    + Attributes (ContractResponse)
    + Body

            {
                "id": 22834500,
                "generated_unique_award_id": "CONT_AWD_H907_9700_SPE2DX16D1500_9700",
                "piid": "H907",
                "category": "contract",
                "type": "C",
                "type_description": "DELIVERY ORDER",
                "description": "4534723185!PV PHARM BRANCH, VENDOR, CAGE 0U9U0",
                "total_obligation": 61241.34,
                "subaward_count": 0,
                "total_subaward_amount": null,
                "date_signed": "2016-06-24",
                "base_exercised_options": 61241.34,
                "base_and_all_options": 61241.34,
                "parent_award": {
                    "agency_id": 1173,
                    "agency_name": "Department of Defense",
                    "agency_slug": "department-of-defense",
                    "sub_agency_id": "9700",
                    "sub_agency_name": "Department of Defense",
                    "award_id": 69513842,
                    "generated_unique_award_id": "CONT_IDV_SPE2DX16D1500_9700",
                    "idv_type_description": "IDC",
                    "multiple_or_single_aw_desc": "SINGLE AWARD",
                    "piid": "SPE2DX16D1500",
                    "type_of_idc_description": "INDEFINITE DELIVERY / INDEFINITE QUANTITY"
                },
                "latest_transaction_contract_data": {
                    "idv_type_description": null,
                    "type_of_idc_description": null,
                    "referenced_idv_agency_iden": "9700",
                    "referenced_idv_agency_desc": "DEPT OF DEFENSE",
                    "solicitation_identifier": null,
                    "solicitation_procedures": "NP",
                    "number_of_offers_received": "4",
                    "extent_competed": "A",
                    "type_set_aside": "NONE",
                    "type_set_aside_description": "NO SET ASIDE USED.",
                    "evaluated_preference": "NONE",
                    "fed_biz_opps": "N",
                    "fed_biz_opps_description": "NO",
                    "small_business_competitive": false,
                    "product_or_service_code": "6505",
                    "naics": "325411",
                    "naics_description": "MEDICINAL AND BOTANICAL MANUFACTURING",
                    "sea_transportation": "U",
                    "clinger_cohen_act_planning": "N",
                    "labor_standards": "X",
                    "cost_or_pricing_data": "N",
                    "domestic_or_foreign_entity": "A",
                    "foreign_funding": "X",
                    "major_program": null,
                    "program_acronym": null,
                    "subcontracting_plan": null,
                    "multi_year_contract": "N",
                    "consolidated_contract": "N",
                    "type_of_contract_pricing": "J",
                    "national_interest_action": "NONE",
                    "national_interest_action_description": "NONE",
                    "multiple_or_single_award_description": null,
                    "solicitation_procedures_description": "NEGOTIATED PROPOSAL/QUOTE",
                    "extent_competed_description": "FULL AND OPEN COMPETITION",
                    "other_than_full_and_open": null,
                    "other_than_full_and_open_description": null,
                    "commercial_item_acquisition": "D",
                    "commercial_item_acquisition_description": "COMMERCIAL ITEM PROCEDURES NOT USED",
                    "commercial_item_test_program": "N",
                    "commercial_item_test_program_description": "NO",
                    "evaluated_preference_description": "NO PREFERENCE USED",
                    "fair_opportunity_limited": null,
                    "fair_opportunity_limited_description": null,
                    "product_or_service_description": "DRUGS AND BIOLOGICALS",
                    "dod_claimant_program": "C9B",
                    "dod_claimant_program_description": "MEDICAL AND DENTAL SUPPLIES AND EQUIPMENT",
                    "dod_acquisition_program": "000",
                    "dod_acquisition_program_description": "NONE",
                    "information_technology_commercial_item_category": "Z",
                    "information_technology_commercial_item_category_description": "NOT IT PRODUCTS OR SERVICES",
                    "sea_transportation_description": "UNKNOWN",
                    "clinger_cohen_act_planning_description": "NO",
                    "construction_wage_rate": "X",
                    "construction_wage_rate_description": "NOT APPLICABLE",
                    "labor_standards_description": "NOT APPLICABLE",
                    "materials_supplies": "N",
                    "materials_supplies_description": "NO",
                    "cost_or_pricing_data_description": "NO",
                    "domestic_or_foreign_entity_description": "U.S. OWNED BUSINESS",
                    "foreign_funding_description": "NOT APPLICABLE",
                    "interagency_contracting_authority": "X",
                    "interagency_contracting_authority_description": "NOT APPLICABLE",
                    "price_evaluation_adjustment": null,
                    "subcontracting_plan_description": null,
                    "multi_year_contract_description": "NO",
                    "purchase_card_as_payment_method": "N",
                    "purchase_card_as_payment_method_description": "NO",
                    "consolidated_contract_description": "NO",
                    "type_of_contract_pricing_description": "FIRM FIXED PRICE"
                },
                "funding_agency": {
                    "id": 1219,
                    "has_agency_page": true,
                    "toptier_agency": {
                        "name": "Department of Defense",
                        "code": "097",
                        "abbreviation": "DOD",
                        "slug": "department-of-defense"
                    },
                    "subtier_agency": {
                        "name": "Defense Logistics Agency",
                        "code": "97AS",
                        "abbreviation": "DLA"
                    },
                    "office_agency_name": "DLA TROOP SUPPORT"
                },
                "awarding_agency": {
                    "id": 1219,
                    "has_agency_page": true,
                    "toptier_agency": {
                        "name": "Department of Defense",
                        "code": "097",
                        "abbreviation": "DOD",
                        "slug": "department-of-defense"
                    },
                    "subtier_agency": {
                        "name": "Defense Logistics Agency",
                        "code": "97AS",
                        "abbreviation": "DLA"
                    },
                    "office_agency_name": "DLA TROOP SUPPORT"
                },
                "period_of_performance": {
                    "start_date": "2016-06-24",
                    "end_date": "2016-06-27",
                    "last_modified_date": "2019-10-27 20:06:44",
                    "potential_end_date": "2016-06-27 00:00:00"
                },
                "recipient": {
                    "recipient_hash": "a704b6c0-d296-fc75-79e9-7a9546b3b634-C",
                    "recipient_name": "MCKESSON CORPORATION",
                    "recipient_uei": "JTAPCFM4NSL4",
                    "recipient_unique_id": "105919864",
                    "parent_recipient_hash": "8b51f520-c590-0517-1e0d-45336a1b9332-P",
                    "parent_recipient_name": "MCKESSON CORPORATION",
                    "parent_recipient_uei": "S1RUKWWRYFL6",
                    "parent_recipient_unique_id": "177667227",
                    "business_categories": [
                        "Category Business",
                        "Corporate Entity Not Tax Exempt",
                        "Not Designated a Small Business",
                        "Special Designations",
                        "U.S.-Owned Business"
                    ],
                    "location": {
                        "location_country_code": "USA",
                        "country_name": "UNITED STATES",
                        "state_code": "CA",
                        "state_name": "CALIFORNIA",
                        "city_name": "WEST SACRAMENTO",
                        "county_code": "113",
                        "county_name": "YOLO",
                        "address_line1": "3775 SEAPORT BLVD",
                        "address_line2": null,
                        "address_line3": null,
                        "congressional_code": "06",
                        "zip4": "3558",
                        "zip5": "95691",
                        "foreign_postal_code": null,
                        "foreign_province": null
                    }
                },
                "executive_details": {
                    "officers": [
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        }
                    ]
                },
                "place_of_performance": {
                    "location_country_code": "USA",
                    "country_name": "UNITED STATES",
                    "county_code": "113",
                    "county_name": "YOLO",
                    "city_name": "WEST SACRAMENTO",
                    "state_code": "CA",
                    "state_name": "CALIFORNIA",
                    "congressional_code": "06",
                    "zip4": "3558",
                    "zip5": "95691",
                    "address_line1": null,
                    "address_line2": null,
                    "address_line3": null,
                    "foreign_province": null,
                    "foreign_postal_code": null
                },
                "psc_hierarchy": {
                    "toptier_code": {},
                    "midtier_code": {
                        "code": "65",
                        "description": "MEDICAL/DENTAL/VETERINARY EQPT/SUPP"
                    },
                    "subtier_code": {},
                    "base_code": {
                        "code": "6505",
                        "description": "DRUGS AND BIOLOGICALS"
                    }
                },
                "naics_hierarchy": {
                    "toptier_code": {
                        "code": "32",
                        "description": "Manufacturing"
                    },
                    "midtier_code": {
                        "code": "3254",
                        "description": "Pharmaceutical and Medicine Manufacturing"
                    },
                    "base_code": {
                        "code": "325411",
                        "description": "Medicinal and Botanical Manufacturing"
                    }
                },
                "total_account_outlay": 3.0,
                "total_account_obligation": 3.0,
                "account_outlays_by_defc": [
                    {
                        "code": "P",
                        "amount": 2.0
                    },
                    {
                        "code": "L",
                        "amount": 1.0
                    }
                ],
                "account_obligations_by_defc": [
                    {
                        "code": "P",
                        "amount": 2.0
                    },
                    {
                        "code": "L",
                        "amount": 1.0
                    }
                ]
            }

+ Request (application/json)
    A request with a financial assistance id; accepts the v2 generated award hash or internal database id.
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `award_id`: `ASST_NON_12FA00PY54661041_12D2` (required, string)

+ Response 200 (application/json)
    + Attributes (FinancialAssistanceResponse)
    + Body

            {
                "id": 70140000,
                "generated_unique_award_id": "ASST_NON_12FA00PY54661041_12D2",
                "fain": "12FA00PY54661041",
                "uri": null,
                "category": "direct payment",
                "type": "10",
                "type_description": "DIRECT PAYMENT WITH UNRESTRICTED USE (RETIREMENT, PENSION, VETERANS BENEFITS, ETC.) (D)",
                "description": "AGRICULTURAL RISK COVERAGE PROG - COUNTY",
                "subaward_count": 0,
                "total_subaward_amount": null,
                "awarding_agency": {
                    "id": 153,
                    "has_agency_page": true,
                    "toptier_agency": {
                        "name": "Department of Agriculture",
                        "code": "012",
                        "abbreviation": "USDA",
                        "slug": "department-of-agriculture"
                    },
                    "subtier_agency": {
                        "name": "Farm Service Agency",
                        "code": "12D2",
                        "abbreviation": "FSA"
                    },
                    "office_agency_name": null
                },
                "funding_agency": {
                    "id": 153,
                    "has_agency_page": true,
                    "toptier_agency": {
                        "name": "Department of Agriculture",
                        "code": "012",
                        "abbreviation": "USDA",
                        "slug": "department-of-agriculture"
                    },
                    "subtier_agency": {
                        "name": "Farm Service Agency",
                        "code": "12D2",
                        "abbreviation": "FSA"
                    },
                    "office_agency_name": null
                },
                "recipient": {
                    "recipient_hash": "6e4362a8-7dd7-8d86-d2ff-8faa5eefe0aa-R",
                    "recipient_name": "REDACTED DUE TO PII",
                    "recipient_uei": null,
                    "recipient_unique_id": null,
                    "parent_recipient_hash": null,
                    "parent_recipient_name": null,
                    "parent_recipient_uei": null,
                    "parent_recipient_unique_id": null,
                    "business_categories": [
                        "Individuals"
                    ],
                    "location": {
                        "location_country_code": "USA",
                        "country_name": "UNITED STATES",
                        "state_code": "ND",
                        "state_name": "NORTH DAKOTA",
                        "city_name": "GARRISON",
                        "county_code": "055",
                        "county_name": "MCLEAN",
                        "address_line1": null,
                        "address_line2": null,
                        "address_line3": null,
                        "congressional_code": "00",
                        "zip4": null,
                        "zip5": "58540",
                        "foreign_postal_code": null,
                        "foreign_province": null
                    }
                },
                "total_subsidy_cost": null,
                "total_loan_value": null,
                "total_obligation": 479.0,
                "date_signed": "2017-10-04",
                "base_and_all_options": null,
                "base_exercised_options": null,
                "non_federal_funding": 0.0,
                "total_funding": 479.0,
                "record_type": 3,
                "cfda_info": [
                    {
                        "applicant_eligibility": "An eligible producer is eligible to enter into a contract if 1) the owner of the farm has an ownership of a crop and assumes all or a part of the risk producing a crop that is commensurate with that claimed ownership of the crop; 2) a producer, other than the owner, on a farm with a share-rent lease for such farm, regardless of the length of the lease, if the owner of the farm enters into the same contract; 3) a producer, other than an owner, on a farm who rents such farm under a lease expiring on or after September 30 of the year of the contract in which case the owner is not required to enter into the contract; 4) a producer, other than an owner , on a farm who cash rents such farm under a leasing expiring before September 30 of the year of the contract; 5)An owner of an eligible farm who cash rents such farm and the lease expires before September 30 of the year of the contract, if the tenant declines to enter into a contract for the applicable year.",
                        "beneficiary_eligibility": "PLC provides payments to eligible producers on farms enrolled for the 2019 through 2023 crop years.",
                        "cfda_federal_agency": "FARM SERVICE AGENCY, AGRICULTURE, DEPARTMENT OF",
                        "cfda_number": "10.112",
                        "cfda_objectives": "The Price Loss Coverage program (PLC) is carried over from the 2014 Farm Bill. PLC provides income support through price loss payments to eligible producers.",
                        "cfda_obligations": "(Direct Payments for Specified Use) FY 18$1,231,000,000.00; FY 19 est $3,965,000,000.00; FY 20 est $4,691,000,000.00; FY 17$1,952,650,000.00; FY 16$5,841,000,000.00; - ",
                        "cfda_popular_name": "(PLC)",
                        "cfda_title": "Price Loss Coverage",
                        "cfda_website": "http://www.fsa.usda.gov",
                        "federal_action_obligation_amount": 479.0,
                        "non_federal_funding_amount": 0.0,
                        "sam_website": "https://beta.sam.gov/fal/25b529f3b5f94b6c939bc0ae8424ae6c/view",
                        "total_funding_amount": 479.0
                    }
                ],
                "transaction_obligated_amount": null,
                "period_of_performance": {
                    "start_date": "2017-10-04",
                    "end_date": "2017-10-04",
                    "last_modified_date": "2018-10-27"
                },
                "executive_details": {
                    "officers": [
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        }
                    ]
                },
                "place_of_performance": {
                    "location_country_code": "USA",
                    "country_name": "UNITED STATES",
                    "county_code": "055",
                    "county_name": "MCLEAN",
                    "city_name": "GARRISON",
                    "state_code": "ND",
                    "state_name": "NORTH DAKOTA",
                    "congressional_code": "00",
                    "zip4": null,
                    "zip5": "58540",
                    "address_line1": null,
                    "address_line2": null,
                    "address_line3": null,
                    "foreign_province": null,
                    "foreign_postal_code": null
                },
                "total_account_outlay": 3.0,
                "total_account_obligation": 3.0,
                "account_outlays_by_defc": [
                    {
                        "code": "P",
                        "amount": 2.0
                    },
                    {
                        "code": "L",
                        "amount": 1.0
                    }
                ],
                "account_obligations_by_defc": [
                    {
                        "code": "P",
                        "amount": 2.0
                    },
                    {
                        "code": "L",
                        "amount": 1.0
                    }
                ],
                "funding_opportunity": {
                    "number": "NOT APPLICABLE",
                    "goals": : "NOT APPLICABLE"
                }
            }

+ Request (application/json)
    A request with an IDV id; accepts the v2 generated award hash or internal database id.
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `award_id`: `CONT_IDV_FA304715A0037_9700` (required, string)

+ Response 200 (application/json)
    + Attributes (IDVResponse)
    + Body

            {
                "id": 69023000,
                "generated_unique_award_id": "CONT_IDV_FA304715A0037_9700",
                "piid": "FA304715A0037",
                "category": "idv",
                "type": "IDV_E",
                "type_description": "BPA",
                "description": "IGF::CT::IGF TRANSPORTATION OF MILITARY WORKING DOGS",
                "total_obligation": 0.0,
                "subaward_count": 0,
                "total_subaward_amount": null,
                "date_signed": "2015-07-21",
                "base_exercised_options": 0.0,
                "base_and_all_options": 0.0,
                "parent_award": null,
                "latest_transaction_contract_data": {
                    "idv_type_description": "BPA",
                    "type_of_idc_description": null,
                    "referenced_idv_agency_iden": null,
                    "referenced_idv_agency_desc": null,
                    "solicitation_identifier": null,
                    "solicitation_procedures": null,
                    "number_of_offers_received": null,
                    "extent_competed": null,
                    "type_set_aside": null,
                    "type_set_aside_description": null,
                    "evaluated_preference": null,
                    "fed_biz_opps": "X",
                    "fed_biz_opps_description": "NOT APPLICABLE",
                    "small_business_competitive": false,
                    "product_or_service_code": "V111",
                    "naics": "488510",
                    "naics_description": "FREIGHT TRANSPORTATION ARRANGEMENT",
                    "sea_transportation": "N",
                    "clinger_cohen_act_planning": "N",
                    "labor_standards": "X",
                    "cost_or_pricing_data": null,
                    "domestic_or_foreign_entity": "D",
                    "foreign_funding": "X",
                    "major_program": null,
                    "program_acronym": null,
                    "subcontracting_plan": "B",
                    "multi_year_contract": null,
                    "consolidated_contract": "N",
                    "type_of_contract_pricing": "1",
                    "national_interest_action": "NONE",
                    "national_interest_action_description": "NONE",
                    "multiple_or_single_award_description": "SINGLE AWARD",
                    "solicitation_procedures_description": null,
                    "extent_competed_description": null,
                    "other_than_full_and_open": null,
                    "other_than_full_and_open_description": null,
                    "commercial_item_acquisition": "A",
                    "commercial_item_acquisition_description": "COMMERCIAL ITEM",
                    "commercial_item_test_program": null,
                    "commercial_item_test_program_description": null,
                    "evaluated_preference_description": null,
                    "fair_opportunity_limited": null,
                    "fair_opportunity_limited_description": null,
                    "product_or_service_description": "TRANSPORTATION/TRAVEL/RELOCATION- TRANSPORTATION: AIR FREIGHT",
                    "dod_claimant_program": null,
                    "dod_claimant_program_description": null,
                    "dod_acquisition_program": null,
                    "dod_acquisition_program_description": null,
                    "information_technology_commercial_item_category": null,
                    "information_technology_commercial_item_category_description": null,
                    "sea_transportation_description": "NO",
                    "clinger_cohen_act_planning_description": "NO",
                    "construction_wage_rate": "N",
                    "construction_wage_rate_description": "NO",
                    "labor_standards_description": "NOT APPLICABLE",
                    "materials_supplies": "N",
                    "materials_supplies_description": "NO",
                    "cost_or_pricing_data_description": null,
                    "domestic_or_foreign_entity_description": "FOREIGN-OWNED BUSINESS NOT INCORPORATED IN THE U.S.",
                    "foreign_funding_description": "NOT APPLICABLE",
                    "interagency_contracting_authority": "X",
                    "interagency_contracting_authority_description": "NOT APPLICABLE",
                    "price_evaluation_adjustment": null,
                    "subcontracting_plan_description": "PLAN NOT REQUIRED ",
                    "multi_year_contract_description": null,
                    "purchase_card_as_payment_method": null,
                    "purchase_card_as_payment_method_description": null,
                    "consolidated_contract_description": "NO",
                    "type_of_contract_pricing_description": "ORDER DEPENDENT (IDV ALLOWS PRICING ARRANGEMENT TO BE DETERMINED SEPARATELY FOR EACH ORDER)"
                },
                "funding_agency": {
                    "id": 1196,
                    "has_agency_page": true,
                    "toptier_agency": {
                        "name": "Department of Defense",
                        "code": "097",
                        "abbreviation": "DOD",
                        "slug": "department-of-defense"
                    },
                    "subtier_agency": {
                        "name": "Department of the Air Force",
                        "code": "5700",
                        "abbreviation": "USAF"
                    },
                    "office_agency_name": "341 TRS DORR"
                },
                "awarding_agency": {
                    "id": 1196,
                    "has_agency_page": true,
                    "toptier_agency": {
                        "name": "Department of Defense",
                        "code": "097",
                        "abbreviation": "DOD",
                        "slug": "department-of-defense"
                    },
                    "subtier_agency": {
                        "name": "Department of the Air Force",
                        "code": "5700",
                        "abbreviation": "USAF"
                    },
                    "office_agency_name": "FA3016  502 CONS CL"
                },
                "period_of_performance": {
                    "start_date": "2015-07-17",
                    "end_date": "2020-07-16",
                    "last_modified_date": "2018-06-11 16:42:25",
                    "potential_end_date": null
                },
                "recipient": {
                    "recipient_hash": "fcc89367-99ce-1509-091c-ce26fb52556b-C",
                    "recipient_name": "GRADLYN - G.K. AIRFREIGHT SERVICE GMBH",
                    "recipient_uei": "EBUHL3LJ3JE9",
                    "recipient_unique_id": "317771921",
                    "parent_recipient_hash": "fcc89367-99ce-1509-091c-ce26fb52556b-P",
                    "parent_recipient_name": "GK AIRFREIGHT-SERVICE GRADLYN KENNELS GMBH",
                    "parent_recipient_uei": "EBUHL3LJ3JE9",
                    "parent_recipient_unique_id": "317771921",
                    "business_categories": [
                        "Category Business",
                        "Foreign Owned",
                        "International Organization",
                        "Not Designated a Small Business",
                        "Special Designations"
                    ],
                    "location": {
                        "location_country_code": "DEU",
                        "country_name": "GERMANY",
                        "state_code": null,
                        "state_name": "HESSEN",
                        "city_name": "FRANKFURT AM MAIN",
                        "county_code": null,
                        "county_name": null,
                        "address_line1": "CARGO CITY NORTH, GATE 26",
                        "address_line2": "BUILDING 458",
                        "address_line3": null,
                        "congressional_code": null,
                        "zip4": "60549",
                        "zip5": null,
                        "foreign_postal_code": null,
                        "foreign_province": null
                    }
                },
                "executive_details": {
                    "officers": [
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        },
                        {
                            "name": null,
                            "amount": null
                        }
                    ]
                },
                "place_of_performance": {
                    "location_country_code": null,
                    "country_name": null,
                    "county_code": null,
                    "county_name": null,
                    "city_name": null,
                    "state_code": null,
                    "state_name": null,
                    "congressional_code": null,
                    "zip4": null,
                    "zip5": null,
                    "address_line1": null,
                    "address_line2": null,
                    "address_line3": null,
                    "foreign_province": null,
                    "foreign_postal_code": null
                },
                "psc_hierarchy": {
                    "toptier_code": {
                        "code": "V",
                        "description": "TRANSPORT, TRAVEL, RELOCATION"
                    },
                    "midtier_code": {
                        "code": "V1",
                        "description": "TRANSPORTATION OF THINGS"
                    },
                    "subtier_code": {},
                    "base_code": {
                        "code": "V111",
                        "description": "TRANSPORTATION/TRAVEL/RELOCATION- TRANSPORTATION: AIR FREIGHT"
                    }
                },
                "naics_hierarchy": {
                    "toptier_code": {
                        "code": "48",
                        "description": "Transportation and Warehousing"
                    },
                    "midtier_code": {
                        "code": "4885",
                        "description": "Freight Transportation Arrangement"
                    },
                    "base_code": {
                        "code": "488510",
                        "description": "Freight Transportation Arrangement"
                    }
                },
                "total_account_outlay": 3.0,
                "total_account_obligation": 3.0,
                "account_outlays_by_defc": [
                    {
                        "code": "P",
                        "amount": 2.0
                    },
                    {
                        "code": "L",
                        "amount": 1.0
                    }
                ],
                "account_obligations_by_defc": [
                    {
                        "code": "P",
                        "amount": 2.0
                    },
                    {
                        "code": "L",
                        "amount": 1.0
                    }
                ]
            }

# Data Structures

## ContractResponse (object)
+ `id` (required, number)
+ `category` (required, enum[string])
    + Members
        + `contract`
+ `type` (required, enum[string])
    + Members
        + `A`
        + `B`
        + `C`
        + `D`
+ `type_description` (required, string)
+ `generated_unique_award_id` (required, string)
+ `piid` (required, string)
    Award id
+ `description` (required, string, nullable)
    Description of the first transaction for this award
+ `total_obligation` (required, number)
+ `base_exercised_options` (required, number, nullable)
+ `base_and_all_options` (required, number)
    The ceiling and amount of the award
+ `date_signed` (required, string, nullable)
+ `subaward_count` (required, number)
+ `total_subaward_amount` (required, number, nullable)
+ `awarding_agency` (required, Agency, fixed-type)
+ `funding_agency` (required, Agency, fixed-type, nullable)
+ `recipient` (required, Recipient, fixed-type)
+ `period_of_performance` (required, PeriodOfPerformance, fixed-type)
+ `place_of_performance` (required, Location, fixed-type)
+ `latest_transaction_contract_data` (required, ContractDetails, fixed-type)
+ `executive_details` (required, Executive, fixed-type)
+ `parent_award` (required, ParentDetails, fixed-type, nullable)
    `null` if the contract has no parent
+ `naics_hierarchy` (required, NAICSHierarchy, fixed-type)
+ `psc_hierarchy` (required, PSCHierarchy, fixed-type)
+ `total_account_outlay` (required, number)
+ `total_account_obligation` (required, number)
+ `account_obligations_by_defc` (required, array[DEFCAmount], fixed-type)
+ `account_outlays_by_defc` (required, array[DEFCAmount], fixed-type)

## IDVResponse (object)
+ `id` (required, number)
+ `category` (required, enum[string])
    + Members
        + `idv`
+ `type` (required, enum[string])
    + Members
        + `IDV_A`
        + `IDV_B`
        + `IDV_B_A`
        + `IDV_B_B`
        + `IDV_B_C`
        + `IDV_C`
        + `IDV_D`
        + `IDV_E`
+ `type_description` (required, string)
+ `generated_unique_award_id` (required, string)
+ `piid` (required, string)
    Award id
+ `description` (required, string, nullable)
    Description of the first transaction for this award
+ `total_obligation` (required, number, nullable)
+ `base_exercised_options` (required, number, nullable)
+ `base_and_all_options` (required, number)
    The ceiling and amount of the award
+ `date_signed` (required, string, nullable)
+ `subaward_count` (required, number)
+ `total_subaward_amount` (required, number, nullable)
+ `awarding_agency` (required, Agency, fixed-type)
+ `funding_agency` (required, Agency, fixed-type, nullable)
+ `recipient` (required, Recipient, fixed-type)
+ `period_of_performance` (required, PeriodOfPerformance, fixed-type)
+ `place_of_performance` (required, Location, fixed-type)
+ `latest_transaction_contract_data` (required, ContractDetails, fixed-type)
+ `executive_details` (required, Executive, fixed-type)
+ `parent_award` (required, ParentDetails, fixed-type, nullable)
    `null` if the idv has no parent
+ `naics_hierarchy` (required, NAICSHierarchy, fixed-type)
+ `psc_hierarchy` (required, PSCHierarchy, fixed-type)
+ `total_account_outlay` (required, number)
+ `total_account_obligation` (required, number)
+ `account_obligations_by_defc` (required, array[DEFCAmount], fixed-type)
+ `account_outlays_by_defc` (required, array[DEFCAmount], fixed-type)
+ `total_outlay` (required, number, nullable)


## FinancialAssistanceResponse (object)
+ `id` (required, number)
+ `category` (required, enum[string])
    + Members
        + `loans`
        + `other`
        + `direct payment`
        + `grant`
+ `type` (required, enum[string])
    + Members
        + `02`
        + `03`
        + `04`
        + `05`
        + `06`
        + `07`
        + `08`
        + `09`
        + `10`
        + `11`
+ `record_type` (required, number)
+ `type_description` (required, string)
+ `generated_unique_award_id` (required, string)
+ `fain` (required, string, nullable)
+ `uri` (required, string, nullable)
+ `description` (required, string, nullable)
+ `transaction_obligated_amount` (required, number, nullable)
    Used for Funding Obligated on Grants pages
+ `total_obligation` (required, number, nullable)
+ `base_exercised_options` (required, number, nullable)
+ `total_subsidy_cost` (required, number, nullable)
    null except for loans
+ `total_loan_value` (required, number, nullable)
    null except for loans
+ `non_federal_funding` (required, number, nullable)
    null except for grants
+ `total_funding` (required, number, nullable)
    null except for grants
+ `date_signed` (required, string, nullable)
+ `subaward_count` (required, number)
+ `total_subaward_amount` (required, number, nullable)
+ `awarding_agency` (required, Agency, fixed-type)
+ `funding_agency` (required, Agency, fixed-type, nullable)
+ `recipient` (required, Recipient, fixed-type)
+ `period_of_performance` (required, PeriodOfPerformanceAssistance, fixed-type)
+ `place_of_performance` (required, Location, fixed-type)
+ `executive_details` (required, Executive, fixed-type)
+ `cfda_info` (required, array[CFDAInfo], fixed-type)
+ `total_account_outlay` (required, number)
+ `total_account_obligation` (required, number)
+ `account_obligations_by_defc` (required, array[DEFCAmount], fixed-type)
+ `account_outlays_by_defc` (required, array[DEFCAmount], fixed-type)
+ `funding_opportunty` (required, FundingOpportunity, fixed-type)

## FundingOpportunity (object)
+ `number` (required, string, nullable)
+ `goals` (required, string, nullable)

## CFDAInfo (object)
+ `applicant_eligibility` (required, string, nullable)
+ `beneficiary_eligibility` (required, string, nullable)
+ `cfda_number` (required, string)
+ `cfda_title` (required, string, nullable)
+ `cfda_objectives` (required, string, nullable)
+ `federal_action_obligation_amount` (required, number)
+ `non_federal_funding_amount` (required, number, nullable)
+ `total_funding_amount` (required, number, nullable)
+ `cfda_federal_agency` (required, string, nullable)
+ `cfda_website` (required, string, nullable)
+ `sam_website` (required, string, nullable)
+ `cfda_obligations` (required, string, nullable)
    Description of the type of spending, the fiscal years, and the amount, as well as miscellaneous other details
+ `cfda_popular_name` (required, string, nullable)

## Agency (object)
+ `id` (required, number)
+ `has_agency_page` (required, boolean)
+ `toptier_agency` (required, TopTierAgency, nullable)
+ `subtier_agency` (required, SubTierAgency, nullable)
+ `office_agency_name` (required, string, nullable)

## ParentDetails (object)
+ `agency_id` (required, number)
+ `agency_name` (required, string)
+ `agency_slug` (required, string, nullable)
    `agency_slug` is a string used to generate a link to the agency profile page. Will be `NULL` if the agency does not have a profile page.
+ `sub_agency_id` (required, string)
+ `sub_agency_name` (required, string)
+ `award_id` (required, number, nullable)
+ `generated_unique_award_id` (required, string)
+ `idv_type_description` (required, string, nullable)
+ `multiple_or_single_aw_desc` (required, string)
+ `piid` (required, string)
+ `type_of_idc_description` (required, string, nullable)

## TopTierAgency (object)
+ `name` (required, string, nullable)
+ `code` (required, string)
+ `abbreviation` (required, string, nullable)
+ `slug` (required, string, nullable)

## SubTierAgency (object)
+ `name` (required, string, nullable)
+ `code` (required, string)
+ `abbreviation` (required, string, nullable)

## Recipient (object)
+ `recipient_name` (required, string, nullable)
+ `recipient_hash` (required, string, nullable)
+ `recipient_uei` (required, string, nullable)
+ `recipient_unique_id` (required, string, nullable)
    The recipient's DUNS
+ `parent_recipient_name` (string, nullable)
+ `parent_recipient_hash` (string, nullable)
+ `parent_recipient_uei` (required, string, nullable)
+ `parent_recipient_unique_id` (required, string, nullable)
    The recipient's parent's DUNS
+ `location` (required, Location, nullable)
    The recipient's location
+ `business_categories` (required, array[string])
    Names of the recipients' business categories in human readable format

## PeriodOfPerformance (object)
+ `start_date` (required, string)
+ `end_date` (required, string)
    Corresponds to database fields current_end_date for contracts and ordering_period_end_date for IDVs
+ `last_modified_date` (required, string)
+ `potential_end_date` (required, string, nullable)

## PeriodOfPerformanceAssistance (object)
+ `start_date` (required, string, nullable)
+ `end_date` (required, string, nullable)
+ `last_modified_date` (required, string, nullable)

## Location (object)
+ `address_line1` (required, string, nullable)
+ `address_line2` (required, string, nullable)
+ `address_line3` (required, string, nullable)
+ `foreign_province` (required, string, nullable)
+ `city_name` (required, string, nullable)
+ `county_code` (required, string, nullable)
+ `county_name` (required, string, nullable)
+ `state_code` (required, string, nullable)
+ `state_name` (required, string, nullable)
+ `zip5` (required, string, nullable)
+ `zip4` (required, string, nullable)
+ `foreign_postal_code` (required, string, nullable)
+ `country_name` (required, string, nullable)
+ `location_country_code` (required, string, nullable)
+ `congressional_code` (required, string, nullable)

## ContractDetails (object)
+ `clinger_cohen_act_planning` (required, string, nullable)
+ `clinger_cohen_act_planning_description` (required, string, nullable)
+ `commercial_item_acquisition` (required, string, nullable)
+ `commercial_item_acquisition_description` (required, string, nullable)
+ `commercial_item_test_program` (required, string, nullable)
+ `commercial_item_test_program_description` (required, string, nullable)
+ `consolidated_contract` (required, string, nullable)
+ `consolidated_contract_description` (required, string, nullable)
+ `construction_wage_rate` (required, string, nullable)
+ `construction_wage_rate_description` (required, string, nullable)
+ `cost_or_pricing_data` (required, string, nullable)
+ `cost_or_pricing_data_description` (required, string, nullable)
+ `dod_acquisition_program` (required, string, nullable)
+ `dod_acquisition_program_description` (required, string, nullable)
+ `dod_claimant_program` (required, string, nullable)
+ `dod_claimant_program_description` (required, string, nullable)
+ `domestic_or_foreign_entity` (required, string, nullable)
+ `domestic_or_foreign_entity_description` (required, string, nullable)
+ `evaluated_preference` (required, string, nullable)
+ `evaluated_preference_description` (required, string, nullable)
+ `extent_competed` (required, string, nullable)
+ `extent_competed_description` (required, string, nullable)
+ `fair_opportunity_limited` (required, string, nullable)
+ `fair_opportunity_limited_description` (required, string, nullable)
+ `fed_biz_opps` (required, string, nullable)
+ `fed_biz_opps_description` (required, string, nullable)
+ `foreign_funding` (required, string, nullable)
+ `foreign_funding_description` (required, string, nullable)
+ `idv_type_description` (required, string, nullable)
+ `information_technology_commercial_item_category` (required, string, nullable)
+ `information_technology_commercial_item_category_description` (required, string, nullable)
+ `interagency_contracting_authority` (required, string, nullable)
+ `interagency_contracting_authority_description` (required, string, nullable)
+ `labor_standards` (required, string, nullable)
+ `labor_standards_description` (required, string, nullable)
+ `major_program` (required, string, nullable)
+ `materials_supplies` (required, string, nullable)
+ `materials_supplies_description` (required, string, nullable)
+ `multi_year_contract` (required, string, nullable)
+ `multi_year_contract_description` (required, string, nullable)
+ `multiple_or_single_award_description` (required, string, nullable)
+ `naics` (required, string, nullable)
+ `naics_description` (required, string, nullable)
+ `national_interest_action` (required, string, nullable)
+ `national_interest_action_description` (required, string, nullable)
+ `number_of_offers_received` (required, string, nullable)
+ `other_than_full_and_open` (required, string, nullable)
+ `other_than_full_and_open_description` (required, string, nullable)
+ `price_evaluation_adjustment` (required, string, nullable)
+ `product_or_service_code` (required, string, nullable)
+ `product_or_service_description` (required, string, nullable)
+ `program_acronym` (required, string, nullable)
+ `purchase_card_as_payment_method` (required, string, nullable)
+ `purchase_card_as_payment_method_description` (required, string, nullable)
+ `referenced_idv_agency_iden` (required, string, nullable)
+ `referenced_idv_agency_desc` (required, string, nullable)
+ `sea_transportation` (required, string, nullable)
+ `sea_transportation_description` (required, string, nullable)
+ `small_business_competitive` (required, boolean, nullable)
+ `solicitation_identifier` (required, string, nullable)
+ `solicitation_procedures` (required, string, nullable)
+ `solicitation_procedures_description` (required, string, nullable)
+ `subcontracting_plan` (required, string, nullable)
+ `subcontracting_plan_description` (required, string, nullable)
+ `type_of_contract_pricing` (required, string, nullable)
+ `type_of_contract_pricing_description` (required, string, nullable)
+ `type_of_idc_description` (required, string, nullable)
+ `type_set_aside` (required, string, nullable)
+ `type_set_aside_description` (required, string, nullable)

## Executive (object)
+ `officers` (required, array[Officer], fixed-type)

## Officer (object)
+ `name` (required, string, nullable)
+ `amount` (required, number, nullable)

## NAICSHierarchy (object)
+ `toptier_code` (required, HierarchyLeaf, fixed-type)
+ `midtier_code` (required, HierarchyLeaf, fixed-type)
+ `base_code` (required, HierarchyLeaf, fixed-type)

## PSCHierarchy (object)
+ `toptier_code` (required, HierarchyLeaf, fixed-type)
+ `midtier_code` (required, HierarchyLeaf, fixed-type)
+ `subtier_code` (required, HierarchyLeaf, fixed-type)
+ `base_code` (required, HierarchyLeaf, fixed-type)

## HierarchyLeaf (object)
+ `description` (optional, string)
+ `code` (optional, string)

## DEFCAmount (object)
+ `code` (required, string)
+ `amount` (required, number)