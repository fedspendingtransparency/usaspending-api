## Awards
**Route:** `/api/v2/awards/<requested_award>/`

**Method** `GET`

This route sends a request to the backend to retrieve data about a specific award

## Request Parameters

- requested_award: (required) ID of award to retrieve. This can either be `generated_unique_award_id` or `id` from awards table


## Response (JSON)

If the award has a `category` of `contract`, the response will look like this:

```
{
    "category": "contract",
    "parent_award_piid": null,
    "total_obligation": 2208,
    "type_description": "DEFINITIVE CONTRACT",
    "type": "D",
    "piid": "47PB0618C0001",
    "base_and_all_options_value": 2208,
    "description": "IGF::CT::IGF  COPIER MAINTENANCE SERVICES FOR GSA OFFICE, SPRINGFIELD, MA",
    "total_subaward_amount": null,
    "base_exercised_options_val": null,
    "id": 68584146,
    "subaward_count": 0,
    "generated_unique_award_id": "CONT_AW_4740_-NONE-_47PB0618C0001_-NONE-",
    "executive_details": {
        "officers": [
            {
                "name": "Roger  Smith",
                "amount": "280000.00"
            },
            {
                "name": "Alexandre  DeSenarclens",
                "amount": "130000.00"
            },
            {
                "name": "Jerome  Pailly",
                "amount": "114000.00"
            },
            {
                "name": "Dennis  Kerling",
                "amount": "104000.00"
            },
            {
                "name": "Eric Patrick  Sandlin",
                "amount": "100000.00"
            }
        ]
    },
    "latest_transaction_contract_data": {
        "small_business_competitive": false,
        "program_system_or_equipmen": null,
        "fair_opportunity_limi_desc": null,
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "program_acronym": null,
        "product_or_service_code": "7540",
        "subcontracting_plan": "B",
        "referenced_idv_agency_iden": null,
        "commercial_item_test_desc": "NO",
        "construction_wage_rat_desc": "NOT APPLICABLE",
        "price_evaluation_adjustmen": "0.00",
        "labor_standards_descrip": "NOT APPLICABLE",
        "materials_supplies_descrip": "NOT APPLICABLE",
        "solicitation_identifier": null,
        "clinger_cohen_act_pla_desc": "NO",
        "cost_or_pricing_data_desc": "NO",
        "sea_transportation_desc": null,
        "multi_year_contract_desc": "NO",
        "fed_biz_opps_description": "NOT APPLICABLE",
        "other_than_full_and_o_desc": "ONLY ONE SOURCE-OTHER (FAR 6.302-1 OTHER)",
        "commercial_item_acquisitio": "D",
        "idv_type_description": null,
        "domestic_or_foreign_e_desc": "U.S. OWNED BUSINESS",
        "evaluated_preference_desc": "NO PREFERENCE USED",
        "type_set_aside_description": "NO SET ASIDE USED.",
        "product_or_service_co_desc": "STANDARD FORMS",
        "information_technolog_desc": null,
        "interagency_contract_desc": "NOT APPLICABLE",
        "type_of_idc_description": null,
        "dod_claimant_program_code": null,
        "major_program": null,
        "extent_competed": "C",
        "multiple_or_single_aw_desc": null,
        "solicitation_procedures": "SSS",
        "naics": "811212",
        "purchase_card_as_paym_desc": "NO",
        "foreign_funding_desc": "NOT APPLICABLE",
        "consolidated_contract_desc": "NOT CONSOLIDATED",
        "number_of_offers_received": "1"
    },
    "funding_agency": {
        "subtier_agency": {
            "name": "Public Buildings Service",
            "abbreviation": "PBS",
            "code": "4740"
        },
        "office_agency_name": null,
        "id": 636,
        "toptier_agency": {
            "name": "General Services Administration",
            "abbreviation": "GSA",
            "code": "4700"
        }
    },
    "awarding_agency": {
        "subtier_agency": {
            "name": "Public Buildings Service",
            "abbreviation": "PBS",
            "code": "4740"
        },
        "office_agency_name": null,
        "id": 636,
        "toptier_agency": {
            "name": "General Services Administration",
            "abbreviation": "GSA",
            "code": "4700"
        }
    },
    "period_of_performance": {
        "period_of_performance_start_date": "2017-10-01",
        "period_of_performance_current_end_date": "2018-09-30"
    },
    "recipient": {
        "recipient_hash": "41c9da13-f8e8-7620-9e00-8681acd9c720",
        "recipient_name": "DOCUSOURCE BUSINESS SOLUTIONS, LLC",
        "recipient_unique_id": "828026281",
        "parent_recipient_unique_id": "828026281",
        "parent_recipient_name": "DOCUSOURCE BUSINESS SOLUTIONS  LLC",
        "business_categories": [
            "us_owned_business",
            "special_designations",
            "small_business",
            "category_business"
        ],
        "location": {
            "location_country_code": "USA",
            "country_name": "UNITED STATES",
            "state_code": "CT",
            "city_name": "MIDDLETOWN",
            "county_name": "MIDDLESEX",
            "address_line1": "299 INDUSTRIAL PARK RD",
            "address_line2": null,
            "address_line3": null,
            "congressional_code": "03",
            "zip4": "1535",
            "zip5": "06457",
            "foreign_postal_code": null,
            "foreign_province": null
        }
    },
    "place_of_performance": {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "county_name": "HAMPDEN",
        "city_name": "SPRINGFIELD",
        "state_code": "MA",
        "congressional_code": "01",
        "zip4": "011031422",
        "zip5": "01103",
        "address_line1": null,
        "address_line2": null,
        "address_line3": null,
        "foreign_province": null,
        "foreign_postal_code": null
    }
}
```

If the `category` is `idv`, the response will look like this:

```
{
    "category": "idv",
    "parent_award_piid": null,
    "total_obligation": 0,
    "type_description": "BPA",
    "type": "IDV_E",
    "piid": "190PRL18A0613",
    "base_and_all_options_value": 0,
    "description": "PROVIDING PROFESSIONAL FOREIGN LANGUAGE INTERPRETING AND TRANSLATING SERVICES",
    "total_subaward_amount": null,
    "base_exercised_options_val": null,
    "id": 65284276,
    "subaward_count": 0,
    "generated_unique_award_id": "CONT_AW_1900_-NONE-_190PRL18A0613_-NONE-",
    "parent_generated_unique_award_id": null,
    "executive_details": {
        "officers": []
    },
    "latest_transaction_contract_data": {
        "small_business_competitive": false,
        "program_system_or_equipmen": null,
        "fair_opportunity_limi_desc": null,
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "program_acronym": null,
        "product_or_service_code": "R608",
        "subcontracting_plan": null,
        "referenced_idv_agency_iden": null,
        "commercial_item_test_desc": null,
        "construction_wage_rat_desc": "NOT APPLICABLE",
        "price_evaluation_adjustmen": null,
        "labor_standards_descrip": "NOT APPLICABLE",
        "materials_supplies_descrip": "NOT APPLICABLE",
        "solicitation_identifier": null,
        "clinger_cohen_act_pla_desc": "NO",
        "cost_or_pricing_data_desc": null,
        "sea_transportation_desc": null,
        "multi_year_contract_desc": null,
        "fed_biz_opps_description": "NOT APPLICABLE",
        "other_than_full_and_o_desc": null,
        "commercial_item_acquisitio": "A",
        "idv_type_description": "BPA",
        "domestic_or_foreign_e_desc": "U.S. OWNED BUSINESS",
        "evaluated_preference_desc": null,
        "type_set_aside_description": null,
        "product_or_service_co_desc": "SUPPORT- ADMINISTRATIVE: TRANSLATION AND INTERPRETING",
        "information_technolog_desc": null,
        "interagency_contract_desc": "OTHER STATUTORY AUTHORITY",
        "type_of_idc_description": null,
        "dod_claimant_program_code": null,
        "major_program": null,
        "extent_competed": null,
        "multiple_or_single_aw_desc": "MULTIPLE AWARD",
        "solicitation_procedures": null,
        "naics": "541930",
        "purchase_card_as_paym_desc": null,
        "foreign_funding_desc": "NOT APPLICABLE",
        "consolidated_contract_desc": "NOT CONSOLIDATED",
        "number_of_offers_received": null
    },
    "funding_agency": {
        "subtier_agency": {
            "name": "Department of State",
            "abbreviation": "DOS",
            "code": "1900"
        },
        "office_agency_name": null,
        "id": 315,
        "toptier_agency": {
            "name": "Department of State",
            "abbreviation": "DOS",
            "code": "1900"
        }
    },
    "awarding_agency": {
        "subtier_agency": {
            "name": "Department of State",
            "abbreviation": "DOS",
            "code": "1900"
        },
        "office_agency_name": null,
        "id": 315,
        "toptier_agency": {
            "name": "Department of State",
            "abbreviation": "DOS",
            "code": "1900"
        }
    },
    "idv_dates": {
        "start_date": "2017-10-01",
        "last_modified_date": "2018-04-04 15:21:28",
        "end_date": "2018-09-30"
    },
    "recipient": {
        "recipient_hash": "d818c3f3-b5a2-add8-8365-79c1213e2385",
        "recipient_name": "DUCELUS, ROOSEVELT",
        "recipient_unique_id": "962788902",
        "parent_recipient_unique_id": "962788902",
        "parent_recipient_name": "DUCELUS  ROOSEVELT",
        "business_categories": [
            "other_than_small_business",
            "sole_proprietorship",
            "category_business",
            "black_american_owned_business",
            "minority_owned_business",
            "us_owned_business",
            "special_designations"
        ],
        "location": {
            "location_country_code": "USA",
            "country_name": "UNITED STATES",
            "state_code": "GA",
            "city_name": "BETHLEHEM",
            "county_name": "BARROW",
            "address_line1": "1900 NATALIE DR",
            "address_line2": null,
            "address_line3": null,
            "congressional_code": "10",
            "zip4": "2570",
            "zip5": "30620",
            "foreign_postal_code": null,
            "foreign_province": null
        }
    },
    "place_of_performance": {
        "location_country_code": null,
        "country_name": null,
        "county_name": null,
        "city_name": null,
        "state_code": null,
        "congressional_code": null,
        "zip4": null,
        "zip5": null,
        "address_line1": null,
        "address_line2": null,
        "address_line3": null,
        "foreign_province": null,
        "foreign_postal_code": null
    }
}
```

If the `category` of the award is any assistance type, the response will instead look like this:

```
{
    "non_federal_funding": null,
    "awarding_agency": {
        "subtier_agency": {
            "name": "Under Secretary for Health/Veterans Health Administration",
            "abbreviation": "",
            "code": "3620"
        },
        "office_agency_name": null,
        "id": 580,
        "toptier_agency": {
            "name": "Department of Veterans Affairs",
            "abbreviation": "VA",
            "code": "3600"
        }
    },
    "category": "other",
    "total_funding": 133004,
    "base_and_all_options_value": null,
    "description": "VHA MEDICAL PROCESS",
    "base_exercised_options": null,
    "total_subaward_amount": null,
    "recipient": {
        "recipient_hash": "005727d6-ca1c-fc53-add0-3b03fb8b6933",
        "recipient_name": "MULTIPLE RECIPIENTS",
        "recipient_unique_id": null,
        "parent_recipient_unique_id": null,
        "parent_recipient_name": null,
        "business_categories": [],
        "location": {
            "location_country_code": "USA",
            "country_name": "UNITED STATES",
            "state_code": "PA",
            "city_name": null,
            "county_name": "NORTHUMBERLAND",
            "address_line1": null,
            "address_line2": null,
            "address_line3": null,
            "congressional_code": "90",
            "zip4": null,
            "zip5": null,
            "foreign_postal_code": null,
            "foreign_province": null
        }
    },
    "id": 68463998,
    "generated_unique_award_id": "ASST_AW_3620_-NONE-_18090913.0121844",
    "total_subsidy_cost": null,
    "uri": "18090913.0121844",
    "total_loan_value": null,
    "total_obligation": 133004,
    "type_description": "OTHER REIMBURSABLE, CONTINGENT, INTANGIBLE, OR INDIRECT FINANCIAL ASSISTANCE",
    "type": "11",
    "funding_agency": {
        "subtier_agency": {
            "name": "Department of Veterans Affairs",
            "abbreviation": "VA",
            "code": "3600"
        },
        "office_agency_name": null,
        "id": 561,
        "toptier_agency": {
            "name": "Department of Veterans Affairs",
            "abbreviation": "VA",
            "code": "3600"
        }
    },
    "fain": null,
    "subaward_count": 0,
    "cfda_number": "64.012",
    "cfda_title": "VETERANS PRESCRIPTION SERVICE",
    "cfda_objectives": "To provide eligible veterans and certain dependents and survivors of veterans with prescription drugs and expendable medical supplies from VA pharmacies upon presentation of prescription(s) from a VA provider or VA authorized provider.",
    "period_of_performance": {
        "period_of_performance_start_date": null,
        "period_of_performance_current_end_date": null
    },
    "place_of_performance": {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "county_name": "NORTHUMBERLAND",
        "city_name": null,
        "state_code": "PA",
        "congressional_code": "90",
        "zip4": null,
        "zip5": null,
        "address_line1": null,
        "address_line2": null,
        "address_line3": null,
        "foreign_province": null,
        "foreign_postal_code": null
    }
}
```

