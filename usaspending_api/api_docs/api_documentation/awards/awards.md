## Awards
**Route:** `/api/v2/awards/<generated_unique_award_id>/`

**Method** `GET`

This route sends a request to the backend to retrieve data about a specific award

## Request Parameters

- generated_unique_award_id: (required) ID of award to retrieve


## Response (JSON)

If the award has a `category` of `contract`, the response will look like this:

```
{
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
    "period_of_performance": {
        "period_of_performance_start_date": "2015-12-29",
        "period_of_performance_current_end_date": "2016-01-14"
    },
    "latest_transaction_contract_data": {
        "type_of_contract_pric_desc": "FIRM FIXED PRICE",
        "naics": "311812",
        "naics_description": "COMMERCIAL BAKERIES",
        "referenced_idv_agency_iden": "9700",
        "idv_type_description": null,
        "multiple_or_single_aw_desc": null,
        "type_of_idc_description": null,
        "dod_claimant_program_code": "B2",
        "clinger_cohen_act_pla_desc": "NO",
        "commercial_item_acquisitio": "A",
        "commercial_item_test_desc": "NO",
        "consolidated_contract_desc": "NO",
        "cost_or_pricing_data_desc": "NO",
        "construction_wage_rat_desc": "NO",
        "evaluated_preference_desc": "NO PREFERENCE USED",
        "extent_competed": "A",
        "fed_biz_opps_description": "NO",
        "foreign_funding_desc": "NOT APPLICABLE",
        "information_technolog_desc": "NOT IT PRODUCTS OR SERVICES",
        "interagency_contract_desc": "NOT APPLICABLE",
        "major_program": null,
        "purchase_card_as_paym_desc": "NO",
        "multi_year_contract_desc": "NO",
        "number_of_offers_received": "4",
        "price_evaluation_adjustmen": null,
        "product_or_service_code": "8940",
        "program_acronym": null,
        "other_than_full_and_o_desc": null,
        "sea_transportation_desc": "UNKNOWN",
        "labor_standards_descrip": "NO",
        "small_business_competitive": "False",
        "solicitation_identifier": null,
        "solicitation_procedures": "NP",
        "fair_opportunity_limi_desc": null,
        "subcontracting_plan": null,
        "program_system_or_equipmen": "000",
        "type_set_aside_description": "NO SET ASIDE USED.",
        "materials_supplies_descrip": "NO",
        "domestic_or_foreign_e_desc": "FOREIGN-OWNED BUSINESS NOT INCORPORATED IN THE U.S."
    },
    "type": "C",
    "type_description": "DELIVERY ORDER",
    "category": "contract",
    "piid": "03VD",
    "total_obligation": "932.10",
    "description": "4532842749!EGGS, SHELL, FRESH, MED,",
    "base_and_all_options_value": "932.10",
    "parent_award_piid": "SPM30012D3486",
    "total_subaward_amount": "0.00",
    "subaward_count": 0,
    "awarding_agency": {
        "office_agency_name": null,
        "toptier_agency": {
            "abbreviation": "DOD",
            "name": "Department of Defense"
        },
        "subtier_agency": {
            "abbreviation": "DLA",
            "name": "Defense Logistics Agency"
        }
    },
    "funding_agency": {
        "office_agency_name": null,
        "toptier_agency": {
            "abbreviation": "DOD",
            "name": "Department of Defense"
        },
        "subtier_agency": {
            "abbreviation": "DLA",
            "name": "Defense Logistics Agency"
        }
    },
    "recipient": {
        "recipient_parent_name": null,
        "parent_recipient_unique_id": "520233955",
        "recipient_name": "EBREX FOOD SERVICES SARL",
        "business_categories": [
            "other_than_small_business",
            "sole_proprietorship",
            "category_business",
            "foreign_owned_and_located_business",
            "special_designations"
        ],
        "recipient_unique_id": "480520290",
        "location": {
            "location_country_code": "CHE",
            "country_name": "SWITZERLAND",
            "state_code": null,
            "city_name": "GENEVE",
            "county_name": null,
            "address_line1": "RUE MAUNOIR 16",
            "address_line2": null,
            "address_line3": null,
            "zip4": "1207",
            "congressional_code": null,
            "zip5": null,
            "foreign_postal_code": null,
            "foreign_province": null
        }
    },
    "place_of_performance": {
        "location_country_code": "CHE",
        "country_name": "SWITZERLAND",
        "state_code": null,
        "city_name": null,
        "county_name": null,
        "address_line1": null,
        "address_line2": null,
        "address_line3": null,
        "zip4": null,
        "congressional_code": null,
        "zip5": null,
        "foreign_postal_code": null,
        "foreign_province": null
    }
}

```

If the `category` of the award is anything other than `contract`, the response will instead look like this:

```

{
    "period_of_performance": {
        "period_of_performance_start_date": "2009-09-21",
        "period_of_performance_current_end_date": "2021-07-31"
    },
    "executive_details": {
        "officers": [
            {
                "name": "Jeannie K Peters",
                "amount": "145000.00"
            },
            {
                "name": "James  Statler",
                "amount": "139639.00"
            },
            {
                "name": "Amy  Smith",
                "amount": "127991.00"
            },
            {
                "name": "Lisa  Beran",
                "amount": "127991.00"
            },
            {
                "name": "Kevin  Field",
                "amount": "120746.00"
            }
        ]
    },
    "cfda_objectives": "To provide rental assistance to very low income individuals and families enabling them to live in affordable decent, safe and sanitary housing.",
    "cfda_title": "SECTION 8 HOUSING ASSISTANCE PAYMENTS PROGRAM",
    "cfda_number": "14.195",
    "type": "06",
    "type_description": "DIRECT PAYMENT FOR SPECIFIED USE",
    "category": "direct payment",
    "piid": null,
    "description": "CONT RENEWALS ALL TYPES",
    "total_subaward_amount": "0.00",
    "subaward_count": 0,
    "awarding_agency": {
        "office_agency_name": null,
        "toptier_agency": {
            "abbreviation": "HUD",
            "name": "Department of Housing and Urban Development"
        },
        "subtier_agency": {
            "abbreviation": "",
            "name": "Assistant Secretary for Housing--Federal Housing Commissioner"
        }
    },
    "funding_agency": null,
    "recipient": {
        "recipient_parent_name": null,
        "parent_recipient_unique_id": null,
        "recipient_name": "KENTUCKY HOUSING CORPORATION",
        "business_categories": [
            "other_than_small_business",
            "category_business"
        ],
        "recipient_unique_id": "082316696",
        "location": {
            "location_country_code": "USA",
            "country_name": "UNITED STATES",
            "state_code": "KY",
            "city_name": "FRANKFORT",
            "county_name": "FRANKLIN",
            "address_line1": "1231 LOUISVILLE ROAD",
            "address_line2": null,
            "address_line3": null,
            "zip4": null,
            "congressional_code": "06",
            "zip5": "40601",
            "foreign_postal_code": null,
            "foreign_province": null
        }
    },
    "place_of_performance": {
        "location_country_code": "USA",
        "country_name": "UNITED STATES",
        "state_code": "KY",
        "city_name": "FRANKFORT",
        "county_name": "FRANKLIN",
        "address_line1": null,
        "address_line2": null,
        "address_line3": null,
        "zip4": null,
        "congressional_code": "06",
        "zip5": "40601",
        "foreign_postal_code": null,
        "foreign_province": null
    }
}



