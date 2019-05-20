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
  "awarding_agency": {
    "id": 1219,
    "office_agency_name": null,
    "subtier_agency": {
      "abbreviation": "DLA",
      "code": "97AS",
      "name": "Defense Logistics Agency"
    },
    "toptier_agency": {
      "abbreviation": "DOD",
      "code": "9700",
      "name": "Department of Defense"
    }
  },
  "base_and_all_options": 18014.04,
  "base_exercised_options": 18014.04,
  "category": "contract",
  "date_signed": "2018-06-21",
  "description": "8505611174!SHIM KIT,WINDSHIELD",
  "executive_details": {
    "officers": []
  },
  "funding_agency": {
    "id": 1219,
    "office_agency_name": null,
    "subtier_agency": {
      "abbreviation": "DLA",
      "code": "97AS",
      "name": "Defense Logistics Agency"
    },
    "toptier_agency": {
      "abbreviation": "DOD",
      "code": "9700",
      "name": "Department of Defense"
    }
  },
  "generated_unique_award_id": "CONT_AW_9700_9700_SPE4A718F9115_SPRPA117GC101",
  "id": 68584146,
  "latest_transaction_contract_data": {
    "clinger_cohen_act_planning": "N",
    "clinger_cohen_act_planning_description": "NO",
    "commercial_item_acquisition": "D",
    "commercial_item_acquisition_description": "COMMERCIAL ITEM PROCEDURES NOT USED",
    "commercial_item_test_program": null,
    "commercial_item_test_program_description": null,
    "consolidated_contract": "D",
    "consolidated_contract_description": "NOT CONSOLIDATED",
    "construction_wage_rate": "X",
    "construction_wage_rate_description": "NOT APPLICABLE",
    "cost_or_pricing_data": "N",
    "cost_or_pricing_data_description": "NO",
    "dod_acquisition_program": "000",
    "dod_acquisition_program_description": "NONE",
    "dod_claimant_program": "A1C",
    "dod_claimant_program_description": "OTHER AIRCRAFT EQUIPMENT ",
    "domestic_or_foreign_entity": "A",
    "domestic_or_foreign_entity_description": "U.S. OWNED BUSINESS",
    "evaluated_preference": "NONE",
    "evaluated_preference_description": "NO PREFERENCE USED",
    "extent_competed": "G",
    "extent_competed_description": "NOT COMPETED UNDER SAP",
    "fair_opportunity_limited": null,
    "fair_opportunity_limited_description": null,
    "fed_biz_opps": "N",
    "fed_biz_opps_description": "NO",
    "foreign_funding": "X",
    "foreign_funding_description": "NOT APPLICABLE",
    "idv_type_description": null,
    "information_technology_commercial_item_category": "Z",
    "information_technology_commercial_item_category_description": "NOT IT PRODUCTS OR SERVICES",
    "interagency_contracting_authority": "X",
    "interagency_contracting_authority_description": "NOT APPLICABLE",
    "labor_standards": "X",
    "labor_standards_description": "NOT APPLICABLE",
    "major_program": null,
    "materials_supplies": "Y",
    "materials_supplies_description": "YES",
    "multi_year_contract": null,
    "multi_year_contract_description": null,
    "multiple_or_single_award_description": null,
    "naics": "336413",
    "naics_description": "OTHER AIRCRAFT PARTS AND AUXILIARY EQUIPMENT MANUFACTURING",
    "number_of_offers_received": "1",
    "other_than_full_and_open": "SP2",
    "other_than_full_and_open_description": "SAP NON-COMPETITION (FAR 13)",
    "price_evaluation_adjustment": null,
    "product_or_service_code": "1560",
    "product_or_service_description": "AIRFRAME STRUCTURAL COMPONENTS",
    "program_acronym": null,
    "purchase_card_as_payment_method": "N",
    "purchase_card_as_payment_method_description": "NO",
    "referenced_idv_agency_iden": "9700",
    "sea_transportation": "U",
    "sea_transportation_description": "UNKNOWN",
    "small_business_competitive": false,
    "solicitation_identifier": "SPE4A717U0350",
    "solicitation_procedures": "SP1",
    "solicitation_procedures_description": "SIMPLIFIED ACQUISITION",
    "subcontracting_plan": "B",
    "subcontracting_plan_description": "PLAN NOT REQUIRED ",
    "type_of_contract_pricing": "J",
    "type_of_contract_pricing_description": "FIRM FIXED PRICE",
    "type_of_idc_description": null,
    "type_set_aside": "NONE",
    "type_set_aside_description": "NO SET ASIDE USED."
  },
  "parent_award_piid": "SPRPA117GC101",
  "period_of_performance": {
    "end_date": "2019-04-03",
    "last_modified_date": "2018-07-02 19:35:09",
    "potential_end_date": "2019-04-03 00:00:00",
    "start_date": "2018-06-21"
  },
  "piid": "SPE4A718F9115",
  "place_of_performance": {
    "address_line1": null,
    "address_line2": null,
    "address_line3": null,
    "city_name": "STRATFORD",
    "congressional_code": "03",
    "country_name": "UNITED STATES",
    "county_name": "FAIRFIELD",
    "foreign_postal_code": null,
    "foreign_province": null,
    "location_country_code": "USA",
    "state_code": "CT",
    "zip4": "1378",
    "zip5": "06614"
  },
  "recipient": {
    "business_categories": [
      "special_designations",
      "us_owned_business",
      "corporate_entity_not_tax_exempt",
      "other_than_small_business",
      "category_business"
    ],
    "location": {
      "address_line1": "6900 MAIN ST",
      "address_line2": null,
      "address_line3": null,
      "city_name": "STRATFORD",
      "congressional_code": "03",
      "country_name": "UNITED STATES",
      "county_name": "FAIRFIELD",
      "foreign_postal_code": null,
      "foreign_province": null,
      "location_country_code": "USA",
      "state_code": "CT",
      "zip4": "1378",
      "zip5": "06614"
    },
    "parent_recipient_name": "LOCKHEED MARTIN CORPORATION",
    "parent_recipient_unique_id": "834951691",
    "recipient_hash": "dc26e2ac-22d7-95fa-4ac3-1e88adbf1207-C",
    "recipient_name": "SIKORSKY AIRCRAFT CORPORATION",
    "recipient_unique_id": "835551474"
  },
  "subaward_count": 0,
  "total_obligation": 18014.04,
  "total_subaward_amount": null,
  "type": "C",
  "type_description": "DELIVERY ORDER"
}
```

If the `category` is `idv`, the response will look like this:

```
{
  "awarding_agency": {
    "id": 315,
    "office_agency_name": null,
    "subtier_agency": {
      "abbreviation": "DOS",
      "code": "1900",
      "name": "Department of State"
    },
    "toptier_agency": {
      "abbreviation": "DOS",
      "code": "1900",
      "name": "Department of State"
    }
  },
  "base_and_all_options": 0,
  "base_exercised_options": null,
  "category": "idv",
  "date_signed": "2017-10-01",
  "description": "PROVIDING PROFESSIONAL FOREIGN LANGUAGE INTERPRETING AND TRANSLATING SERVICES",
  "executive_details": {
    "officers": []
  },
  "funding_agency": {
    "id": 315,
    "office_agency_name": null,
    "subtier_agency": {
      "abbreviation": "DOS",
      "code": "1900",
      "name": "Department of State"
    },
    "toptier_agency": {
      "abbreviation": "DOS",
      "code": "1900",
      "name": "Department of State"
    }
  },
  "generated_unique_award_id": "CONT_AW_1900_-NONE-_190PRL18A0613_-NONE-",
  "id": 68943678,
  "latest_transaction_contract_data": {
    "clinger_cohen_act_planning": "N",
    "clinger_cohen_act_planning_description": "NO",
    "commercial_item_acquisition": "A",
    "commercial_item_acquisition_description": "COMMERCIAL ITEM",
    "commercial_item_test_program": null,
    "commercial_item_test_program_description": null,
    "consolidated_contract": "D",
    "consolidated_contract_description": "NOT CONSOLIDATED",
    "construction_wage_rate": "X",
    "construction_wage_rate_description": "NOT APPLICABLE",
    "cost_or_pricing_data": null,
    "cost_or_pricing_data_description": null,
    "dod_acquisition_program": null,
    "dod_acquisition_program_description": null,
    "dod_claimant_program": null,
    "dod_claimant_program_description": null,
    "domestic_or_foreign_entity": "A",
    "domestic_or_foreign_entity_description": "U.S. OWNED BUSINESS",
    "evaluated_preference": null,
    "evaluated_preference_description": null,
    "extent_competed": null,
    "extent_competed_description": null,
    "fair_opportunity_limited": null,
    "fair_opportunity_limited_description": null,
    "fed_biz_opps": "X",
    "fed_biz_opps_description": "NOT APPLICABLE",
    "foreign_funding": "X",
    "foreign_funding_description": "NOT APPLICABLE",
    "idv_type_description": "BPA",
    "information_technology_commercial_item_category": null,
    "information_technology_commercial_item_category_description": null,
    "interagency_contracting_authority": "B",
    "interagency_contracting_authority_description": "OTHER STATUTORY AUTHORITY",
    "labor_standards": "X",
    "labor_standards_description": "NOT APPLICABLE",
    "major_program": null,
    "materials_supplies": "X",
    "materials_supplies_description": "NOT APPLICABLE",
    "multi_year_contract": null,
    "multi_year_contract_description": null,
    "multiple_or_single_award_description": "MULTIPLE AWARD",
    "naics": "541930",
    "naics_description": "TRANSLATION AND INTERPRETATION SERVICES",
    "number_of_offers_received": null,
    "other_than_full_and_open": null,
    "other_than_full_and_open_description": null,
    "price_evaluation_adjustment": null,
    "product_or_service_code": "R608",
    "product_or_service_description": "SUPPORT- ADMINISTRATIVE: TRANSLATION AND INTERPRETING",
    "program_acronym": null,
    "purchase_card_as_payment_method": null,
    "purchase_card_as_payment_method_description": null,
    "referenced_idv_agency_iden": null,
    "sea_transportation": null,
    "sea_transportation_description": null,
    "small_business_competitive": false,
    "solicitation_identifier": null,
    "solicitation_procedures": null,
    "solicitation_procedures_description": null,
    "subcontracting_plan": null,
    "subcontracting_plan_description": null,
    "type_of_contract_pricing": "J",
    "type_of_contract_pricing_description": "FIRM FIXED PRICE",
    "type_of_idc_description": null,
    "type_set_aside": null,
    "type_set_aside_description": null
  },
  "parent_award": null,
  "parent_award_piid": null,
  "parent_generated_unique_award_id": null,
  "period_of_performance": {
    "end_date": "2018-09-30",
    "last_modified_date": "2018-04-04 15:21:28",
    "potential_end_date": null,
    "start_date": "2017-10-01"
  },
  "piid": "190PRL18A0613",
  "place_of_performance": {
    "address_line1": null,
    "address_line2": null,
    "address_line3": null,
    "city_name": null,
    "congressional_code": null,
    "country_name": null,
    "county_name": null,
    "foreign_postal_code": null,
    "foreign_province": null,
    "location_country_code": null,
    "state_code": null,
    "zip4": null,
    "zip5": null
  },
  "recipient": {
    "business_categories": [
      "special_designations",
      "other_than_small_business",
      "minority_owned_business",
      "us_owned_business",
      "sole_proprietorship",
      "black_american_owned_business",
      "category_business"
    ],
    "location": {
      "address_line1": "1900 NATALIE DR",
      "address_line2": null,
      "address_line3": null,
      "city_name": "BETHLEHEM",
      "congressional_code": "10",
      "country_name": "UNITED STATES",
      "county_name": "BARROW",
      "foreign_postal_code": null,
      "foreign_province": null,
      "location_country_code": "USA",
      "state_code": "GA",
      "zip4": "2570",
      "zip5": "30620"
    },
    "parent_recipient_name": "DUCELUS  ROOSEVELT",
    "parent_recipient_unique_id": "962788902",
    "recipient_hash": "d818c3f3-b5a2-add8-8365-79c1213e2385-C",
    "recipient_name": "DUCELUS, ROOSEVELT",
    "recipient_unique_id": "962788902"
  },
  "subaward_count": 0,
  "total_obligation": 0,
  "total_subaward_amount": null,
  "type": "IDV_E",
  "type_description": "BPA"
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

