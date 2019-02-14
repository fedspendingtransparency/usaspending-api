FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Profile

These endpoints are used to power USAspending.gov's award profile pages. This data can be used to view details about a specific award.

# Group Award Page

These endpoints support the individual Award pages that display data for a specific award type.

## Awards [/api/v2/awards/{award_id}/]

This endpoint returns a list of data that is associated with the award profile page.

+ Parameters
    + award_id: `TEST` (required, string)
        Accepts the v2 generated award hash or internal database id.

### Award [GET]

+ Request A request with a contract id (application/json)
    + Parameters
        + `award_id`: `CONT_AW_9700_-NONE-_SP045003WG347_-NONE-`

+ Response 200 (application/json)
    + Attributes (ContractResponse)

+ Request A request with a financial assistance id (application/json)
    + Parameters
        + `award_id`: `ASST_AW_1665_0000000000001828_-NONE-`

+ Response 200 (application/json)
    + Attributes (FinancialAssistanceResponse)

+ Request A request with an IDV id (application/json)
    + Parameters
        + `award_id`: `CONT_AW_9700_-NONE-_N0018918D0057_-NONE-`

+ Response 200 (application/json)
    + Attributes (IDVResponse)

# Data Structures

## ContractResponse (object)
+ category: `contract` (required, enum[string])
    + Members
        + contract
+ type: `D` (required, enum[string])
    + Members
        + A
        + B
        + C
        + D
+ type_description: `Definitive Contracts` (required, string)
+ generated_unique_award_id: `25764264` (required, string)
+ piid: `W31P4Q15A0024` (required, string)
    Award id
+ parent_award_piid: `1301` (required, string, nullable)
    null if the contract has no parent
+ description: `ewraijwrw` (required, string, nullable)
    Description of the first transaction for this award
+ total_obligation: 30400 (required, number)
+ base_exercised_options: 23000 (required, number, nullable)
+ base_and_all_options: 24500 (required, number)
    The ceiling and amount of the award
+ date_signed: `2006-01-15` (required, string, nullable)
+ subaward_count: 430 (required, number)
+ total_subaward_amount: 35345353453 (required, number, nullable)
+ awarding_agency (required, Agency, fixed-type)
+ funding_agency (required, Agency, fixed-type, nullable)
+ recipient (required, Recipient, fixed-type)
+ period_of_performance (required, PeriodOfPerformance, fixed-type)
+ place_of_performance (required, Location, fixed-type)
+ latest_transaction_contract_data (required, ContractDetails, fixed-type)
+ executive_details (required, Executive, fixed-type)

## IDVResponse (object)
+ category: `idv` (required, enum[string])
    + Members
        + idv
+ type: `IDV_E` (required, enum[string])
    + Members
        + `IDV_A`
        + `IDV_B`
        + `IDV_B_A`
        + `IDV_B_B`
        + `IDV_B_C`
        + `IDV_C`
        + `IDV_D`
        + `IDV_E`
+ type_description: `Blanket Purchase Agreements (BPA) Calls` (required, string)
+ generated_unique_award_id: `6657452ew23` (required, string)
+ piid: `W31P4Q15A0024` (required, string)
    Award id
+ parent_award_piid: `1301` (required, string, nullable)
    null if the IDV has no parent
+ parent_generated_unique_award_id: `CONT_AW_9700_4730_W56HZV10AA913_GS10F0243K` (required, nullable)
    null if the IDV has no parent
+ description: `ewraijwrw` (required, string, nullable)
    Description of the first transaction for this award
+ total_obligation: 30400 (required, number, nullable)
+ base_exercised_options: 23000 (required, number, nullable)
+ base_and_all_options: 24500 (required, number)
    The ceiling and amount of the award
+ date_signed: `2006-01-15` (required, string, nullable)
+ subaward_count: 430 (required, number)
+ total_subaward_amount: 35345353453 (required, number, nullable)
+ awarding_agency (required, Agency, fixed-type)
+ funding_agency (required, Agency, fixed-type, nullable)
+ recipient (required, Recipient, fixed-type)
+ period_of_performance (required, PeriodOfPerformance, fixed-type)
+ place_of_performance (required, Location, fixed-type)
+ latest_transaction_contract_data (required, ContractDetails, fixed-type)
+ executive_details (required, Executive, fixed-type)
+ parent_award (required, ParentIDVDetails, fixed-type, nullable)

## FinancialAssistanceResponse (object)
+ category: `loans` (required, enum[string])
    + Members
        + loans
        + other
        + direct payment
        + grant
+ type: `07` (required, enum[string])
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
+ type_description: `Direct Loans` (required, string)
+ generated_unique_award_id: `42954959` (required, string)
+ fain: `43533A3` (required, string, nullable)
+ uri: `5341QQ` (required, string, nullable)
+ description: `ewraijwrw` (required, string, nullable)
+ transaction_obligated_amount: 10700 (required, number, nullable)
    Used for Funding Obligated on Grants pages
+ total_obligation: 2324 (required, number, nullable)
+ base_exercised_options (required, number, nullable)
+ total_subsidy_cost: 123 (required, number, nullable)
    null except for loans
+ total_loan_value: 24343 (required, number, nullable)
    null except for loans
+ non_federal_funding (required, number, nullable)
    null except for grants
+ total_funding (required, number, nullable)
    null except for grants
+ date_signed: `2006-01-15` (required, string, nullable)
+ subaward_count: 430 (required, number)
+ total_subaward_amount: 35345353453 (required, number, nullable)
+ cfda_objectives: `Some HTML string` (required, string, nullable)
+ cfda_number: `0.434` (required, string, nullable)
+ cfda_title: `Flood Insurance` (required, string, nullable)
+ awarding_agency (required, Agency, fixed-type)
+ funding_agency (required, Agency, fixed-type, nullable)
+ recipient (required, Recipient, fixed-type)
+ period_of_performance (required, PeriodOfPerformanceAssistance, fixed-type)
+ place_of_performance (required, Location, fixed-type)

## Agency (object)
+ id: 123 (required, number)
+ toptier_agency (required, TopTierAgency, nullable)
+ subtier_agency (required, SubTierAgency, nullable)
+ office_agency_name: `STRATEGIC SYSTEMS` (required, string, nullable)

## ParentIDVDetails (object)
+ award_id: 5738 (required, number, nullable)
+ idv_type_description: `test` (required, string, nullable)
+ type_of_idc_description: `r3w` (required, string, nullable)
+ agency_id: `123` (required, string)
+ multiple_or_single_aw_desc: `something` (required, string)
+ piid: `345` (required, string)

## TopTierAgency (object)
+ name: `Department of Defense` (required, string, nullable)
+ abbreviation: `DOD` (required, string, nullable)

## SubTierAgency (object)
+ name: `Department of Navy` (required, string, nullable)
+ abbreviation: `DON` (required, string, nullable)

## Recipient (object)
+ recipient_name: `Booz Allen Hamilton` (required, string, nullable)
+ recipient_hash: `e4096343-5e8f-352a-f8af-d2a8b0f0ae68-C` (required, string)
+ recipient_unique_id: `2424224` (required, string, nullable)
    The recipient's DUNS
+ parent_recipient_unique_id: `2424232` (required, string, nullable)
    The recipient's parent's DUNS
+ location (required, Location, nullable)
    The recipeint's location
+ parent_recipient_name: `HoneyWell` (string, nullable)
+ business_categories (required, array[string])
    Names of the recipients' business catagories in human readable format

##PeriodOfPerformance
+ start_date: `2004-02-19` (required, string)
+ end_date: `2005-02-19` (required, string)
    Corresponds to database fields current_end_date for contracts and ordering_period_end_date for IDVs
+ last_modified_date: `2301-02-20` (required, string)
+ potential_end_date: `2301-02-23` (required, string, nullable)

##PeriodOfPerformanceAssistance
+ start_date: `2004-02-19` (required, string, nullable)
+ end_date: `2005-02-19` (required, string, nullable)
+ last_modified_date: `2301-02-20` (required, string, nullable)

## Location
+ address_line1: `123 Sesame St` (required, string, nullable)
+ address_line2 (required, string, nullable)
+ address_line3 (required, string, nullable)
+ foreign_province (required, string, nullable)
+ city_name: `McLean` (required, string, nullable)
+ county_name: `Fairfax` (required, string, nullable)
+ state_code: `VA` (required, string, nullable)
+ zip5: `22102` (required, string, nullable)
+ zip4 (required, string, nullable)
+ foreign_postal_code (required, string, nullable)
+ country_name (required, string, nullable)
+ location_country_code: `TW` (required, string, nullable)
+ congressional_code: `05` (required, string, nullable)

## ContractDetails
+ idv_type_description: `test` (required, string, nullable)
+ type_of_idc_description: `r3w` (required, string, nullable)
+ referenced_idv_agency_iden: `424` (required, string, nullable)
+ multiple_or_single_aw_desc: `testing` (required, string, nullable)
+ solicitation_identifier: `DE-2342-323-SOL` (required, string, nullable)
+ solicitation_procedures: `Quote` (required, string, nullable)
+ number_of_offers_received: `4` (required, string, nullable)
+ extent_competed: `Full` (required, string, nullable)
+ other_than_full_and_o_desc: `none` (required, string, nullable)
+ type_set_aside_description: `No set aside used` (required, string, nullable)
+ commercial_item_acquisitio: `COMMERCIAL ITEM` (required, string, nullable)
+ commercial_item_test_desc: `PROCEDURE NOT USED` (required, string, nullable)
+ evaluated_preference_desc: `NO` (required, string, nullable)
+ fed_biz_opps_description: `No Preference Used` (required, string, nullable)
+ small_business_competitive: true (required, boolean)
+ fair_opportunity_limi_desc: `test` (optional, string, nullable)
+ product_or_service_code: `t324242` (required, string, nullable)
+ product_or_service_desc: `423we` (required, string, nullable)
+ naics: `35353` (required, string, nullable)
+ naics_description: `323t2` (required, string, nullable)
+ dod_claimant_program_code: `ERWRWRWR5242-242` (required, string, nullable)
+ dod_acquisition_program_code: `dod123` (required, string, nullable)
+ dod_acquisition_program_description: `dod acquisition` (required, string, nullable)
+ information_technology_commercial_item_category_code: `ERWRWRWR5242-242` (required, string, nullable)
+ information_technology_commercial_item_category: `commercial item` (string, nullable)
+ sea_transportation_desc: `seaworld` (string, nullable)
+ clinger_cohen_act_pla_desc: `NO` (required, string, nullable)
+ construction_wage_rat_desc: `YES` (required, string, nullable)
+ labor_standards_descrip: `NO` (required, string, nullable)
+ materials_supplies_descrip: `YES` (required, string, nullable)
+ cost_or_pricing_data_desc: `No` (required, string, nullable)
+ domestic_or_foreign_e_desc: `U.S. Owned` (required, string, nullable)
+ foreign_funding_desc: `Yes` (required, string, nullable)
+ interagency_contract_desc: `Company A` (required, string, nullable)
+ major_program: `None used` (required, string, nullable)
+ price_evaluation_adjustmen: `0.00` (required, string, nullable)
+ program_acronym: `NOT SURE` (required, string, nullable)
+ subcontracting_plan: `PLAN` (required, string, nullable)
+ multi_year_contract_desc: `No` (required, string, nullable)
+ purchase_card_as_paym_desc: `Yes` (required, string, nullable)
+ consolidated_contract_desc: `NO` (required, string, nullable)
+ type_of_contract_pric_desc: `FIRM FIXED PRICE` (required, string, nullable)

## Executive
+ officers (required, array[Officer])

## Officer
+ name: `John Doe` (required, string)
+ amount: 234242 (required, number)
<!--
# Group Tables

These endpoints support the tables on the individual Award Profile pages.

## SubAwards [/api/v2/subawards/]

This endpoint returns a list of sub-awards, their amount, action date, recipient name, and description.

### SubAwards [POST]

+ Request (application/json)
    + Attributes (object)
        + award_id: 123 (optional, string)
            The internal id of the award to filter on. If not included, all sub-awards are returned.
        + limit: 15 (optional, number)
            The number of results to include per page.
            + Default: 10
        + page: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + sort: subaward_number (optional, enum[string])
            The field results are sorted by.
            + Default: subaward_number
            + Members
                + subaward_number
                + description
                + action_date
                + amount
                + recipient_name
        + order: desc (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: desc

+ Response 200 (application/json)
    + Attributes
        + results (array[SubAwardResult], fixed-type)
        + page_metadata (PageMetaDataObject)

## Transactions [/api/v2/transactions/]

This endpoint returns a list of transactions, their amount, type, action date, action type, modification number, and description.

### Transactions [POST]

+ Request (application/json)
    + Attributes (object)
        + award_id: `12342er` (required, string)
            The internal id of the award to filter on.
        + limit: 15 (optional, number)
            The number of results to include per page.
            + Default: 10
        + page: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + sort: action_date (optional, enum[string])
            The field results are sorted by.
            + Default: `action_date`
            + Members
                + `modification_number`
                + `action_date`
                + `federal_action_obligation`
                + `face_value_loan_guarantee`
                + `original_loan_subsidy_cost`
                + `action_type_description`
                + description
        + order: desc (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: desc

+ Response 200 (application/json)
    + Attributes
        + results (array[TransactionResult], fixed-type)
        + page_metadata (PageMetaDataObject)

## Financial System Details [/api/v2/accounts/awards/]

This endpoint returns financial accounts by award.

## Financial System Details [POST]

+ Request (application/json)
    + Attributes (object)
        + award_id: `12178065` (required, string)
            The internal id of the award to filter on.
         + limit: 15 (optional, number)
            The number of results to include per page.
            + Default: 10
        + page: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + sort: `submission_date` (optional, enum[string])
            The field results are sorted by.
            + Default: `submission_date`
            + Members
                + `submission_date`
                + `federal_account_name`
                + `treasury_account_symbol`
                + `program_activity_name`
                + `object_class_name`
                + `obligated_amount`
        + order: desc (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: desc

+ Response 200 (application/json)
    + Attributes
        + results (array[FinancialSystemDetailsResult], fixed-type)
        + page_metadata (PageMetaDataObject)
        + award_id: `12178065` (required, string)
            The award id sent in the request.

# Data Structures

## SubAwardResult (object)
+ id: `1` (required, string)
    The internal sub-award id.
+ subaward_number: `2-A` (required, string)
    The sub-award id.
+ description: description (required, string)
+ action_date: `1999-01-15` (required, string)
    Action date in the format `YYYY-MM-DD`.
+ amount: 1234.56 (required, number)
    Monetary value of the sub-award.
+ recipient_name: Recipient A (required, string)

## TransactionResult (object)
+ id: `1` (required, string)
    The internal transaction id.
+ type: A (required, string)
    Award type code
+ type_description: BPA (required, string)
+ action_date: `1999-01-15` (required, string)
    Action date in the format `YYYY-MM-DD`.
+ action_type: C (required, string)
    Action type code
+ action_type_description: description (required, string)
+ modification_number: `0` (required, string)
+ description: MANAGEMENT AND OPERATIONS (required, string)
+ federal_action_obligation: 1234.56 (required, number, nullable)
    Monetary value of the transaction. Null for results with award type codes that correspond to loans.
+ face_value_loan_guarantee: 1234.56 (required, number, nullable)
    Face value of the loan. Null for results with award type codes that **do not** correspond to loans.
+ original_loan_subsidy_cost: 234.12 (required, number, nullable)
    Original subsidy cost of the loan. Null for results with award type codes that **do not** correspond to loans.

## FinancialSystemDetailsResult(object)
+ submission_id: 123 (required, number)
+ reporting_fiscal_year: 2018 (required, number)
+ reporting_fiscal_quarter: 4 (required, number)
+ submission_date: `FY 2018 Q4` (required, string)
    A string comprised of the reporting fiscal year and quarter that can be used for sorting by submission date.
+ obligated_amount: 45600000.00 (required, number)
+ federal_account_name: `Science, Energy Programs, Energy` (required, string)
+ federal_account_number: `089-0222` (required, string)
+ treasury_account_symbol: `089-X-0222-000` (required, string)
+ treasury_account_identifier: 19163 (required, number)
    Internal id for the treasury account.
+ major_object_class_name: `Acquisition of assets` (required, string)
+ major_object_class_code: `30` (required, string)
+ object_class_name: `Land and structures` (required, string)
+ object_class_code: `320` (required, string)
+ program_activity_name: `ADVANCED SCIENTIFIC COMPUTING RESEARCH` (required, string)
+ program_activity_code: `0002` (required, string)

## PageMetaDataObject (object)
+ page: 1 (required, number)
+ hasNext: false (required, boolean)
+ hasPrevious: false (required, boolean)

# Group IDV Awards Page

These endpoints support IDVs only.

## Award Amounts [/api/v2/awards/idvs/amounts/{award_id}/]

This endpoint returns aggregated award amounts for IDVs.

+ Parameters
    + award_id: `12178065` (required, string)
         Accepts the v2 generated award hash or internal database id.s

### Award Amounts [GET]

+ Response 200 (application/json)
    + Attributes
        + Attributes (AwardAmountsResponse)

## IDV Related Awards [/api/v2/awards/idvs/awards/]

This endpoint returns related awards for IDVs.

### IDV Related Awards [POST]

+ Request (application/json)
    + Attributes (object)
        + award_id: `12178065342fsg` (required, string)
            The v2 generated award hash or internal database id.
        + idv: true (optional, boolean)
             Field to request an IDV or a regular contract award
        + limit: 15 (optional, number)
            The number of results to include per page.
            + Default: 10
        + page: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + sort: `period_of_performance_start_date` (optional, enum[string])
            The field results are sorted by.
            + Default: start_date
            + Members
                + `piid`
                + `period_of_performance_start_date`
                + `description`
                + `period_of_performance_current_end_date`
                + `last_date_to_order`
                + `funding_agency`
                + `award_type`
                + `obligated_amount`
        + order: desc (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: desc

+ Response 200 (application/json)
    + Attributes
        + results (array[IDVRelatedAwardsResponse], fixed-type)
        + page_metadata (PageMetaDataObject)

# Data Structures

## AwardAmountsResponse (object)
+ award_id: 12178065 (required, number)
    The award id sent in the request.
+ generated_unique_award_id: `CONT_AW_1540_NONE_DJB30605051_NONE` (required, string)
+ idv_count: 0 (required, number)
+ contract_count: 0 (required, number)
+ rollup_base_exercised_options_val: 0.00 (required, number)
+ rollup_base_and_all_options_value: 106321.10 (required, number)
+ rollup_total_obligation: 106321.10 (required, number)

## IDVRelatedAwardsResponse (object)
+ award_id: 12178065 (required, number)
    The award id sent in the request.
+ generated_unique_award_id: `CONT_AW_1540_NONE_DJB30605051_NONE` (required, string)
+ piid: `W31P4Q15A0024` (required, string)
+ funding_agency (required, Agency, fixed-type)
+ award_type: `BPA`(required, string)
+ obligated_amount: 242342 (required, number)
+ description: `reerwrawa` (required, string, nullable)
+ period_of_performance_start_date: `2004-02-19` (required, string, nullable)
    The starting date of the contract in the format `YYYY-MM-DD`
+ period_of_performance_current_end_date: `2005-02-19` (required, string, nullable)
    The ending date of the contract in the format `YYYY-MM-DD`
+ last_date_to_order: `2301-02-23` (required, string, nullable)

 -->