FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending by Award [/api/v2/search/spending_by_award/]

This endpoints supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

This endpoint takes award filters and fields, and returns the fields of the filtered awards.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
        + `fields` (required, SpendingByAwardFields)
        + `limit` (optional, number)
            How many results are returned. If no limit is specified, the limit is set to 10.
        + `order` (optional, enum[string])
            Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
            + Default: `desc`
            + Members
                + `desc`
                + `asc`
        + `page` (optional, number)
            The page number that is currently returned.
        + `sort` (optional, string)
            Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first field provided.
        + `subawards` (optional, boolean)
            True when you want to group by Subawards instead of Awards. Defaulted to False.
        + `last_record_unique_id` (optional, number)
            The unique id of the last record in the results set. Used in the experimental Elasticsearch API functionality.
        + `last_record_sort_value` (optional, string)
            The value of the last record that is being sorted on. Used in the experimental Elasticsearch API functionality.
        + `spending_level` (optional, enum[string])
            Group the spending by level. This also determines what data source is used for the totals.
            + Members
                + `awards`
                + `subawards`
            + Default
                + `awards`
    + Body

            {
                "subawards": false,
                "limit": 10,
                "page": 1,
                "filters": {
                    "award_type_codes": ["A", "B", "C"],
                    "time_period": [{"start_date": "2018-10-01", "end_date": "2019-09-30"}]
                },
                "fields": [
                    "Award ID",
                    "Recipient Name",
                    "Start Date",
                    "End Date",
                    "Award Amount",
                    "Awarding Agency",
                    "Awarding Sub Agency",
                    "Contract Award Type",
                    "Award Type",
                    "Funding Agency",
                    "Funding Sub Agency"
                ]
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `spending_level` (required, enum[string])
            Group the spending by level. This also determines what data source is used for the totals.
            + Members
                + `awards`
                + `subawards`
            + Default
                + `awards`
        + `limit` (required, number)
        + `results` (required, array[SpendingByAwardResponse], fixed-type)
        + `page_metadata` (PageMetadataObject)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

# Data Structures

## SpendingByAwardFields (array)
The SpendingByAward API can accept any of the following fields based on whether awards or subawards are being searched.
Awards are searched by default, however the subawards can be searched by setting the subawards field to "True".

### Awards
Award based searches have a base set of fields that are valid for all award types, however there are some type specific fields that are only valid for certain award types, which have been described further below.

#### Base fields
- `Award ID`
- `Recipient Name`
- `Recipient DUNS Number`
- `recipient_id`
- `Awarding Agency`
- `Awarding Agency Code`
- `Awarding Sub Agency`
- `Awarding Sub Agency Code`
- `Funding Agency`
- `Funding Agency Code`
- `Funding Sub Agency`
- `Funding Sub Agency Code`
- `Place of Performance City Code`
- `Place of Performance State Code`
- `Place of Performance Country Code`
- `Place of Performance Zip5`
- `Description`
- `Last Modified Date`
- `Base Obligation Date`
- `prime_award_recipient_id`
- `generated_internal_id`
- `def_codes`
- `COVID-19 Obligations`
- `COVID-19 Outlays`
- `Infrastructure Obligations`
- `Infrastructure Outlays`
- `Recipient UEI`
- `Recipient Location`
- `Primary Place of Performance`

#### Contracts
Contracts can be searched for specifically by using the Contract Type Codes (A, B, C, or D) and the following fields are additional fields that can be requested.

- `Start Date`
- `End Date`
- `Award Amount`
- `Total Outlays`
- `Contract Award Type`
- `NAICS`
- `PSC`

#### IDV
IDV can be searched for specifically by using the IDV Type Codes (IDV_A, IDV_B, IDV_B_A, IDV_B_B, IDV_B_C, IDV_C, IDV_D or IDV_E) and the following fields are additional fields that can be requested.

- `Start Date`
- `Award Amount`
- `Total Outlays`
- `Contract Award Type`
- `Last Date to Order`
- `NAICS`
- `PSC`

#### Loan
Loans can be searched for specifically by using the Loan Type Codes (07 or 08) and the following fields are additional fields that can be requested.

- `Issued Date`
- `Loan Value`
- `Subsidy Cost`
- `SAI Number`
- `CFDA Number`
- `Assistance Listings`
- `primary_assistance_listing`

#### Non Loan Assistance
Non Loan Assistance Awards can be searched for specifically by using the Non Loan Assistance Type Codes. The Non Loan Assist Type Codes include Grant Type Codes (02, 03, 04 or 05), Direct Payments (06 or 10), and Other Type Codes (09, 11, or -1). The following fields are additional fields that can be requested.

- `Start Date`
- `End Date`
- `Award Amount`
- `Total Outlays`
- `Award Type`
- `SAI Number`
- `CFDA Number`
- `Assistance Listings`
- `primary_assistance_listing`

### Subaward

#### Contract Subawards
- `Awarding Agency`
- `Awarding Sub Agency`
- `NAICS`
- `Prime Award ID`
- `prime_award_recipient_id`
- `Prime Award Recipient UEI`
- `Prime Recipient Name`
- `PSC`
- `Sub-Award Amount`
- `Sub-Award Date`
- `Sub-Award Description`
- `Sub-Award ID`
- `Sub-Award Primary Place of Performance`
- `sub_award_recipient_id`
- `Sub-Award Type`
- `Sub-Awardee Name`
- `Sub-Recipient Location`
- `Sub-Recipient UEI`

#### Grant Subawards
- `Assistance Listing`
- `Awarding Agency`
- `Awarding Sub Agency`
- `Prime Award ID`
- `prime_award_recipient_id`
- `Prime Award Recipient UEI`
- `Prime Recipient Name`
- `Sub-Award Amount`
- `Sub-Award Date`
- `Sub-Award Description`
- `Sub-Award ID`
- `Sub-Award Primary Place of Performance`
- `sub_award_recipient_id`
- `Sub-Award Type`
- `Sub-Awardee Name`
- `Sub-Recipient Location`
- `Sub-Recipient UEI`

## SpendingByAwardResponse (object)
+ `internal_id` (required, number)
+ `Award Amount` (optional, number)
+ `Total Outlays` (optional, number)
+ `Award ID` (optional, string)
+ `Award Type` (optional, string, nullable)
+ `Awarding Agency Code` (optional, string, nullable)
+ `Awarding Agency` (optional, string, nullable)
+ `awarding_agency_id` (optional, number, nullable)
+ `Awarding Sub Agency Code` (optional, string, nullable)
+ `Awarding Sub Agency` (optional, string, nullable)
+ `Base Obligation Date` (optional, string)
+ `CFDA Number` (optional, string, nullable)
    Assistance awards only
+ `Contract Award Type` (optional, string)
    Procurement awards only
+ `Description` (optional, string, nullable)
+ `End Date` (optional, string)
+ `Funding Agency Code` (optional, string, nullable)
+ `Funding Agency` (optional, string, nullable)
+ `Funding Sub Agency Code` (optional, string, nullable)
+ `Funding Sub Agency` (optional, string, nullable)
+ `generated_internal_id` (optional, string)
+ `Issued Date` (optional, string, nullable)
+ `Last Date to Order` (optional, string, nullable)
    Procurement IDVs only
+ `Last Modified Date` (optional, string)
+ `Loan Value` (optional, number)
    Assistance awards only
+ `Period of Performance Current End Date` (optional, string, nullable)
+ `Period of Performance Start Date` (optional, string)
+ `Place of Performance City Code` (optional, number)
+ `Place of Performance Country Code` (optional, string, nullable)
+ `Place of Performance State Code` (optional, number, nullable)
+ `Place of Performance Zip5` (optional, number)
+ `COVID-19 Outlays` (optional, number)
+ `COVID-19 Obligations` (optional, number)
+ `Infrastructure Outlays` (optional, number)
+ `Infrastructure Obligations` (optional, number)
+ `def_codes` (optional, array[string], fixed-type)
+ `Prime Award ID` (optional, string, nullable)
    Sub-Awards only, returns the ID (piid/fain/uri) of the prime award.
+ `Prime Recipient Name` (optional, string, nullable)
    Sub-Awards only, returns the name of the prime award's recipient.
+ `prime_award_recipient_id` (optional, string, nullable)
    Sub-Awards only, return the recipient id of the prime award's recipient.
+ `prime_award_internal_id` (optional, string, nullable)
    Sub-Awards only, return the award id of the prime award.
+ `prime_award_generated_internal_id` (optional, string)
    Sub-Awards only, return the generated unique award id of the prime award.
+ `Recipient DUNS Number` (optional, string, nullable)
+ `Recipient Name` (optional, string, nullable)
+ `recipient_id` (optional, string, nullable)
    A unique identifier for the recipient which includes the recipient hash and level.
+ `SAI Number` (optional, string, nullable)
    Assistance awards only
+ `Start Date` (optional, string)
+ `Sub-Award Amount` (optional, string)
    Sub-Awards only
+ `Sub-Award Date` (optional, string)
    Sub-Awards only
+ `Sub-Award ID` (optional, string)
    Sub-Awards only
+ `Sub-Award Type` (optional, string)
    Sub-Awards only
+ `Sub-Awardee Name` (optional, string)
    Sub-Awards only
+ `Subsidy Cost` (optional, number)
    Assistance awards only
+ `agency_slug` (optional, string, nullable)
    Returns with "Awarding Agency", is null when the agency does not have an agency profile page

## PageMetadataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)
+ `last_record_unique_id` (optional, number)
+ `last_record_sort_value` (optional, string)

## Filter Objects
### AdvancedFilterObject (object)
+ `keywords` : [`transport`] (optional, array[string])
+ `description` (optional, string)
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text`: [`Hampton`, `Roads`] (optional, array[string])
    + Text searched across a recipient’s name, UEI, and DUNS
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names`: [`category_business`, `sole_proprietorship`] (optional, array[string])
+ `award_type_codes` (required, FilterObjectAwardTypes)
+ `award_ids`: [`SPE30018FLGFZ`, `SPE30018FLJFN`] (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers`: [`10.331`] (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `contract_pricing_type_codes`: [`J`] (optional, array[string])
+ `set_aside_type_codes`: [`NONE`] (optional, array[string])
+ `extent_competed_type_codes`: [`A`] (optional, array[string])
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)
+ `program_activity` (optional, array[number])
+ `program_activities` (optional, array[ProgramActivityObject])
    A filter option that supports filtering by a program activity name or code. Please note that if this filter is used at least one of the members of the object, ProgramActivityObject, need to be provided.
+ `def_codes` (optional, array[DEFC], fixed-type)
  If the `def_codes` provided are in the COVID-19 or IIJA group and the subaward flag is set to `False`, the query will only return prime awards that have at least one File C record with the supplied DEFC and also have non-zero COVID-19 or IIJA related obligations or outlays.
  If the `def_codes` provided are in the COVID-19 or IIJA group and the subaward parameter is set to `True`, the query will only return results that have a sub_action_date on or after the enactment date of the public law associated with that disaster code.
    + Example: Providing the `Z` DEF code and setting the subaward parameter to `True` will only return results where the `sub_action_date` is on or after `11/15/2021` since this is the enactment date of the public law associated with disaster code `Z`.
+ `award_unique_id` (optional, string)

### TimePeriodObject (object)
This TimePeriodObject can fall into different categories based on the request.
+ if `subawards` true

    See the Subaward Search category defined in [SubawardSearchTimePeriodObject](../../../search_filters.md#subaward-search-time-period-object)

+ otherwise

    See the Award Search category defined in [AwardSearchTimePeriodObject](../../../search_filters.md#award-search-time-period-object)


### LocationObject (object)
These fields are defined in the [StandardLocationObject](../../../search_filters.md#standard-location-object)

### AgencyObject (object)
+ `type` (required, enum[string])
    + Members
        + `awarding`
        + `funding`
+ `tier` (required, enum[string])
    + Members
        + `toptier`
        + `subtier`
+ `name`: `Office of Inspector General` (required, string)
+ `toptier_name`: `Department of the Treasury` (optional, string)
    Only applicable when `tier` is `subtier`.  Ignored when `tier` is `toptier`.  Provides a means by which to scope subtiers with common names to a
    specific toptier.  For example, several agencies have an "Office of Inspector General".  If not provided, subtiers may span more than one toptier.

### AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound`: 1000000 (optional, number)

### ProgramActivityObject (object)
At least one of the following fields are required when using the ProgramActivityObject.
+ `name`: (optional, string)
+ `code`: (optional, number)

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

### PSCCodeObject (object)
+ `require`: [[`Service`, `B`, `B5`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`Service`, `B`, `B5`, `B502`]] (optional, array[array[string]], fixed-type)

### TASCodeObject (object)
+ `require`: [[`091`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`091`, `091-0800`]] (optional, array[array[string]], fixed-type)

### TreasuryAccountComponentsObject (object)
+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier - three characters
+ `aid` (required, string)
    Agency Identifier - three characters
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability - four digits
+ `epoa` (optional, string, nullable)
    Ending Period of Availability - four digits
+ `a` (optional, string, nullable)
    Availability Type Code - X or null
+ `main` (required, string)
    Main Account Code - four digits
+ `sub` (optional, string, nullable)
    Sub-Account Code - three digits

### FilterObjectAwardTypes (array)
List of filterable award types

#### Sample
- `A`
- `B`
- `C`
- `D`

#### Default
- `02`
- `03`
- `04`
- `05`
- `06`
- `07`
- `08`
- `09`
- `10`
- `11`
- `A`
- `B`
- `C`
- `D`
- `IDV_A`
- `IDV_B`
- `IDV_B_A`
- `IDV_B_B`
- `IDV_B_C`
- `IDV_C`
- `IDV_D`
- `IDV_E`

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)
