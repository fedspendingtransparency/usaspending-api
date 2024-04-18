FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending by Award [/api/v2/search/spending_by_subaward_grouped/]

This endpoint supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

Searches for subaward records based on a provided set of filters and groups them by their prime award.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
            These filters are applied at the Subaward level (`children`). They will not affect the Prime Awards (`results`) themselves
        + `fields` (required, SpendingBySubawardFields)
            The field names to include for `children` objects (Subawards) in the response.
        + `limit`: 5 (optional, number)
            The number of `results` (Prime Awards) to include per page. The number of `children` (Subawards) objects will always be limited to 10
            + Default: 10
        + `page`: 1 (optional, number)
            The page of `results` (Prime Awards) to return based on `limit`.
            + Default: 1
        + `sort`: `Prime Award ID` (required, string)
            The field on which to order `results` objects (Prime Awards) in the response. The `children` (Subawards) object's sort will always be `Subaward Obligations`
            + Default: `Prime Award ID`
        + `order` (optional, enum[string])
            The direction in which to order `results` (Prime Awards). `asc` for ascending or `desc` for descending. The `children` (Subawards) objects will always be ordered `asc`
            + Default: `desc`
            + Members
                + `asc`
                + `desc`
    + Body

            {
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
        + `limit` (required, number)
        + `results` (required, array[AwardGroupResponse], fixed-type)
        + `page_metadata` (PageMetaDataObject)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

# Data Structures

## Request Objects

### SpendingBySubawardFields (array)
The Spending by Subaward Grouped API can accept any of the following fields regardless of the supplied `award_type_codes` filter values. 
- `Sub-Award ID`
- `Sub-Award Type`
- `Sub-Awardee Name`
- `Sub-Award Date`
- `Sub-Award Amount`
- `Awarding Agency`
- `Awarding Sub Agency`
- `Prime Award ID`
- `Prime Recipient Name`
- `prime_award_recipient_id`

## Filter Objects
### AdvancedFilterObject (object)
+ `keywords` : [`transport`] (optional, array[string])
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
+ `object_class` (optional, array[string])
+ `program_activity` (optional, array[number])
+ `def_codes` (optional, array[DEFC], fixed-type)
  If the `def_codes` provided are in the COVID-19 or IIJA group, the query will only return subaward results with a prime award matching one of the supplied def codes and a sub_action_date on or after the enactment date of the public law associated with that disaster code.
    + Example: Providing the `Z` DEF code will only return results where the `sub_action_date` is on or after `11/15/2021` since this is the enactment date of the public law associated with disaster code `Z`.

### TimePeriodObject (object)
See the Subaward Search category defined in [SubawardSearchTimePeriodObject](../../../search_filters.md#subaward-search-time-period-object)

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

## Response Objects

### AwardGroupResponse (object)
+ `Prime Award ID` (required, string)
+ `Matching Subaward Count` (required, number)
+ `Matching Subaward Obligation` (required, number)
+ `children` (required, array[SubawardResponse], fixed-type)
    Regardless of the number of matching Subawards associated with a Prime Award, only 10 at most will be included in this `children` section.
    To retrieve more Subawards with the provided filters associated with the Award, use the [api/v2/search/spending_by_award/](./spending_by_award.md) endpoint with `subawards` set to `true` and an additional filter for Award ID.
+ `page_metadata` (required, PageMetaDataObject, fixed-type)
+ `limit`: 10 (required, number)

### SubawardResponse (object)
+ `internal_id` (required, number)
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
+ `Awarding Agency` (optional, string, nullable)
+ `Awarding Sub Agency` (optional, string, nullable)
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
    

### PageMetaDataObject (object)
+ `page`: 1 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)