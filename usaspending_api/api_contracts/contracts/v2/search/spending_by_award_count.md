FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending by Award Count [/api/v2/search/spending_by_award_count/]

These endpoints support the advanced search page and allow for complex filtering for specific subsets of spending data.

## Number of awards for each type [POST /api/v2/search/spending_by_award_count/]

This endpoint takes award filters, and returns the number of awards in each award type (Contracts, Loans, Direct Payments, Grants, Other and IDVs).

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
        + `spending_level` (optional, enum[string])
            Group the spending by level. This also determines what data source is used for the totals.
            + Members
                + `awards` 
                + `subawards`
            + Default
                + `awards`
        + `subawards`: false (optional, boolean)
            True when you want to group by Subawards instead of Awards. Defaulted to False unless spending_level is set to `subawards`, then the default is True.
    + Body

            {
                "filters": {
                    "keywords": ["Filter is required"]
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (AwardTypeResult)
        + `spending_level` (required, enum[string])
            Spending level value that was provided in the request.
            + Members
                + `awards`
                + `subawards`
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

# Data Structures

## AwardTypeResult (object)
+ `grants` (required, number)
+ `loans` (required, number)
+ `contracts` (required, number)
+ `direct_payments` (required, number)
+ `other` (required, number)
+ `idvs` (required, number)

## SubawardTypeResult (object)
+ `subgrants` (required, number)
+ `subcontracts` (required, number)

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
+ `award_type_codes` (optional, FilterObjectAwardTypes)
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
+ `program_activities` (optional, array[ProgramActivityObject])
    A filter option that supports filtering by a program activity name or code. Please note that if this filter is used at least one of the members of the object, ProgramActivityObject, need to be provided.
+ `def_codes` (optional, array[DEFC], fixed-type)
  If the `def_codes` provided are in the COVID-19 group and the subaward flag is set to `False`, the query will only return prime awards that have at least one File C record with the supplied DEFC and also have non-zero COVID-19 related obligations or outlays.
  If the `def_codes` provided are in the COVID-19 or IIJA group and the subaward parameter is set to `True`, the query will only return results that have a sub_action_date on or after the enactment date of the public law associated with that disaster code.

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

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

### ProgramActivityObject (object)
At least one of the following fields are required when using the ProgramActivityObject.
+ `name`: (optional, string)
+ `code`: (optional, number)

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
