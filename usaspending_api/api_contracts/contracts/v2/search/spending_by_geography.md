FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending by Geography [/api/v2/search/spending_by_geography/]

This endpoint supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

This endpoint takes award filters, and returns aggregated obligation amounts in different geographic areas.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
        + `subawards` (optional, boolean)
            True when you want to group by Subawards instead of Awards. Defaulted to False.
        + `spending_level` (optional, enum[string])
            Group the spending by level. This also determines what data source is used for the totals.
            + Members
                + `transactions`
                + `awards`
                + `subawards`
            + Default
                + `transactions`
        + `scope` (required, enum[string])
            When fetching transactions, use the primary place of performance or recipient location
            + Members
                + `place_of_performance`
                + `recipient_location`
        + `geo_layer` (required, enum[string])
            Set the type of areas in the response
            + Members
                + `state`
                + `county`
                + `district`
                + `country`
        + `geo_layer_filters` (optional, array[string])
            List of U.S. state codes, U.S. county codes, U.S. Congressional districts, or ISO 3166-1 alpha-3 country codes to show results for.

    + Body

            {
                "filters": {
                    "keywords": ["Filter is required"]
                },
                "scope": "place_of_performance",
                "geo_layer": "state"
            }


+ Response 200 (application/json)
    + Attributes (object)
        + `scope`
        + `geo_layer`
        + `spending_level` (required, enum[string])
            Spending level value that was provided in the request.
            + Members
                + `transactions`
                + `awards`
                + `subawards`
        + `results` (array[GeographyTypeResult], fixed-type)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "spending_level": "transactions",
                "results": [
                    {
                        "shape_code": "ND",
                        "aggregated_amount": 4771026.93,
                        "display_name": "North Dakota",
                        "population": 762062,
                        "per_capita": 6.26
                    },
                    {
                        "shape_code": "NV",
                        "aggregated_amount": 26928552.59,
                        "display_name": "Nevada",
                        "population": 3080156,
                        "per_capita": 8.74
                    },
                    {
                        "shape_code": "OH",
                        "aggregated_amount": 187505278.16,
                        "display_name": "Ohio",
                        "population": 11689100,
                        "per_capita": 16.04
                    }
                ],
                "messages": [
                    "For searches, time period start and end dates are currently limited to an earliest date of 2007-10-01.  For data going back to 2000-10-01, use either the Custom Award Download feature on the website or one of our download or bulk_download API endpoints as listed on https://api.usaspending.gov/docs/endpoints.",
                    "The 'subawards' field will be deprecated in the future. Set 'spending_level' to 'subawards' instead. See documentation for more information."
                ]
            }

# Data Structures

## GeographyTypeResult (object)
+ `aggregated_amount` (required, number)
+ `display_name` (required, string)
+ `shape_code` (required, string)
+ `population` (required, number, nullable)
+ `per_capita` (required, number, nullable)
+ `total_outlays` (optional, number) Only included when the `spending_level` of the Response is `awards`


## Filter Objects
### AdvancedFilterObject (object)
+ `keywords` : [`transport`] (optional, array[string])
+ `description` (optional, string)
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + The **default** value below only applies to `geo_layer` values of `county`, `district` and `state`.
    + Default: `domestic`
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text`: [`Hampton`, `Roads`] (optional, array[string])
    + Text searched across a recipient’s name, UEI, and DUNS
+ `recipient_scope` (optional, enum[string])
    + The **default** value below only applies to `geo_layer` values of `county`, `district` and `state`.
    + Default: `domestic`
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
  If the `def_codes` provided are in the COVID-19 or IIJA group, the query will only return results of transactions where the `action_date` is on or after the enactment date of the public law associated with that disaster code.

### TimePeriodObject (object)
This TimePeriodObject can fall into different categories based on the request.
+ if `spending_level` is `subawards` or `subawards` is true

    See the Subaward Search category defined in [SubawardSearchTimePeriodObject](../../../search_filters.md#subaward-search-time-period-object)

+ otherwise

    See the Transaction Search category defined in [TransactionSearchTimePeriodObject](../../../search_filters.md#transaction-search-time-period-object)

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
+ `name` (required, string)
+ `toptier_name` (optional, string)
    Only applicable when `tier` is `subtier`.  Ignored when `tier` is `toptier`.  Provides a means by which to scope subtiers with common names to a
    specific toptier.  For example, several agencies have an "Office of Inspector General".  If not provided, subtiers may span more than one toptier.

### ProgramActivityObject (object)
At least one of the following fields are required when using the ProgramActivityObject.
+ `name`: (optional, string)
+ `code`: (optional, number)

### AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound` (optional, number)

### NAICSCodeObject (object)
+ `require` (optional, array[string])
+ `exclude` (optional, array[string])

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