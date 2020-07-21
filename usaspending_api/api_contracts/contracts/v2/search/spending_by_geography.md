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
        + `geo_layer_filters` (optional, array[string])

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
        + `results` (array[GeographyTypeResult], fixed-type)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "scope": "place_of_performance",
                "geo_layer": "state",
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
                    "For searches, time period start and end dates are currently limited to an earliest date of 2007-10-01.  For data going back to 2000-10-01, use either the Custom Award Download feature on the website or one of our download or bulk_download API endpoints as listed on https://api.usaspending.gov/docs/endpoints."
                ]
            }

# Data Structures

## GeographyTypeResult (object)
+ `aggregated_amount` (required, number)
+ `display_name` (required, string)
+ `shape_code` (required, string)
+ `population` (required, number, nullable)
+ `per_capita` (required, number, nullable)


## Filter Objects
### AdvancedFilterObject (object)
+ `keywords` : `transport` (optional, array[string])
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text`: `Hampton` (optional, array[string])
+ `recipient_id` (optional, string)
    A hash of recipient DUNS, name, and level. A unique identifier for recipients, used for profile page urls.
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names`: `category_business` (optional, array[string])
+ `award_type_codes` (optional, FilterObjectAwardTypes)
+ `award_ids`: `SPE30018FLGFZ`, `SPE30018FLJFN` (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers`: `10.331` (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `contract_pricing_type_codes`: `J` (optional, array[string])
+ `set_aside_type_codes`: `NONE` (optional, array[string])
+ `extent_competed_type_codes`: `A` (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)

### TimePeriodObject (object)
+ `start_date`: `2017-10-01` (required, string)
    Currently limited to an earliest date of `2007-10-01` (FY2008).  For data going back to `2000-10-01` (FY2001), use either the Custom Award Download
    feature on the website or one of our `download` or `bulk_download` API endpoints.
+ `end_date`: `2018-09-30` (required, string)
    Currently limited to an earliest date of `2007-10-01` (FY2008).  For data going back to `2000-10-01` (FY2001), use either the Custom Award Download
    feature on the website or one of our `download` or `bulk_download` API endpoints.
+ `date_type` (optional, enum[string])
    + Members
        + `action_date`
        + `last_modified_date`

### LocationObject (object)
+ `country`: `USA` (required, string)
+ `state`: `VA` (optional, string)
+ `county` (optional, string)
+ `city` (optional, string)
+ `district` (optional, string)
+ `zip` (optional, string)

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
