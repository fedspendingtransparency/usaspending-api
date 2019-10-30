FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending by Geography [/api/v2/search/spending_by_geography/]

This endpoint supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

This endpoint takes award filters, and returns aggregated obligation amounts in different geographic areas.

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, FilterObject)
        + `subawards`: false (optional, boolean)
            True when you want to group by Subawards instead of Awards. Defaulted to False.
        + `scope`: `place_of_performance` (required, enum[string])
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
        + `geo_layer_filters` (required, array[string])
            - `DC`
            - `MD`
            - `VA`
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

# Data Structures

## GeographyTypeResult (object)
+ `aggregated_amount` (required, number)
+ `display_name` (required, string)
+ `shape_code` (required, string)


## Filter Objects
### FilterObject (object)
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
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers`: `10.331` (optional, array[string])
+ `naics_codes`: `311812` (optional, array[string])
+ `psc_codes`: `8940`, `8910` (optional, array[string])
+ `contract_pricing_type_codes`: `J` (optional, array[string])
+ `set_aside_type_codes`: `NONE` (optional, array[string])
+ `extent_competed_type_codes`: `A` (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)

### TimePeriodObject (object)
+ `start_date`: `2017-10-01` (required, string)
+ `end_date`: `2018-09-30` (required, string)
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
+ `name`: `Department of Defense` (required, string)

### AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound`: 1000000 (optional, number)

### TASCodeObject (object)
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
