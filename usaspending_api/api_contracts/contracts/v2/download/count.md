FORMAT: 1A
HOST: https://api.usaspending.gov

# Advanced Search

These endpoints support the advanced search page and allow for complex filtering for specific subsets of spending data.

# Group Visualizations

These endpoints return data that are grouped in preset units to support the various data visualizations on USAspending.gov's Advanced Search page.

## Download Count [/api/v2/download/count/]

Returns the number of transactions that would be included in a download request for the given filter set.

### Download Count Data [POST]
+ Request (application/json)
    + Attributes (object)
        + filters (required, FilterObject)
    + Body
        
            {
                "filters": { 
                    "keywords": ["Filter is required"] 
                }
            }

+ Response 200 (application/json)
    + Attributes
        + transaction_rows_gt_limit : true (required, boolean)
            A boolean returning whether the transaction count is over the maximum row limit.

# Data Structures

## Filter Objects
### FilterObject (object)
+ `keywords` : poptarts (optional, array[string])
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + domestic
    + foreign
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject])
+ `recipient_search_text`: `Hampton` (optional, array[string])
+ `recipient_id` (optional, string)
    A hash of recipient DUNS, name, and level. A unique identifier for recipients, used for profile page urls.
+ `recipient_scope` (optional, enum[string])
    + domestic
    + foreign
+ `recipient_locations` (optional, array[LocationObject])
+ `recipient_type_names`: `category_business` (optional, array[string])
    See options at https://github.com/fedspendingtransparency/usaspending-api/wiki/Recipient-Business-Types
+ `award_type_codes` (optional, FilterObjectAwardTypes)
    See use at
    https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation#award-type
+ `award_ids`: SPE30018FLGFZ, SPE30018FLJFN (optional, array[string])
+ `award_amounts` (optional, array[AwardAmounts])
+ `program_numbers`: `10.331` (optional, array[string])
+ `naics_codes`: 311812 (optional, array[string])
+ `psc_codes`: 8940, 8910 (optional, array[string])
+ `contract_pricing_type_codes`: `J` (optional, array[string])
+ `set_aside_type_codes`: `NONE` (optional, array[string])
+ `extent_competed_type_codes`: `A` (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)

### TimePeriodObject (object)
+ `start_date`: `2017-10-01` (required, string)
+ `end_date`: `2018-09-30` (required, string)
+ `date_type` (optional, enum[string])
    + action_date
    + last_modified_date

### LocationObject (object)
+ country: `USA` (required, string)
+ state: `VA` (optional, string)
+ county (optional, string)
+ city (optional, string)

### AgencyObject (object)
+ type (required, enum[string])
    + awarding
    + funding
+ tier (required, enum[string])
    + toptier
    + subtier
+ name: `Department of Defense` (required, string)

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

### Sample
- A
- B
- C
- D

### Default
- 02
- 03
- 04
- 05
- 06
- 07
- 08
- 09
- 10
- 11
- A
- B
- C
- D
- IDV_A
- IDV_B
- IDV_B_A
- IDV_B_B
- IDV_B_C
- IDV_C
- IDV_D
- IDV_E
