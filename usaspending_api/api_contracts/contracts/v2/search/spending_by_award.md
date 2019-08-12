FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending by Award [/api/v2/search/spending_by_award/]

This endpoints supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## List fields of filtered awards [POST /api/v2/search/spending_by_award/]

This endpoint takes award filters and fields, and returns the fields of the filtered awards.

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, FilterObject)
        + `fields` (required, SpendingByAwardFields)
            See options at https://github.com/fedspendingtransparency/usaspending-api/blob/stg/usaspending_api/api_docs/api_documentation/advanced_award_search/spending_by_award.md#fields
        + `limit`: 60 (optional, number)
            How many results are returned. If no limit is specified, the limit is set to 10.
        + `order`: `desc` (optional, string)
            Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order. Defaults to asc.
        + `page`: 1 (optional, number)
            The page number that is currently returned.
        + `sort`: `Award Amount` (optional, string)
            Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first field provided.
        + `subawards`: false (optional, boolean)
            True when you want to group by Subawards instead of Awards. Defaulted to False.
    + Body

            {
                "filters": { 
                    "award_type_codes": ["A", "B", "C"]
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
    + Attributes
        + `limit`: 60 (optional, number)
        + `results` (array[SpendingByAwardResponse])
        + `page_metadata` (PageMetadataObject)

# Data Structures

## SpendingByAwardFields (array)
List of table columns

### Sample
- Award ID
- Recipient Name
- Start Date
- End Date
- Award Amount
- Awarding Agency
- Awarding Sub Agency
- Contract Award Type
- Award Type
- Funding Agency
- Funding Sub Agency

### Default
- `Award ID`
- `Recipient Name`
- `Start Date`
- `End Date`
- `Award Amount`
- `Awarding Agency`
- `Awarding Sub Agency`
- `Contract Award Type`
- `Award Type`
- `Funding Agency`
- `Funding Sub Agency`

## SpendingByAwardResponse (object)
+ `Recipient Name` (optional, string, nullable)
+ `Recipient DUNS Number` (optional, string, nullable)
+ `Awarding Agency` (optional, string, nullable)
+ `Awarding Agency Code` (optional, string, nullable)
+ `Awarding Sub Agency` (optional, string, nullable)
+ `Awarding Sub Agency Code` (optional, string, nullable)
+ `Funding Agency` (optional, string, nullable)
+ `Funding Agency Code` (optional, string, nullable)
+ `Funding Sub Agency` (optional, string, nullable)
+ `Funding Sub Agency Code` (optional, string, nullable)
+ `Place of Performance City Code` (optional, number)
+ `Place of Performance State Code `(optional, number, nullable)
+ `Place of Performance Country Code` (optional, string, nullable)
+ `Place of Performance Zip5` (optional, number)
+ `Period of Performance Start Date` (optional, string)
+ `Period of Performance Current End Date` (optional, string, nullable)
+ `Description` (optional, string, nullable)
+ `Last Modified Date` (optional, string)
+ `Base Obligation Date` (optional, string)
+ `Award ID` (optional, string)
+ `Start Date` (optional, string)
+ `End Date` (optional, string)
+ `Last Date to Order` (optional, string, nullable)
+ `Award Amount` (optional, number)
+ `Award Type` (optional, string)
+ `Contract Award Type` (optional, string)
+ `SAI Number` (optional, string, nullable)
+ `CFDA Number` (optional, string, nullable)
+ `Issued Date` (optional, string, nullable)
+ `Loan Value` (optional, number, nullable)
+ `Subsidy Cost` (optional, number, nullable)
+ `internal_id`  (optional, string)

## PageMetadataObject (object)
+ page (required, number)
+ hasNext (required, boolean)

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
