FORMAT: 1A
HOST: https://api.usaspending.gov

# Advanced Search

These endpoints support the advanced search page and allow for complex filtering for specific subsets of spending data.

# Group Visualizations

These endpoints return data that are grouped in preset units to support the various data visualizations on USAspending.gov's Advanced Search page.

## Spending by Award [/api/v2/search/spending_by_award/]

This endpoint takes award filters and fields, and returns the fields of the filtered awards.

### Spending by Award [POST]

+ Request (application/json)
    + Attributes (object)
        + `filters` (optional, FilterObject)
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

+ Response 200 (application/json)
    + Attributes
        + `limit`: 60 (optional, number)
        + `results` (array[SpendingByAwardResponse])
        + `page_metadata` (PageMetadataObject)

## Spending By Category [/api/v2/search/spending_by_category/]

This endpoint returns a list of the top results of specific categories sorted by the total amounts in descending order.

### Spending By Category [POST]

+ Request (application/json)
    + Attributes (object)
        + category: `awarding_agency` (required, enum[string])
            + Members
                + `awarding_agency`
                + `awarding_subagency`
                + `funding_agency`
                + `funding_subagency`
                + `recipient_duns`
                + `recipient_parent_duns`
                + `cfda`
                + `psc`
                + `naics`
                + `county`
                + `district`
                + `federal_account`
                + `country`
                + `state_territory`
        + filters (required, FilterObject)
            The filters to find with said category
        + limit: 5 (optional, number)
            The number of results to include per page
        + page: 1 (optional, number)
            The page of results to return based on the limit

+ Response 200 (application/json)
    + Attributes
        + category: `awarding_agency` (required, string)
        + results (array[CategoryResult], fixed-type)
        + limit: 10 (required, number)
        + page_metadata (PageMetadataObject)

## Spending Over Time [/api/v2/search/spending_over_time/]

This endpoint returns a list of aggregated award amounts grouped by time period in ascending order (earliest to most recent).

### Spending Over Time [POST]

+ Request (application/json)
    + Attributes (object)
        + group: `quarter` (required, enum[string])
            + Members
                + `fiscal_year`
                + `quarter`
                + `month`
        + filters (required, FilterObject)
        + subawards (optional, boolean)
            True to group by sub-awards instead of prime awards. Defaults to false.
            + Default: false

+ Response 200 (application/json)
    + Attributes
        + group (required, enum[string])
            + Members
                + `fiscal_year`
                + `quarter`
                + `month`
        + results (array[TimeResult], fixed-type)

## Download Count [/api/v2/download/count/]

Returns the number of transactions that would be included in a download request for the given filter set.

### Download Count Data [POST]
+ Request (application/json)
    + Attributes (object)
        + filters (required, FilterObject)

+ Response 200 (application/json)
    + Attributes
        + transaction_rows_gt_limit : true (required, boolean)
            A boolean returning whether the transaction count is over the maximum row limit.


## Generated Filter Hash for URL [/api/v1/references/filter/]

Generates a hash for URL, based on selected filter criteria.

### Generated Filter Hash for URL Data [POST]
+ Request (application/json)
    + Attributes (object)
        + filters (optional, FilterObject)

+ Response 200 (application/json)
    + Attributes
        + hash : `5703c297b902ac3b76088c5c275b53f9` (required, string)


## Restore Filters From URL Hash [/api/v1/references/hash/]

Restores selected filter criteria, based on URL hash.

### Restore Filters From URL Hash Data [POST]
+ Request (application/json)
    + Attributes (object)
        + hash : `5703c297b902ac3b76088c5c275b53f9` (required, string)

+ Response 200 (application/json)
    + Attributes
        + filter (optional, object)
            + filters (optional, object)
                + awardAmounts (required, object)
                + awardType (required, array[string])
                + extentCompeted (required, array[string])
                + keyword (required, object)
                + locationDomesticForeign (required, string)
                + pricingType (required, array[string])
                + recipientDomesticForeign (required, string)
                + recipientType (required, array[string])
                + selectedAwardIDs (required, object)
                + selectedAwardingAgencies (required, object)
                + selectedCFDA (required, object)
                + selectedFundingAgencies (required, object)
                + selectedLocations (required, object)
                + selectedNAICS (required, object)
                + selectedPSC (required, object)
                + selectedRecipientLocations (required, object)
                + selectedRecipients (required, array[string])
                + setAside (required, array[string])
                + timePeriodEnd (required, string, nullable)
                + timePeriodFY (required, array[string])
                + timePeriodStart (required, string, nullable)
                + timePeriodType (required, string)
            + version (optional, string)

    + Body

            {
                "filter": {
                    "filters": {
                        "awardAmounts": {},
                        "awardType": [],
                        "extentCompeted": [],
                        "keyword": {},
                        "locationDomesticForeign": "all",
                        "pricingType": [],
                        "recipientDomesticForeign": "all",
                        "recipientType": [],
                        "selectedAwardIDs": {},
                        "selectedAwardingAgencies": {},
                        "selectedCFDA": {},
                        "selectedFundingAgencies": {},
                        "selectedLocations": {},
                        "selectedNAICS": {},
                        "selectedPSC": {},
                        "selectedRecipientLocations": {},
                        "selectedRecipients": [],
                        "setAside": [],
                        "timePeriodEnd": null,
                        "timePeriodFY": ["2019"],
                        "timePeriodStart": null,
                        "timePeriodType": "fy"
                    },
                    "version": "2017-11-21"
                }
            }

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

## CategoryResult (object)
+ id (required, number)
    The id is the database key.
+ name (required, string, nullable)
+ code (required, string, nullable)
    code is a user-displayable code (such as a program activity or NAICS code, but **not** a database ID). When no such code is relevant, return a `null`.
+ amount (required, number)

## TimeResult (object)
+ `time_period` (required, TimePeriodGroup)
+ `aggregated_amount` (required, number)
    The aggregate award amount for this time period and the given filters.

## PageMetadataObject (object)
+ page (required, number)
+ hasNext (required, boolean)

## TimePeriodGroup (object)
+ `fiscal_year`: `2018` (required, string)
+ `quarter`: `1` (optional, string)
    Excluded when grouping by `fiscal_year` or `month`.
+ `month`: `1` (optional, string)
    Excluded when grouping by `fiscal_year` or `quarter`.


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