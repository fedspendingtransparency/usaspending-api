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
        + limit: 60 (optional, number)
        + results (array[SpendingByAwardResponse])
        + page_metadata (PageMetadataObject)

## Spending by Award Count [/api/v2/search/spending_by_award_count/]

This endpoint takes award filters, and returns the number of awards in each award type (Contracts, Loans, Direct Payments, Grants, Other and IDVs).

### Spending by Award Count [POST]

+ Request (application/json)
    + Attributes (object)
        + filters (required, FilterObject)
        + subawards: false (optional, boolean)
            True when you want to group by Subawards instead of Awards. Defaulted to False.

+ Response 200 (application/json)
    + Attributes
        + results (AwardTypeResult)

## Spending By Category [/api/v2/search/spending_by_category/]

This endpoint returns a list of the top results of specific categories sorted by the total amounts in descending order.

### Spending By Category [POST]

+ Request (application/json)
    + Attributes (object)
        + category: `awarding_agency` (required, enum[string])
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
            + `fiscal_year`
            + `quarter`
            + `month`
        + filters (required, FilterObject)
        + subawards (optional, boolean)
            True to group by sub-awards instead of prime awards. Defaults to false.
            + Default: false

+ Response 200 (application/json)
    + Attributes
        + group: `quarter` (required, enum[string])
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
                + locationDomesticForeign : `all` (required, string)
                + pricingType (required, array[string])
                + recipientDomesticForeign : `all` (required, string)
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
                + timePeriodFY : `2019` (required, array[string])
                + timePeriodStart (required, string, nullable)
                + timePeriodType : `fy` (required, string)
            + version: `2017-11-21` (optional, string)

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

## FilterObjectAwardTypes (array)
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

## SpendingByAwardResponse (object)
+ Recipient Name : `MULTIPLE RECIPIENTS` (optional, string)
+ Recipient DUNS Number: `001006360` (optional, string)
+ Awarding Agency : `Social Security Administration` (optional, string)
+ Awarding Agency Code : 01 (optional, number)
+ Awarding Sub Agency : `Social Security Administration` (optional, string)
+ Awarding Sub Agency Code : 01 (optional, number)
+ Funding Agency : `Social Security Administration` (optional, string)
+ Funding Agency Code : 01 (optional, number)
+ Funding Sub Agency : `Social Security Administration` (optional, string)
+ Funding Sub Agency Code : 01 (optional, number)
+ Place of Performance City Code : 01 (optional, number)
+ Place of Performance State Code : 02 (optional, number)
+ Place of Performance Country Code : 03 (optional, number)
+ Place of Performance Zip5 : 22205 (optional, number)
+ Period of Performance Start Date : `2002-10-13` (optional, string)
+ Period of Performance Current End Date : `2003-04-01` (optional, string)
+ Description : `description` (optional, string)
+ Last Modified Date : `2002-12-18` (optional, string)
+ Base Obligation Date : `2003-04-01` (optional, string)
+ Award ID : `N0001902C3002`  (optional, string)
+ Start Date : `2001-10-26` (optional, string)
+ End Date : `2019-07-31` (optional, string)
+ Last Date to Order: `2022-08-12` (optional, string)
+ Award Amount : 1573663  (optional, number)
+ Award Type : `IDV_C` (optional, string)
+ Contract Award Type: `Federal Supply Schedule` (optional, string)
+ SAI Number : `4.5435` (optional, array[string])
+ CFDA Number : `10.553` (optional, array[string])
+ Issued Date : `2018-09-11` (optional, string)
+ Loan Value : 26358334512 (optional, number)
+ Subsidy Cost : 3000186413 (optional, number)
+ internal_id : 1018950  (optional, number)

## AwardTypeResult (object)
+ grants : 1 (required, number)
+ loans : 1 (required, number)
+ contracts : 1 (required, number)
+ direct_payments : 1 (required, number)
+ other : 1 (required, number)
+ idvs : 1 (required, number)

## CategoryResult (object)
+ id: 1 (required, number)
    The `id` is the database key.
+ name: Aircraft Manufacturing (required, string)
+ code: 336411 (optional, string)
    `code` is a user-displayable code (such as a program activity or NAICS code, but **not** a database ID). When no such code is relevant, return a `null`.
+ amount: 591230394.12 (required, number)

## TimeResult (object)
+ time_period (required, TimePeriodGroup)
+ aggregated_amount: 200000000 (required, number)
    The aggregate award amount for this time period and the given filters.

## PageMetadataObject (object)
+ page: 1 (required, number)
+ hasNext: false (required, boolean)

## FilterObject (object)
+ `keywords` : poptarts (optional, array[string])
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope`: `domestic` (optional, enum[string])
    + domestic
    + foreign
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject])
+ `recipient_search_text`: `Hampton` (optional, array[string])
+ `recipient_id` (optional, string)
    A hash of recipient DUNS, name, and level. A unique identifier for recipients, used for profile page urls.
+ `recipient_scope`: domestic (optional, enum[string])
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

## TimePeriodObject (object)
+ start_date: `2017-10-01` (required, string)
+ end_date: `2018-09-30` (required, string)
+ `date_type`: `action_date` (optional, enum[string])
    + action_date
    + last_modified_date

## TimePeriodGroup (object)
+ fiscal_year: `2018` (required, string)
+ quarter: `1` (optional, string)
    Excluded when grouping by `fiscal_year` or `month`.
+ month: `1` (optional, string)
    Excluded when grouping by `fiscal_year` or `quarter`.

## LocationObject (object)
+ country: USA (required, string)
+ state: VA (optional, string)
+ county: 059 (optional, string)
+ city: DC (optional, string)

## AgencyObject (object)
+ type: awarding (required, enum[string])
    + awarding
    + funding
+ tier: toptier (required, enum[string])
    + `toptier`
    + `subtier`
+ name: `Department of Defense` (required, string)

## AwardAmounts (object)
+ lower_bound (optional, number)
+ upper_bound: 1000000 (optional, number)