FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By Funding Subagency [/api/v2/search/spending_by_category/funding_subagency/]

This endpoint supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

This endpoint returns a list of the top results of Funding Subagencies sorted by the total amounts in descending order.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, FilterObject)
            The filters to find with said category
        + `limit`: 5 (optional, number)
            The number of results to include per page
        + `page`: 1 (optional, number)
            The page of results to return based on the limit
        + `subawards` (optional, boolean)
            Determines whether Prime Awards or Sub Awards are searched

+ Response 200 (application/json)
    + Attributes (object)
        + `category`: `funding_subagency` (required, string)
        + `results` (required, array[CategoryResult], fixed-type)
        + `limit`: 10 (required, number)
        + `page_metadata` (PageMetadataObject)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "category": "funding_subagency",
                "limit": 10,
                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": false,
                    "hasPrevious": false
                },
                "results": [
                    {
                        "amount": 440219861071.72,
                        "name": "Centers for Medicare and Medicaid Services",
                        "code": "CMS",
                        "id": 831
                    },
                    {
                        "amount": 284429830939.4,
                        "name": "Social Security Administration",
                        "code": "SSA",
                        "id": 539
                    }
                ],
                "messages": [
                    "For searches, time period start and end dates are currently limited to an earliest date of 2007-10-01.  For data going back to 2000-10-01, use either the Custom Award Download feature on the website or one of our download or bulk_download API endpoints as listed on https://api.usaspending.gov/docs/endpoints."
                ]
            }

# Data Structures

## CategoryResult (object)
+ `id` (required, number)
    The id is the database key.
+ `name` (required, string, nullable)
+ `code` (required, string, nullable)
    `code` is a user-displayable code (such as a program activity or NAICS code, but **not** a database ID). When no such code is relevant, return a `null`.
+ `amount` (required, number)

## PageMetadataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)

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
    A unique identifier for the recipient which includes the recipient hash and level.
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
+ `psc_codes`: `8940`, `8910` (optional, array[string])
+ `contract_pricing_type_codes`: `J` (optional, array[string])
+ `set_aside_type_codes`: `NONE` (optional, array[string])
+ `extent_competed_type_codes`: `A` (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)

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
+ `name`: `Office of Inspector General` (required, string)
+ `toptier_name`: `Department of the Treasury` (optional, string)
    Only applicable when `tier` is `subtier`.  Ignored when `tier` is `toptier`.  Provides a means by which to scope subtiers with common names to a
    specific toptier.  For example, several agencies have an "Office of Inspector General".  If not provided, subtiers may span more than one toptier.

### AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound`: 1000000 (optional, number)

### NAICSCodeObject (object)
+ `require`: `33` (optional, array[string])
+ `exclude`: `3333` (optional, array[string])

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
