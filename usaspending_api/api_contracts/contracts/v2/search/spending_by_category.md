FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By Category [/api/v2/search/spending_by_category/]

This endpoint supports the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

This endpoint returns a list of the top results of specific categories sorted by the total amounts in descending order.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `category` (required, enum[string])
            + Members
                + `awarding_agency`
                + `awarding_subagency`
                + `cfda`
                + `country`
                + `county`
                + `district`
                + `federal_account`
                + `funding_agency`
                + `funding_subagency`
                + `naics`
                + `object_class`
                + `program_activity`
                + `psc`
                + `recipient_duns`
                + `recipient_parent_duns`
                + `state_territory`
                + `tas`
        + `filters` (required, AdvancedFilterObject)
            The filters to find with said category
        + `limit` (optional, number)
            The number of results to include per page
        + `page` (optional, number)
            The page of results to return based on the limit
    + Body

            
            {
                "category": "awarding_agency",
                "filters": {
                    "keywords": ["Filter is required"]
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `category` (required, string)
        + `results` (required, array[CategoryResult], fixed-type)
        + `limit` (required, number)
        + `page_metadata` (PageMetadataObject)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

# Data Structures

## CategoryResult (object)
+ `id` (required, number)
    The id is the database key.
+ `recipient_id` (optional, string, nullable)
    A unique identifier for the recipient which includes the recipient hash and level.
+ `name` (required, string, nullable)
+ `code` (required, string, nullable)
    `code` is a user-displayable code (such as a program activity or NAICS code, but **not** a database ID). When no such code is relevant, return a `null`.
+ `amount` (required, number)

## PageMetadataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)

## Filter Objects
### AdvancedFilterObject (object)
+ `keywords` (optional, array[string])
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text` (optional, array[string])
+ `recipient_id` (optional, string)
    A unique identifier for the recipient which includes the recipient hash and level.
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names` (optional, array[string])
+ `award_type_codes` (optional, FilterObjectAwardTypes)
+ `award_ids` (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers` (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `contract_pricing_type_codes` (optional, array[string])
+ `set_aside_type_codes` (optional, array[string])
+ `extent_competed_type_codes` (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)

### TimePeriodObject (object)
+ `start_date` (required, string)
    Currently limited to an earliest date of `2007-10-01` (FY2008).  For data going back to `2000-10-01` (FY2001), use either the Custom Award Download
    feature on the website or one of our `download` or `bulk_download` API endpoints.
+ `end_date` (required, string)
    Currently limited to an earliest date of `2007-10-01` (FY2008).  For data going back to `2000-10-01` (FY2001), use either the Custom Award Download
    feature on the website or one of our `download` or `bulk_download` API endpoints.
+ `date_type` (optional, enum[string])
    + Members
        + `action_date`
        + `last_modified_date`

### LocationObject (object)
+ `country` (required, string)
+ `state` (optional, string)
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
