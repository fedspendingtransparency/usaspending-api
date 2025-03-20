FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By County [/api/v2/search/spending_by_category/county/]

This endpoint supports the state profile page and allow for complex filtering for specific subsets of spending data.

## POST

This endpoint returns a list of the top results of Counties sorted by the total amounts in descending order.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
            The filters to find with said category
        + `limit`: 5 (optional, number)
            The number of results to include per page
        + `page`: 1 (optional, number)
            The page of results to return based on the limit
        + `spending_level` (optional, enum[string])
            Group the spending by level. This also determines what data source is used for the totals.
            + Members
                + `transactions`
                + `awards`
                + `subawards`
            + Default
                + `transactions`
        + `subawards` (optional, boolean)
            Determines whether Prime Awards or Sub Awards are searched. This field will be depreciated soon.
    + Body


            {
                "filters": {
                    "recipient_id": "1c3edaaa-611b-840c-bf2b-fd34df49f21f-P",
                    "time_period": [
                        {
                            "start_date": "2019-09-28",
                            "end_date": "2020-09-28"
                        }
                    ]
                },
                "category": "county",
                "limit": 5,
                "page": 1,
                "spending_level": "transactions"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `category`: `county` (required, string)
        + `spending_level` (required, enum[string])
            Spending level value that was provided in the request.
            + Members
                + `transactions`
                + `awards`
                + `subawards`
        + `results` (required, array[CategoryResult], fixed-type)
        + `limit`: 10 (required, number)
        + `page_metadata` (PageMetadataObject)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body


            {
                "category": "county",
                "spending_level": "transactions",
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
                        "amount": 17557452350.05,
                        "code": "439",
                        "name": "TARRANT",
                        "id": null,
                        "total_outlays": null,
                    },
                    {
                        "amount": 5412138054.37,
                        "code": "079",
                        "name": "RICHLAND",
                        "id": null,
                        "total_outlays": null,
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
+ `total_outlays` (required, number, nullable)

## PageMetadataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)

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
+ `recipient_search_text`: [`Hampton`] (optional, array[string])
+ `recipient_id` (optional, string)
    A unique identifier for the recipient which includes the recipient hash and level. This filter is not supported by subawards.
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names`: [`category_business`] (optional, array[string])
+ `award_type_codes` (optional, FilterObjectAwardTypes)
+ `award_ids`: [`SPE30018FLGFZ`, `SPE30018FLJFN`] (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers`: [`10.331`] (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `contract_pricing_type_codes`: [`J`] (optional, array[string])
+ `set_aside_type_codes`: [`NONE`] (optional, array[string])
+ `extent_competed_type_codes`: [`A`] (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)
+ `def_codes` (optional, array[DEFC], fixed-type)
  If the `def_codes` provided are in the COVID-19 or IIJA group, the query will only return results of transactions where the `action_date` is on or after the enactment date of the public law associated with that disaster code.

### TimePeriodObject (object)
This TimePeriodObject can fall into different categories based on the request.
+ if `subawards` true

    See the Subaward Search category defined in [SubawardSearchTimePeriodObject](../../../search_filters.md#subaward-search-time-period-object)

+ otherwise

    See the Transaction Search category defined in [TransactionSearchTimePeriodObject](../../../search_filters.md#transaction-search-time-period-object)

### LocationObject (object)
These fields are defined in the [StandardLocationObject](../../../../search_filters.md#standard-location-object)

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

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

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
