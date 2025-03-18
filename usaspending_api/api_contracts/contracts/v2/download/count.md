FORMAT: 1A
HOST: https://api.usaspending.gov

# Download Count [/api/v2/download/count/]

These endpoints support the advanced search page and allow for complex filtering for specific subsets of spending data.

## POST

Returns the number of transactions that would be included in a download request for the given filter set.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
        + `spending_level` (optional, enum[string])
            + Members
                + `subawards`
                + `transactions`
                + `awards`
            + Default: `transactions`
    + Body


            {
                "filters": {
                    "keywords": ["cheese"],
                    "time_period": [
                        {
                            "start_date": "2019-10-01",
                            "end_date": "2020-09-30"
                        },
                        {
                            "start_date": "2010-10-01",
                            "end_date": "2011-09-30"
                        }
                    ],
                    "recipient_type_names": ["higher_education"],
                    "place_of_performance_locations": [{"country": "USA", "state": "WI"}],
                    "psc_codes": {"require": [["Product","10","1035"]]}
                },
                "spending_level": "awards"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `calculated_transaction_count` (required, number)
            The calculated count of all transactions which would be included in the download files.
        + `maximum_transaction_limit` (required, number)
            The current allowed maximum number of transactions in a row-limited download. Visit https://www.usaspending.gov/download_center/custom_award_data to download larger volumes of data.
        + `transaction_rows_gt_limit` (required, boolean)
            A boolean returning whether the transaction count is over the maximum row limit.
        + `calculated_count` (required, number)
            The calculated count of Awards, Transactions, or Subawards included in the download files.
        + `spending_level` (required, string)
            The spending_level provided by the user or the default value of transactions.
        + `maximum_limit` (required, number)
            The current allowed maximum number of Awards, Transactions, or Subawards in a row-limited download. Visit https://www.usaspending.gov/download_center/custom_award_data to download larger volumes of data.
        + `rows_gt_limit` (required, boolean)
            A boolean returning whether the Awards, Transactions, or Subawards count is over the maximum row limit.
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body


            {
                "calculated_transaction_count": 87343,
                "maximum_transaction_limit": 500000,
                "transaction_rows_gt_limit": false,
                "calculated_count": 87343,
                "spending_level": "awards",
                "maximum_limit": 500000,
                "rows_gt_limit": false,
                "messages": ["For searches, time period start and end dates are currently limited to an earliest date of 2007-10-01.  For data going back to 2000-10-01, use either the Custom Award Download feature on the website or one of our download or bulk_download API endpoints as listed on https://api.usaspending.gov/docs/endpoints."]
            }

# Data Structures

## AdvancedFilterObject (object)
+ `keywords`: `transport` (optional, array[string])
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text`: `Hampton` (optional, array[string])
+ `recipient_id` (optional, string)
    A hash of recipient UEI, DUNS, name, and level. A unique identifier for recipients, used for profile page urls. This filter is not supported by subawards.
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names`: `category_business` (optional, array[string])
+ `award_type_codes` (optional, FilterObjectAwardTypes)
+ `award_ids`: [`SPE30018FLGFZ`, `SPE30018FLJFN`]  (optional, array[string])
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

## TimePeriodObject (object)
See the Transaction Search category defined in [TransactionSearchTimePeriodObject](../../../search_filters.md#transaction-search-time-period-object)

## LocationObject (object)
These fields are defined in the [StandardLocationObject](../../../search_filters.md#standard-location-object)

## AgencyObject (object)
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

## AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound`: 1000000 (optional, number)

### NAICSCodeObject (object)
+ `require`: `33` (optional, array[string], fixed-type)
+ `exclude`: `3333` (optional, array[string], fixed-type)

### PSCCodeObject (object)
+ `require`: [[`Service`, `B`, `B5`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`Service`, `B`, `B5`, `B502`]] (optional, array[array[string]], fixed-type)

### TASCodeObject (object)
+ `require`: [[`091`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`091`, `091-0800`]] (optional, array[array[string]], fixed-type)

### TreasuryAccountComponentsObject (object)
+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier - three characters
+ `aid`: `090` (required, string)
    Agency Identifier - three characters
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability - four digits
+ `epoa` (optional, string, nullable)
    Ending Period of Availability - four digits
+ `a` (optional, string, nullable)
    Availability Type Code - X or null
+ `main`: `1000` (required, string)
    Main Account Code - four digits
+ `sub` (optional, string, nullable)
    Sub-Account Code - three digits

## FilterObjectAwardTypes (array)
List of filterable award types

### Sample
- "A"
- "B"
- "C"
- "D"

### Default
- "02"
- "03"
- "04"
- "05"
- "06"
- "07"
- "08"
- "09"
- "10"
- "11"
- "A"
- "B"
- "C"
- "D"
- "IDV_A"
- "IDV_B"
- "IDV_B_A"
- "IDV_B_B"
- "IDV_B_C"
- "IDV_C"
- "IDV_D"
- "IDV_E"
- "-1"
