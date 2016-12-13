# The USAspending Application Programming Interface (API)

The USAspending API allows the public to accesss data published via the Broker or USAspending.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Table of Contents
  * [Status Codes](#status-codes)
  * [Data Routes](#data-routes)
    * [Routes and Methods](#routes-and-methods)
  * [GET Requests](#get-requests)
  * [POST Requests](#post-requests)
  * [Autocomplete Queries](#autocomplete-queries)
  * [Geographical Hierarchy Queries](#geographical-hierarchy-queries)


## DATA Act Data Store Route Documentation

Routes do not currently require any authorization

### Status Codes
In general, status codes returned are as follows:

* 200 if successful
* 400 if the request is malformed
* 500 for server-side errors

### Data Routes

Data routes are split by payload into POST and GET methods. In general, the format of a request and response will remain the same with the route only changing the data provided in the response. Some routes support aggregate metadata (such as the sum of a certain column for all entries in a dataset) and will be noted in the route listing.

#### Routes and Methods
The currently available routes are:
  * **/v1/awards/summary/**
    - _Description_: Provides award level summary data
    - _Methods_: GET, POST
    - _Metadata_: Provides `total_obligation_sum`, a sum of the `total_obligation` for all data in the dataset

  * **/v1/awards/summary/autocomplete/**
    - _Description_: Provides a fast endpoint for evaluating autocomplete queries against the awards/summary endpoint
    - _Methods_: POST

  * **/v1/references/locations/**
    - _Description_: Returns all `Location` data.
    - _Methods_: POST

  * **/v1/references/locations/geocomplete**
    - _Description_: A structured hierarchy geographical autocomplete. See [Geographical Hierarchy Queries](#geographical-hierarchy-queries) for more information
    - _Methods_: POST

  * **/v1/awards/**
    - _Description_: Returns all `FinancialAccountsByAwardsTransactionObligations` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET

  * **/v1/accounts/**
    - _Description_: Returns all `AppropriationAccountBalances` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET

  * **/v1/accounts/tas/**
    - _Description_: Returns all `TreasuryAppropriationAccount` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET

  * **/v1/financial_activities/**
    - _Description_: Returns all `FinancialAccountsByProgramActivityObjectClass` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET

  * **/v1/submissions/**
    - _Description_: Returns all `SubmissionAttributes` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET

#### GET Requests
GET requests can be specified by attaching any field value pair to the route. This method supports any fields present in the data object and only the `equals` operation. It also supports pagination variables. Additionally, you may specifcy complex fields that use Django's foreign key traversal; for more details on this see `field` from the POST request. Examples below:

`/v1/awards/summary/?page=5&limit=1000`

`/v1/awards/summary/?funding_agency__fpds_code=0300`

#### POST Requests
The structure of the post request allows for a flexible and complex query with built-in pagination support.

#### Body (JSON)

```
{
    "page": 1,
    "limit": 1000,
    "fields": ["fain", "total_obligation"],
    "unique_values": ["recipient__location__location_state_code", "awarding_agency__name"],
    "exclude": ["recipient"]
    "filters": [
      {
        "field": "fain",
        "operation": "equals",
        "value": "12-34-56"
      },
      {
        "combine_method": "OR",
        "filters": [ . . . ]
      }
    ]
}
```

#### Body Description

* `page` - _Optional_ - If your request requires pagination, this parameter specifies the page of results to return. Default: 1
* `limit` - _Optional_ - The maximum length of a page in the response. Default: 100
* `unique_values` - _Optional_ - A list of fields for which you would like to know the unique values and how many items have that value. These are processed _after_ the filters. An example response with that value would be:
  ```
  {
    "unique_values_metadata": {
      "awarding_agency__name": {
        "SMALL BUSINESS ADMINISTRATION": 5,
        "DEPARTMENTAL OFFICES": 3,
        "null": 4
      },
      "recipient__location__location_state_code": {
        "SD": 1,
        "FL": 2,
        "MN": 1,
        "UT": 1,
        "TX": 2,
        "VA": 1,
        "CA": 1,
        "NC": 1,
        "LA": 1,
        "GA": 1
      }
    },
    "results": [ . . . ],
    "total_metadata": { . . . }
  }
  ```
In this case, two entires matching the specified filter have the state code of `FL`.
* `fields` - _Optional_ - What fields to return. Must be a list. Omitting this will return all fields.
* `exclude` - _Optional_ - What fields to exclude from the return. Must be a list.
* `filters` - _Optional_ - An array of objects specifying how to filter the dataset. When multiple filters are specified in the root list, they will be joined via _and_
  * `field` - A string specifying the field to compare the value to. This supports Django's foreign key relationship traversal; therefore, `funding_agency__fpds_code` will filter on the field `fpds_code` for the referenced object stored in `funding_agency`.
  * `operation` - The operation to use to compare the field to the value. Some operations place requirements upon the data type in the values parameter, noted below. The options for this field are:
    * `equals` - Evaluates the equality of the value with that stored in the field
      ```
      {
        "field": "fain",
        "operation": "equals",
        "value": "1234567"
      }
      ```
    * `less_than` - Evaluates whether the value stored in the field is less than the value specified in the filter
    ```
    {
      "field": "total_obligation",
      "operation": "less_than",
      "value": 2000
    }
    ```
    * `less_than_or_equal` - As `less_than`, but inclusive
    ```
    {
      "field": "total_obligation",
      "operation": "less_than_or_equal",
      "value": 100000
    }
    ```
    * `greater_than` - Evaluates whether the value stored in the field is greater than the value specified in the filter
    ```
    {
      "field": "total_obligation",
      "operation": "greater_than",
      "value": 3.50
    }
    ```
    * `greater_than_or_equal` - As `greater_than`, but inclusive
    ```
    {
      "field": "total_obligation",
      "operation": "greater_than_or_equal",
      "value": 20
    }
    ```
    * `in` - Evaluates if the value stored in the field is any of the values specified in the value parameter. `value` must be an array of values
    ```
    {
      "field": "recipient__name",
      "operation": "in",
      "value": [
          "DEPARTMENT OF LABOR",
          "HOUSING AND URBAN DEVELOPMENT",
          "LEXCORP"
        ]
    }
    ```
    * `range` - Evaluates if the value stored in the field is in the range defined by a list specified in the value parameter. `value` must be an array of length two. The first value in the array will be treated as the start of the range.
    ```
    {
      "field": "total_obligation",
      "operation": "range",
      "value": [250000, 1000000]
    }
    ```
    * `contains` - A case-insensitive containment test
    ```
    {
      "field": "recipient__name",
      "operation": "contains",
      "value": "BOEING"
    }
    ```
    * `is_null` - Evaluates if the field is null or not null. `value` must be either `true` or `false`
    ```
    {
      "field": "awarding_agency",
      "operation": "is_null",
      "value": false
    }
    ```
    * `search` - Executes a full text search on the specified field or fields
    ```
    {
      "field": "awarding_agency__name",
      "operation": "search",
      "value": "congress"
    }
    ```
    **_or_**
    ```
    {
      "field": ["awarding_agency__name", "recipient__name"]
      "operation": "search",
      "value": "treasury"
    }
    ```
    * `fy` - Evaluates if the field (generally should be a datetime field) falls within the federal fiscal year specified as `value`. `value` should be a 4-digit integer specifying the fiscal year. An example of a fiscal year is FY 2017 which runs from October 1st 2016 to September 30th 2017. Does not need `value_format` as it is assumed.
    ```
    {
      "field": "date_signed",
      "operation": "fy",
      "value": 2017
    }
    ```
    * `range_intersect` - Evaluates if the range defined by a two-field list intersects with the range defined
    by the two length array `value`. `value` can be a single item _only_ if `value_format` is also set to a
    range converting value. An example of where this is useful is when a contract spans multiple fiscal years, to evaluate whether it overlaps with any one particular fiscal year - that is, the range defined by `period_of_performance_start` to `period_of_performance_end` intersects with the fiscal year.

    For example, if your `field` parameter defines a range as `[3,5]` then the following ranges will intersect:
      * `[2,3]` - As the 3 overlaps
      * `[4,4]` - As the entire range is contained within another
      * `[0,100]` - As the entire range is contained within another
      * `[5,10]` - As the 5 overlaps

    Mathematically speaking, ranges will intersect as long as there exists some value `C` such that `r1 <= C <= r2` and `f1 <= C <= f2`
    ```
    {
      "field": ["create_date", "update_date"],
      "operation": "range_intersect",
      "value": ["2016-11-01", "2016-11-02"]
    }
    ```
    **_or_**
    ```
    {
      "field": ["create_date", "update_date"],
      "operation": "range_intersect",
      "value": 2017,
      "value_format": "fy"
    }
    ```
  * `value` - Specifies the value to compare the field against. Some operations require specific datatypes for the value, and they are documented in the `operation` section.
  * `value_format` - Specifies the format for the value. Only used in some operations where noted. Valid choices are enumerated below
    * `fy` - Treats a single value as a fiscal year range
  * `combine_method` - This is a special field which modifies how the filter behaves. When `combine_method` is specified, the only other allowed parameter on the filter is `filters` which should contain an array of filter objects. The `combine_method` will be used to logically join the filters in this list. Options are `AND` or `OR`.
  ```
  {
			"combine_method": "OR",
			"filters": [
				{
					"field": "funding_agency__fpds_code",
					"operation": "equals",
					"value": "0300"

				},
				{
					"field": "awarding_agency__fpds_code",
					"operation": "in",
					"value": ["0300", "0500"]

				}
				]

	}
  ```

#### Response (JSON)
The response object structure is the same whether you are making a GET or a POST request. The only difference is the data objects contained within the results parameter. An example of a response from `/v1/awards/summary/` can be found below

```
{
  "page_metadata": {
    "total_obligation_sum": -6000, // This is a meta-data field specific to the endpoint
    "num_pages": 499,
    "page_number": 1,
    "count": 1
  },
  "total_metadata": {
    "total_obligation_sum": 13459752.5, // Again, this is a meta-data field specific to the endpoint but applied to the entire dataset
    "count": 499
  },
  "results": [
    {
      "id": 4,
      "recipient": null,
      "awarding_agency": null,
      "funding_agency": null,
      "procurement_set": [
        {
          "procurement_id": 4,
          "action_date": "2016-09-08",
          "action_type": null,
          "federal_action_obligation": "-6000.00",
          "modification_number": "M0001",
          "award_description": null,
          "drv_award_transaction_usaspend": null,
          "drv_current_total_award_value_amount_adjustment": null,
          "drv_potential_total_award_value_amount_adjustment": null,
          "piid": "TEPS111408",
          "parent_award_id": null,
          "cost_or_pricing_data": null,
          "type_of_contract_pricing": null,
          "contract_award_type": null,
          "naics": null,
          "naics_description": null,
          "period_of_performance_potential_end_date": null,
          "ordering_period_end_date": null,
          "current_total_value_award": null,
          "potential_total_value_of_award": null,
          "referenced_idv_agency_identifier": null,
          "idv_type": null,
          "multiple_or_single_award_idv": null,
          "type_of_idc": null,
          "a76_fair_act_action": null,
          "dod_claimant_program_code": null,
          "clinger_cohen_act_planning": null,
          "commercial_item_acquisition_procedures": null,
          "commercial_item_test_program": null,
          "consolidated_contract": null,
          "contingency_humanitarian_or_peacekeeping_operation": null,
          "contract_bundling": null,
          "contract_financing": null,
          "contracting_officers_determination_of_business_size": null,
          "cost_accounting_standards": null,
          "country_of_product_or_service_origin": null,
          "davis_bacon_act": null,
          "evaluated_preference": null,
          "extent_competed": null,
          "fed_biz_opps": null,
          "foreign_funding": null,
          "gfe_gfp": null,
          "information_technology_commercial_item_category": null,
          "interagency_contracting_authority": null,
          "local_area_set_aside": null,
          "major_program": null,
          "purchase_card_as_payment_method": null,
          "multi_year_contract": null,
          "national_interest_action": null,
          "number_of_actions": null,
          "number_of_offers_received": null,
          "other_statutory_authority": null,
          "performance_based_service_acquisition": null,
          "place_of_manufacture": null,
          "price_evaluation_adjustment_preference_percent_difference": null,
          "product_or_service_code": null,
          "program_acronym": null,
          "other_than_full_and_open_competition": null,
          "recovered_materials_sustainability": null,
          "research": null,
          "sea_transportation": null,
          "service_contract_act": null,
          "small_business_competitiveness_demonstration_program": null,
          "solicitation_identifier": null,
          "solicitation_procedures": null,
          "fair_opportunity_limited_sources": null,
          "subcontracting_plan": null,
          "program_system_or_equipment_code": null,
          "type_set_aside": null,
          "epa_designated_product": null,
          "walsh_healey_act": null,
          "transaction_number": null,
          "referenced_idv_modification_number": null,
          "rec_flag": null,
          "drv_parent_award_awarding_agency_code": null,
          "drv_current_aggregated_total_value_of_award": null,
          "drv_current_total_value_of_award": null,
          "drv_potential_award_idv_amount_total_estimate": null,
          "drv_potential_aggregated_award_idv_amount_total_estimate": null,
          "drv_potential_aggregated_total_value_of_award": null,
          "drv_potential_total_value_of_award": null,
          "create_date": "2016-10-13T17:57:54.834457Z",
          "update_date": "2016-10-13T18:04:04.433041Z",
          "last_modified_date": null,
          "certified_date": null,
          "reporting_period_start": null,
          "reporting_period_end": null,
          "awarding_agency": 284,
          "recipient": 4,
          "award": 4,
          "submission": 1
        }
      ],
      "financialassistanceaward_set": [],
      "type": "C",
      "piid": "TEPS111408",
      "parent_award_id": null,
      "fain": null,
      "uri": null,
      "total_obligation": "-6000.00",
      "total_outlay": null,
      "date_signed": "2016-09-08",
      "description": null,
      "period_of_performance_start_date": null,
      "period_of_performance_current_end_date": null,
      "last_modified_date": null,
      "certified_date": null,
      "create_date": "2016-10-13T17:57:54.829889Z",
      "update_date": "2016-10-13T18:04:04.435900Z",
      "place_of_performance": null,
      "latest_submission": null
    }
  ]
}
```

### Response Description
The response has three functional parts:
  * `page_metadata` - Includes data about the pagination and any page-level metadata specific to the endpoint
    * `page_number` - What page is currently being returned
    * `num_page` - The number of pages available for this set of filters
    * `count` - The length of the `results` array for this page
  * `total_metadata` - Includes data about the total dataset and any dataset-level metadata specific to the endpoint
    * `count` - The total number of items in this dataset, spanning all pages
  * `results` - An array of objects corresponding to the data returned by the specified route. Will _always_ be an array, even if the number of results is only one.

### Autocomplete Queries
Autocomplete queries currently require the endpoint to have additional handling, as such, only a few have been implemented (notably awards/summary).
#### Body
```
{
	"fields": ["recipient__location__location_state_code", "recipient__location__location_state_name"],
	"value": "a",
	"mode": "contains"
}
```
#### Body Description
  * `fields` - A list of fields to be searched for autocomplete. This allows for foreign key traversal using the usual Django patterns. This should _always_ be a list, even if the length is only one
  * `value` - The value to use as the autocomplete pattern. Typically a string, but could be a number in uncommon circumstances. The search will currently _always_ be case insensitive
  * `mode` - _Optional_ - The search mode. Options available are:
    * `contains` - Matches if the field's value contains the specified value
    * `startswith` - Matches if the field's value starts with the specified value

#### Response
```
{
  "results": {
    "recipient__location__location_state_name": [
      "Texas",
      "California",
      "Georgia"
    ],
    "recipient__location__location_state_code": [
      "CA",
      "GA"
    ]
  },
  "counts": {
    "recipient__location__location_state_name": 3,
    "recipient__location__location_state_code": 2
  }
}
```
#### Response Description
  * `results` - The actual results. For each field search, will contain a list of all unique values matching the requested value and mode
  * `counts` - Contains the length of each array in the results object

### Geographical Hierarchy Queries
This is a special type of autocomplete query which allows users to search for geographical locations in a hierarchy.

#### Body
```
{
  "value": "u",
  "mode": "startswith",
  "scope": "domestic"
}
```

#### Body Description
  * `value` - The value to use as the autocomplete pattern. The search will currently _always_ be case insensitive
  * `mode` - _Optional_ -The search mode. Options available are:
    * `contains` - Matches if the field's value contains the specified value. This is the default behavior
    * `startswith` - Matches if the field's value starts with the specified value
  * `scope` - _Optional_ - The scope of the search. Options available are:
    * `domestic` - Matches only entries with the United States as the `location_country_code`
    * `foreign` - Matches only entries where the `location_country_code` is _not_ the United States
    * `all` - Matches any location entry. This is the default behavior

#### Response
```
[
  {
    "place_type": "STATE",
    "parent": "UNITED STATES",
    "matched_ids": [
      9,
      10
    ],
    "place": "Utah"
  },
  {
    "place_type": "COUNTRY",
    "parent": "USA",
    "matched_ids": [
      7,
      5,
      3,
      9,
      1,
      8,
      6,
      4,
      10,
      2
    ],
    "place": "UNITED STATES"
  },
  {
    "place_type": "STATE",
    "parent": "UNITED STATES",
    "matched_ids": [
      9
    ],
    "place": "UT"
  }
  ```
#### Response Description
  * `place` - The value of the place. e.g. A country's name, or a county name, etc.
  * `matched_ids` - An array of `location_id`s that match the given data. This can be used to look up awards, recipients, or other data by requesting these ids
  * `place_type` - The type of place. Options are:
    * `COUNTRY`
    * `CITY`
    * `COUNTY`
    * `STATE`
    * `ZIP`
    * `POSTAL CODE` - Used for foreign postal codes
    * `PROVINCE`
  * `parent` - The parent of the object, in a logical hierarchy. The parents for each type are listed below:
    * `COUNTRY` - Will specify the parent as the country code for reference purposes
    * `CITY` - Will specify the county the city is in for domestic cities, or the country for foreign cities
    * `COUNTY` - Will specify the state the the city is in for domestic cities
    * `STATE` - Will specify the country the state is in
    * `ZIP` - Will specify the state the zip code falls in. If a zip code falls in multiple states, two results will be generated
    * `POSTAL CODE` - Will specify the country the postal code falls in
    * `PROVINCE` - Will specify the country the province is in
