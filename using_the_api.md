# The USAspending Application Programming Interface (API)

The USAspending API allows the public to access data published via the Broker or USAspending.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Table of Contents
  * [Status Codes](#status-codes)
  * [Data Endpoints](#data-endpoints)
    * [Endpoints and Methods](#endpoints-and-methods)
    * [Summary Endpoints and Methods](#summary-endpoints-and-methods)
  * [GET Requests](#get-requests)
  * [POST Requests](#post-requests)
  * [Autocomplete Queries](#autocomplete-queries)
  * [Geographical Hierarchy Queries](#geographical-hierarchy-queries)


## DATA Act Data Store Endpoint Documentation

Endpoints do not currently require any authorization

### Status Codes
In general, status codes returned are as follows:

* 200 if successful
* 400 if the request is malformed
* 500 for server-side errors

### Data Endpoints

Data endpoints are split by payload into POST and GET methods. In general, the format of a request and response will remain the same with the endpoint only changing the data provided in the response.

#### Endpoints and Methods
The currently available endpoints are:
  * **[/v1/accounts/](https://api.usaspending.gov/api/v1/accounts/)**
    - _Description_: Returns all `AppropriationAccountBalances` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET


  * **[/v1/accounts/tas/](https://api.usaspending.gov/api/v1/accounts/tas/)**
    - _Description_: Returns all `TreasuryAppropriationAccount` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET


  * **[/v1/awards/](https://api.usaspending.gov/api/v1/awards/)**
    - _Description_: Returns all `FinancialAccountsByAwardsTransactionObligations` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET


  * **[/v1/awards/summary/](https://api.usaspending.gov/api/v1/awards/summary/)**
    - _Description_: Provides award level summary data
    - _Methods_: GET, POST


  * **[/v1/awards/summary/autocomplete/](https://api.usaspending.gov/api/v1/awards/summary/autocomplete/)**
      - _Description_: Provides a fast endpoint for evaluating autocomplete queries against the awards/summary endpoint
    - _Methods_: POST


  * **[/v1/transactions/](https://api.usaspending.gov/api/v1/awards/transactions/)**
    - _Description_: Provides award transactions data **Note:** This endpoint is under active development and currently serves contract data only
    - _Methods_: POST


  * **[/v1/references/locations/](https://api.usaspending.gov/api/v1/references/locations/)**
    - _Description_: Returns all `Location` data.
    - _Methods_: POST


  * **[/v1/references/locations/geocomplete](https://api.usaspending.gov/api/v1/references/locations/geocomplete/)**
    - _Description_: A structured hierarchy geographical autocomplete. See [Geographical Hierarchy Queries](#geographical-hierarchy-queries) for more information
    - _Methods_: POST


  * **[/v1/references/agency/](https://api.usaspending.gov/api/v1/references/agency/)**
    - _Description_: Provides agency data
    - _Methods_: POST


  * **[/v1/references/agency/autocomplete/](https://api.usaspending.gov/api/v1/references/agency/autocomplete/)**
    - _Description_: Provides a fast endpoint for evaluating autocomplete queries against the agency endpoint
    - _Methods_: POST


  * **[/v1/financial_activities/](https://api.usaspending.gov/api/v1/financial_activities/)**
    - _Description_: Returns all `FinancialAccountsByProgramActivityObjectClass` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET


  * **[/v1/submissions/](https://api.usaspending.gov/api/v1/submissions/)**
    - _Description_: Returns all `SubmissionAttributes` data. _NB_: This endpoint is due for a rework in the near future
    - _Methods_: GET

#### Summary Endpoints and Methods
Summarized data is available for some of the endpoints listed above:

* **[/v1/awards/total/](https://api.usaspending.gov/api/v1/awards/total/)**
* **[/v1/transactions/total/](https://api.usaspending.gov/api/v1/transactions/total/)**
* more coming soon

You can get summarized data via a `POST` request that specifies:

* `field`: the field to be summarized
* `aggregate`: the aggregate function to use when summarizing the data (defaults to `sum`; `avg`, `count`, `min`, and `max` are also supported)
* `group`: the field to group by (optional; if not specified, data will be summarized across all objects)
* `date_part`: applies only when `group` is a data field and specifies which part of the date to group by; `year`, `month`, and `day` are currently supported, and `quarter` is coming soon

Requests to the summary endpoints can also contain the `page`, `limit`, and `filters` parameters as described in [POST Requests](#post-requests). **Note:** If you're filtering the data, the filters are applied before the data is summarized.

The `results` portion of the response will contain:

* `item`: the value of the field in the request's `group` parameter (if the request did not supply `group`, `item` will not be included)
* `aggregate`: the summarized data

For example, to request the yearly sum of obligated dollars across transactions for award types "B" and "C" (_i.e._, purchase orders and delivery orders):

```json
{
    "field": "federal_action_obligation",
    "group": "action_date",
    "date_part": "year",
    "aggregate": "sum",
    "filters": [
        {
            "field": "type",
            "operation": "in",
            "value": ["A", "B", "C", "D"]
        }
     ]
}
```

Response:

```json
{
  "total_metadata": {
    "count": 2
  },
  "page_metadata": {
    "page_number": 1,
    "num_pages": 1,
    "count": 2
  },
  "results": [
    {
      "item": "2015",
      "aggregate": "44948.00"
    },
    {
      "item": "2016",
      "aggregate": "1621763.83"
    }
  ]
}
```

#### GET Requests
GET requests can be specified by attaching any field value pair to the endpoint. This method supports any fields present in the data object and only the `equals` operation. It also supports pagination variables. Additionally, you may specifcy complex fields that use Django's foreign key traversal; for more details on this see `field` from the POST request. Examples below:

`/v1/awards/summary/?page=5&limit=1000`

`/v1/awards/summary/?funding_agency__fpds_code=0300`

#### POST Requests
The structure of the post request allows for a flexible and complex query with built-in pagination support.

#### Body (JSON)

```
{
    "page": 1,
    "limit": 1000,
    "verbose": true,
    "order": ["recipient__location__location_state_code", "-recipient__name"],
    "fields": ["fain", "total_obligation"],
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
* `verbose` - _Optional_ - Most data is self-limiting the amount of data returned. To return all fields without
having to specify them directly in `fields`, you can set this to `true`. Default: `false`
* `order` - _Optional_ - Specify the ordering of the results. This should _always_ be a list, even if it is only of length one. It will order by the first entry, then the second, then the third, and so on in order. This defaults to ascending. To get descending order, put a `-` in front of the field name, e.g. to sort descending on `awarding_agency__name`, put `-awarding_agency__name` in the list.

  ```
  {
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
  * `operation` - The operation to use to compare the field to the value. Some operations place requirements upon the data type in the values parameter, noted below. To negate an operation, use `not_`. For example, `not_equals` or `not_in`. The options for this field are:
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
    "page_number": 1,
    "num_pages": 26,
    "count": 1
  },
  "total_metadata": {
    "count": 26
  },
  "results": [
    {
      "id": 47950,
      "type": "05",
      "type_description": "Cooperative Agreement",
      "piid": null,
      "fain": "SBAHQ15J0005",
      "uri": null,
      "total_obligation": "15000.00",
      "total_outlay": null,
      "date_signed": "2016-09-20",
      "description": "FY 15 7J",
      "period_of_performance_start_date": "2015-09-30",
      "period_of_performance_current_end_date": "2016-12-28",
      "awarding_agency": {
        "toptier_agency": {
          "cgac_code": "073",
          "fpds_code": "7300",
          "name": "SMALL BUSINESS ADMINISTRATION"
        },
        "subtier_agency": {
          "subtier_code": "7300",
          "name": "SMALL BUSINESS ADMINISTRATION"
        },
        "office_agency": null
      },
      "funding_agency": {
        "toptier_agency": {
          "cgac_code": "073",
          "fpds_code": "7300",
          "name": "SMALL BUSINESS ADMINISTRATION"
        },
        "subtier_agency": {
          "subtier_code": "7300",
          "name": "SMALL BUSINESS ADMINISTRATION"
        },
        "office_agency": null
      },
      "recipient": {
        "legal_entity_id": 799999094,
        "ultimate_parent_legal_entity_id": null,
        "recipient_name": "PROJECT SOLUTIONS, INC.",
        "business_types": "Q",
        "location": {
          "location_country_name": "UNITED STATES",
          "location_state_code": "SD",
          "location_state_name": "South Dakota",
          "location_city_name": "Rapid City",
          "location_address_line1": "3022 W Saint Louis St",
          "location_address_line2": null,
          "location_address_line3": null,
          "location_zip5": "57702",
          "location_foreign_postal_code": null,
          "location_foreign_province": null,
          "location_foreign_city_name": null,
          "location_country_code": "USA"
        }
      },
      "place_of_performance": 18,
      "procurement_set": [],
      "financialassistanceaward_set": [
        {
          "type": "05",
          "action_date": "2016-09-20",
          "federal_action_obligation": "8901.33",
          "modification_number": "0003:560400DB",
          "description": "FY 15 7J",
          "cfda_number": "59.007",
          "cfda_title": "7(j) Technical Assistance",
          "face_value_loan_guarantee": null,
          "original_loan_subsidy_cost": null,
          "update_date": "2017-01-20T21:27:59.580606Z"
        },
        {
          "type": "05",
          "action_date": "2016-09-20",
          "federal_action_obligation": "6098.67",
          "modification_number": "0003:670400DB",
          "description": "FY 15 7J",
          "cfda_number": "59.007",
          "cfda_title": "7(j) Technical Assistance",
          "face_value_loan_guarantee": null,
          "original_loan_subsidy_cost": null,
          "update_date": "2017-01-20T21:27:59.531692Z"
        }
      ]
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
  * `results` - An array of objects corresponding to the data returned by the specified endpoint. Will _always_ be an array, even if the number of results is only one.

### Autocomplete Queries
Autocomplete queries currently require the endpoint to have additional handling, as such, only a few have been implemented (notably awards/summary).
#### Body
```
{
	fields": ["toptier_agency__name", "subtier_agency__name"],
	"value": "DEFENSE",
	"mode": "contains",
  "limit": 100,
  "matched_objects": true
}
```
#### Body Description
  * `fields` - A list of fields to be searched for autocomplete. This allows for foreign key traversal using the usual Django patterns. This should _always_ be a list, even if the length is only one
  * `value` - The value to use as the autocomplete pattern. Typically a string, but could be a number in uncommon circumstances. The search will currently _always_ be case insensitive
  * `mode` - _Optional_ - The search mode. Options available are:
    * `contains` - Matches if the field's value contains the specified value
    * `startswith` - Matches if the field's value starts with the specified value
  * `matched_objects` - _Optional_ - Boolean value specifying whether or not to return matching data objects. Default: false
  * `limit` - _Optional_ - Limits the number of query matches. Defaults to 10.

#### Response
```
{
  "results": {
    "toptier_agency__name": [
      "DEFENSE NUCLEAR FACILITIES SAFETY BOARD",
      "DEPT OF DEFENSE"
    ],
    "subtier_agency__name": [
      "DEFENSE NUCLEAR FACILITIES SAFETY BOARD",
      "DEFENSE HUMAN RESOURCES ACTIVITY",
      "DEPT OF DEFENSE",
      "DEFENSE THREAT REDUCTION AGENCY (DTRA)",
      "ASSISTANT SECRETARY FOR DEFENSE PROGRAMS"
    ]
  },
  "counts": {
    "toptier_agency__name": 2,
    "subtier_agency__name": 5
  },
  "matched_objects": {
    "toptier_agency__name": [
      {
        "toptier_agency": {
          "cgac_code": "097",
          "fpds_code": "9700",
          "name": "DEPT OF DEFENSE"
        },
        "subtier_agency": {
          "subtier_code": "97JC",
          "name": "MISSILE DEFENSE AGENCY (MDA)"
        },
        "office_agency": null
      },

       . . .

      {
        "toptier_agency": {
          "cgac_code": "097",
          "fpds_code": "9700",
          "name": "DEPT OF DEFENSE"
        },
        "subtier_agency": {
          "subtier_code": "97F7",
          "name": "JOINT IMPROVISED EXPLOSIVE DEVICE DEFEAT ORGANIZATION (JIEDDO)"
        },
        "office_agency": null
      }
    ],
    "subtier_agency__name": [
      {
        "toptier_agency": {
          "cgac_code": "089",
          "fpds_code": "8900",
          "name": "ENERGY, DEPARTMENT OF"
        },
        "subtier_agency": {
          "subtier_code": "8925",
          "name": "ASSISTANT SECRETARY FOR DEFENSE PROGRAMS"
        },
        "office_agency": null
      },

      . . .

      {
        "toptier_agency": {
          "cgac_code": "097",
          "fpds_code": "9700",
          "name": "DEPT OF DEFENSE"
        },
        "subtier_agency": {
          "subtier_code": "9700",
          "name": "DEPT OF DEFENSE"
        },
        "office_agency": null
      }
    ]
  }
}
```
#### Response Description
  * `results` - The actual results. For each field search, will contain a list of all unique values matching the requested value and mode
  * `counts` - Contains the length of each array in the results object
  * `matched_objects` - Only exists if `matched_objects` was specified in the request. An object broken up by specified `fields` with matching objects from the autocomplete query stored in arrays.

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
    * `CONGRESSIONAL DISTRICT` - These are searched using the pattern `XX-##` where `XX` designates a state code, and `##` designates the district number. For example, `VA-06` is district `06` in Virginia
    * `COUNTRY`
    * `CITY`
    * `COUNTY`
    * `STATE`
    * `ZIP`
    * `POSTAL CODE` - Used for foreign postal codes
    * `PROVINCE`
  * `parent` - The parent of the object, in a logical hierarchy. The parents for each type are listed below:
    * `CONGRESSIONAL DISTRICT` - Will specify the parent as the state containing the district
    * `COUNTRY` - Will specify the parent as the country code for reference purposes
    * `CITY` - Will specify the state the city is in for domestic cities, or the country for foreign cities
    * `COUNTY` - Will specify the state the the city is in for domestic cities
    * `STATE` - Will specify the country the state is in
    * `ZIP` - Will specify the state the zip code falls in. If a zip code falls in multiple states, two results will be generated
    * `POSTAL CODE` - Will specify the country the postal code falls in
    * `PROVINCE` - Will specify the country the province is in
