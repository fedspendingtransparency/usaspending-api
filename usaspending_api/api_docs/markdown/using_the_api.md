<ul class="nav nav-stacked" id="sidebar">
  <li><a href="/docs/intro-tutorial">Introductory Tutorial</a></li>
  <li><a href="/docs/using-the-api">Using this API</a>
  <!--<ul>
    <li><a href="#get-requests">GET Requests</a></li>
    <li><a href="#post-requests">POST Requests</a></li>
    <li><a href="#summary-endpoints-and-methods">Summary Endpoints & Methods</a></li>
    <li><a href="#pagination">Pagination</a></li>
    <li><a href="#autocomplete-queries">Autocomplete Queries</a></li>
    <li><a href="#geographical-hierarchy-queries">Geographical Hierarchy Queries</a></li>
  </ul>-->
  </li>
  <li><a href="/docs/endpoints">Endpoints</a></li>
  <li><a href="/docs/data-dictionary">Data Dictionary</a></li>
  <li><a href="/docs/recipes">Request Recipes</a></li>
</ul>
[//]: # (Begin Content)

# Using the USAspending Application Program Interface (API) <a name="introduction"></a>

The USAspending API allows the public to access data published via the DATA Act Data Broker or via USAspending.

This guide is intended for users who are already familiar with APIs. If you're not sure what _endpoint_ means, and what `GET` and `POST` requests are, you'll probably find the [introductory tutorial](/docs/intro-tutorial) more useful.






#### GET Requests <a name="get-requests"></a>
GET requests support simple equality filters for fields in the underlying data model. These can be specified by attaching field value pairs to the endpoint as URL parameters:

`/v1/awards?type=B`

Field names support Django's foreign key traversal; for more details on this see `field` in [POST Requests](#post-requests). For example:

`/v1/awards/?type=B&awarding_agency__toptier_agency__cgac_code=073`

#### POST Requests <a name="post-requests"></a>
The structure of the post request allows for a flexible and complex query.

#### POST Request Preservation <a name="post-requests-preservation"></a>
All requests will return a `req` object in their response. This code allows you to share and preserve POST requests from page to page. You can send a request to any endpoint and specify the `req` from any request to re-run that request. For example, if you sent a POST request to `/api/v1/awards/` and returned a `req` of `abcd` you could re-run that request by hitting:

`/api/v1/awards/?req=abcd`

Or by POSTing
```
{
  "req": "abcd"
}
```

Do note that this _ignores_ pagination variables, so you can request a different page from the same request without changing your `req`. For example:

`/api/v1/awards/?req=abcd&page=2`

Or by POSTing
```
{
  "page": 2,
  "req": "abcd"
}
```
Would get the second page of the previous request.

#### Body (JSON)
Below is an example body for the `/v1/awards/?page=1&limit=200` POST request. The API expects the content in JSON format, so the requests's content-type header should be set to `application/json`.

```
{
    "verbose": true,
    "order": ["recipient__location__state_code", "-recipient__recipient_name"],
    "fields": ["fain", "total_obligation"],
    "exclude": ["recipient"],
    "filters": [
      {
        "field": "piid",
        "operation": "equals",
        "value": "SBAHQ16M0163"
      },
      {
        "combine_method": "OR",
        "filters": [ . . . ]
      }
    ]
}
```

#### Options

* `exclude` - _Optional_ - What fields to exclude from the return. Must be a list.
* `fields` - _Optional_ - What fields to return. Must be a list. Omitting this will return all fields.
* `order` - _Optional_ - Specify the ordering of the results. This should _always_ be a list, even if it is only of length one. It will order by the first entry, then the second, then the third, and so on in order. This defaults to ascending. To get descending order, put a `-` in front of the field name. For example, to sort descending on `awarding_agency__name`, put `-awarding_agency__name` in the list.
* `verbose` - _Optional_ - Endpoints that return lists of items (`/awards/` and `/accounts/`, for example) return a default list of fields. To instead return all fields, set this value to `true`. Note that you can also use the `fields` and `exclude` options to override the default field list. Default: false.
* `filters` - _Optional_ - An array of objects specifying how to filter the dataset. When multiple filters are specified in the root list, they will be joined via _and_.
  * `field` - A string specifying the field to compare the value to. This supports Django's foreign key relationship traversal; therefore, `funding_agency__fpds_code` will filter on the field `fpds_code` for the referenced object stored in `funding_agency`.
  * `operation` - The operation to use to compare the field to the value. Some operations place requirements upon the data type in the values parameter, noted below. To negate an operation, use `not_`. For example, `not_equals` or `not_in`. The options for this field are:
    * `equals` - Evaluates the equality of the value with that stored in the field.
      ```
      {
        "field": "fain",
        "operation": "equals",
        "value": "1234567"
      }
      ```
    * `less_than` - Evaluates whether the value stored in the field is less than the value specified in the filter.
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
    * `greater_than` - Evaluates whether the value stored in the field is greater than the value specified in the filter.
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
    * `in` - Evaluates if the value stored in the field is any of the values specified in the value parameter. `value` must be an array of values.
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
    * `contained_by` - A special operation for array fields, matches where the value of the field is entirely contained by the specified array.
    ```
    {
      "field": "business_categories",
      "operation": "contained_by",
      "value": ["local_government", "woman_owned_business"]
    }
    ```
    * `overlap` - A special operation for array fields, matches where the value of the field has any overlap with the specified array.
    ```
    {
      "field": "business_categories",
      "operation": "overlap",
      "value": ["local_government"]
    }
    ```
    * `length_greater_than` and `length_less_than` - A special operation for array fields. As `less_than` and `greater_than`, but on the length of the data in the ArrayField.
    ```
    {
      "field": "business_categories",
      "operation": "length_greater_than",
      "value": "0"
    }
    ```
    * `is_null` - Evaluates if the field is null or not null. `value` must be either `true` or `false`.
    ```
    {
      "field": "awarding_agency",
      "operation": "is_null",
      "value": false
    }
    ```
    * `search` - Executes a full text search on the specified field or fields.
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
    * `range_intersect` - Evaluates if the range defined by a two-field list intersects with the range defined by the two length array `value`. `value` can be a single item _only_ if `value_format` is also set to a range converting value. An example of where this is useful is when a contract spans multiple fiscal years, to evaluate whether it overlaps with any one particular fiscal year - that is, the range defined by `period_of_performance_start` to `period_of_performance_end` intersects with the fiscal year.

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
  * `value_format` - Specifies the format for the value. Only used in some operations where noted. Valid choices are enumerated below.
    * `fy` - Treats a single value as a fiscal year range.
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
The response object structure is the same whether you are making a GET or a POST request. An example of a response from `/v1/awards/` can be found below:

```
{
  "page_metadata": {
   "page": 1,
   "has_next_page": false,
   "next": null,
   "previous": null
  },
  "results": [
  _agency": {
        "subtier_code": "7300",
        "name": "SMALL BUSINESS ADMINISTRATION"
      },
      "office_agency": null
    },
    "recipient": {
      "legal_entity_id": 799999094,
      "parent_recipient_unique_id": null,
      "recipient_name": "PROJECT SOLUTIONS, INC.",
      "business_types": "Q",
      "business_types_description": "For-Profit Organization (Other than Small Business)",
      "location": {
        "country_name": "UNITED STATES",
        "state_code": "SD",
        "state_name": "South Dakota",
        "city_name": null,
        "address_line1": null,
        "address_line2": null,
        "address_line3": null,
        "zip5": null,
        "foreign_postal_code": null,
        "foreign_province": null,
        "foreign_city_name": null,
        "location_country_code": "USA"
      }
    },
    "place_of_performance": {
      "country_name": "UNITED STATES",
      "state_code": null,
      "state_name": "South Dakota",
      "city_name": null,
      "address_line1": null,
      "address_line2": null,
      "address_line3": null,
      "zip5": null,
      "foreign_postal_code": null,
      "foreign_province": null,
      "foreign_city_name": null,
      "location_country_code": "USA"
    },
    "financial_set": [
      {
        "financial_accounts_by_awards_id": 14185,
        "program_activity_name": null,
        "piid": null,
        "fain": "SBAHQ15J0005",
        "uri": null,
        "gross_outlay_amount_by_award_fyb": null,
        "gross_outlay_amount_by_award_cpe": null,
        "last_modified_date": null,
        "certified_date": null,
        "treasury_account": {
          "treasury_account_identifier": 63089,
          "tas_rendering_label": "0732015/20160400",
          "account_title": "Entrepreneurial Development Programs, Small Business Administration",
          "reporting_agency_id": "073",
          "reporting_agency_name": "Small Business Administration"
        },
        "program_activity_code": null,
        "object_class": "410",
        "transaction_obligations": [
          {
            "transaction_obligated_amount": "8901.33"
          }
        ]
      },
  ]
}
```

### Response Description
The response has three functional parts:
  * `page_metadata` - Includes data about the pagination and any page-level metadata specific to the endpoint.
    * `page` - What page is currently being returned.
    * `has_next_page` - Whether or not there is a page after this one.
    * `next` - The link to the next page of this response, if applicable.
    * `previous` - The link to the previous page of this response, if applicable.
  * `req` - The special code corresponding to this POST request. See [Post request preservation]("#post-requests-preservation") for how to use this
  * `results` - An array of objects corresponding to the data returned by the specified endpoint. Will _always_ be an array, even if the number of results is only one.

### CSV Bulk Downloads

Bulk CSV downloads are available via the `/api/v1/download/` endpoint. This supports any data endpoint via POST or GET. Because these CSV files can take some time to generate, the response of this endpoint contains the status of file generation and the location of the file once it is complete.

In the following examples, 'request checksum' refers to the string returned by the `req` variable from the section on [POST request preservation](#post-requests-preservation).

Examples
* To get a CSV of all Awards, you would access `/api/v1/download/awards/`
* To get a CSV of all Awards with a particular set of filters, you can pass the request checksum, `/api/v1/download/awards/?req=CHECKSUM` or POST the request object to the endpoint
* To check the status of a CSV download, make note of the `request_checksum` and return to the url, with `req=CHECKSUM` attached. For example, `/api/v1/download/awards/?req=CHECKSUM`

Response

```
{
  "location": http://path_to_csv_file/something.csv,
  "status": "This file has been requested, and is awaiting queueing",
  "request_checksum": "e78f4722f85",
  "request_path": "/api/v1/awards/",
  "retry_url": "http://api_location/api/v1/awards/?req=e78f4722f85"
}
```

* location - The URL where the file can be accessed (once it has been generated)
* status - A plain english description of the current status of the file generation
* request_checksum - The checksum of the request, which can be used to check the status
* request_path - The path of the CSV request
* retry_url - An easy to use URL to retry your download request, and check if it has been generated yet

Expected response status codes:
* 200 - The file is ready for download, and `location` contains the URL
* 202 - The request has been queued for generation, and `location` is null
* 400 - The endpoint or request checksum that was specified is not currently supported by CSV bulk downloads

### Summary Endpoints and Methods <a name="summary-endpoints-and-methods"></a>
  Summarized data is available for some of the endpoints listed above:

  * **/v1/awards/total/**
  * **/v1/transactions/total/**
  * more coming soon

  You can get summarized data via a `POST` request that specifies:

  * `field`: the field to be summarized (this supports Django's foreign key traversal; for more details on this see `field` in [POST Requests](#post-requests)).
  * `aggregate`: the aggregate function to use when summarizing the data (defaults to `sum`; `avg`, `count`, `min`, and `max` are also supported)
  * `group`: the field to group by (optional; if not specified, data will be summarized across all objects)
  * `date_part`: applies only when `group` is a data field and specifies which part of the date to group by; `year`, `month`, and `day` are currently supported, and `quarter` is coming soon
  * `show_nulls`: Whether to display results where the `group` data or the aggregate field data is null. In effect, this sets `show_null_aggregates` and `show_null_groups` to true. Defaults to false.
  * `show_null_groups`: Whether to display aggregates where all the `group` data is null. Defaults to false.
  * `show_null_aggregates`: Whether to display entries where the aggregate is null. 

  Requests to the summary endpoints can also contain the `filters` parameters as described in [POST Requests](#post-requests). **Note:** If you're filtering the data, the filters are applied before the data is summarized.

  The `results` portion of the response will contain:

  * `item`: the value of the field in the request's `group` parameter (if the request did not supply `group`, `item` will not be included)
  * `aggregate`: the summarized data

  To order the response by the items being returned via the `group` parameter, you can specify an `order` in the request: `"order": ["item"]`. To order the response by the aggregate values themselves, add `"order": ["aggregate]` to the request.

  For example, to request the yearly sum of obligated dollars across transactions for award types "B" and "C" (_i.e._, purchase orders and delivery orders) and to ensure that the response is ordered by year:

  POST request to `/transactions/total`:

  ```json
  {
      "field": "federal_action_obligation",
      "group": "action_date",
      "date_part": "year",
      "aggregate": "sum",
      "order": ["item"],
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
    "page_metadata": {
     "page": 1,
     "has_next_page": false,
     "next": null,
     "previous": null
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

  To summarize a field using a foreign key and to order the response by the summarized values from highest to lowest:

  POST request to `/awards/total`:

  ```json
  {
      "field": "total_obligation",
      "group": "place_of_performance__state_code",
      "aggregate": "sum",
      "order": ["-aggregate"]
  }
  ```
  Response:

  ```json
  {
    "page_metadata": {
     "page": 1,
     "has_next_page": false,
     "next": null,
     "previous": null
    },
    "results": [
      {
        "item": "MO",
        "aggregate": "500000.00"
      },
      {
        "item": "DC",
        "aggregate": "485795.43"
      },
      {
        "item": "UT",
        "aggregate": "0.00"
      },
      {
        "item": "HI",
        "aggregate": "-2891.33"
      }
    ],
  }
  ```

### Pagination <a name="pagination"></a>
  To control the number of items returned on a single "page" of a request or to request a specific page number, use the following URL parameters:

  * `page` - specifies the page of results to return. The default is 1.
  * `limit` - specifies the maximum number of items to return in a response page. The default is 100.

  For example, the following request will limit the awards on a single page to 20 and will return page 5 of the results:

  `/v1/awards/?page=5&limit=20`

### Autocomplete Queries <a name="autocomplete-queries"></a>

Autocomplete Endpoints allow developers to include autocomplete functionality in user interfaces for the API. Only a few have been implemented thus far (notably `/awards/`).

These endpoints currently only support POST requests. Let's look at `/api/v1/awards/autocomplete`, which performs autocomplete requests against award records. Each autocomplete request requires at least an array of `fields` to search against, and a `value` to search for.


#### Options
  * `fields` - A list of fields to be searched for autocomplete. This allows for foreign key traversal using the usual Django patterns. This should _always_ be a list, even if the length is only one.
  * `value` - The value to use as the autocomplete pattern. Typically a string, but could be a number in uncommon circumstances. The search will currently _always_ be case insensitive.
  * `mode` - _Optional_ - The search mode. Options available are:
    * `contains` - Matches if the field's value contains the specified value.
    * `startswith` - Matches if the field's value starts with the specified value.
  * `matched_objects` - _Optional_ - Boolean value specifying whether or not to return matching data objects. Default: false.
  * `limit` - _Optional_ - Limits the number of query matches. Defaults to 10.
  * `filters` - _Optional_ - As on regular endpoint, filters the data before performing the autocomplete.

#### Example

##### Body
```
{
	fields": ["toptier_agency__name", "subtier_agency__name"],
	"value": "DEFENSE",
	"mode": "contains",
    "limit": 100,
    "matched_objects": true
  "filters": []
}
```

##### Response
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

### Geographical Hierarchy Queries <a name="geographical-hierarchy-queries"></a>
This is a special type of autocomplete query which allows users to search for geographical locations in a hierarchy.

#### Body
```
{
  "value": "u",
  "mode": "startswith",
  "scope": "domestic",
  "usage": "recipient",
  "limit": 50
}
```

#### Body Description
  * `value` - The value to use as the autocomplete pattern. The search will currently _always_ be case insensitive.
  * `mode` - _Optional_ -The search mode. Options available are:
    * `contains` - Matches if the field's value contains the specified value. This is the default behavior.
    * `startswith` - Matches if the field's value starts with the specified value.
  * `scope` - _Optional_ - The scope of the search. Options available are:
    * `domestic` - Matches only entries with the United States as the `location_country_code`
    * `foreign` - Matches only entries where the `location_country_code` is _not_ the United States
    * `all` - Matches any location entry. This is the default behavior
  * `usage` - _Optional_ - The usage of the search. Options available are:
    * `recipient` - Matches only entries where the location is used as a recipient location.
    * `place_of_performance` - Matches only entries where the location is used as a place of performance.
    * `all` - Matches all locations. This is the default behavior.
  * `limit` - _Optional_ - The maximum number of responses in the autocomplete response. Defaults to 10.

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
  * `matched_ids` - An array of `location_id`s that match the given data. This can be used to look up awards, recipients, or other data by requesting these ids.
  * `place_type` - The type of place. Options are:
    * `CONGRESSIONAL DISTRICT` - These are searched using the pattern `XX-##` where `XX` designates a state code, and `##` designates the district number. For example, `VA-06` is district `06` in Virginia.
    * `COUNTRY`
    * `CITY`
    * `COUNTY`
    * `STATE`
    * `ZIP`
    * `POSTAL CODE` - Used for foreign postal codes
    * `PROVINCE`
  * `parent` - The parent of the object, in a logical hierarchy. The parents for each type are listed below:
    * `CONGRESSIONAL DISTRICT` - Will specify the parent as the state containing the district.
    * `COUNTRY` - Will specify the parent as the country code for reference purposes.
    * `CITY` - Will specify the state the city is in for domestic cities, or the country for foreign cities.
    * `COUNTY` - Will specify the state the the city is in for domestic cities.
    * `STATE` - Will specify the country the state is in.
    * `ZIP` - Will specify the state the zip code falls in. If a zip code falls in multiple states, two results will be generated.
    * `POSTAL CODE` - Will specify the country the postal code falls in.
    * `PROVINCE` - Will specify the country the province is in.
