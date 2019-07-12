## [Spending by Award](#spending-by-award)
**Route:** `/api/v2/search/spending_by_award/`

**Method:** `POST`

This route takes award filters and fields, and returns the fields of the filtered awards.

### Request
fields: Defines what award variables are returned in an array using the Fields provided below.

filters: Defines how the awards are filtered.  The filter object is defined here.  Each top-level key in the filter object is compounded together using AND logic. However, when multiple values are provided for a specific key, those values are compounded using OR logic.

[Filter Object](../search_filters.md)

subawards (**OPTIONAL**): Boolean value.  True when you want to group by Subawards instead of Awards.  Defaulted to False.

limit (**OPTIONAL**): How many results are returned. If no limit is specified, the limit is set to 10.

page (**OPTIONAL**): The page number that is currently returned.

sort (**OPTIONAL**): Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first `field` provided. Example: ['Award ID']

order (**OPTIONAL**): Optional parameter indicating what direction results should be sorted by. Valid options include `asc` for ascending order or `desc` for descending order. Defaults to `asc`.

```
{
  "fields": [
    "Award ID",
    "Recipient Name",
    "Start Date",
    "End Date",
    "Award Amount",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Award Type",
    "Funding Agency",
    "Funding Sub Agency"
  ],
  "filters": {
    "agencies": [
      {
        "name": "Social Security Administration",
        "tier": "toptier",
        "type": "awarding"
      }
    ],
    "award_amounts": [
      {
        "lower_bound": 0.00,
        "upper_bound": 1600000.00
      }
    ],
    "award_ids": ["0002", "2017Q1", "0274"],
    "award_type_codes": ["A","B","C","D"],
    "place_of_performance_scope": "domestic",
    "recipient_locations": [
      {
        "country": "USA",
        "state": "AK"
      }
    ],
    "recipient_scope": "domestic"
  },
  "order": "desc",
  "sort": "Award Amount",
  "subawards": false
}
```
### Fields
The possible fields returned are split by contracts (and IDV) or assistance awards (loans, grants, etc.)

#### Possible Award Fields (available for every type of award)
```
    'Recipient Name': 'recipient_name',
    'Recipient DUNS Number': 'recipient_unique_id',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Agency Code': 'awarding_toptier_agency_code',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Awarding Sub Agency Code': 'awarding_subtier_agency_code',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Agency Code': 'funding_toptier_agency_code',
    'Funding Sub Agency': 'funding_subtier_agency_name',
    'Funding Sub Agency Code': 'funding_subtier_agency_code',
    'Place of Performance City Code': 'pop_city_code',
    'Place of Performance State Code': 'pop_state_code',
    'Place of Performance Country Code': 'pop_country_code',
    'Place of Performance Zip5': 'pop_zip5',
    'Period of Performance Start Date': 'period_of_performance_start_date',
    'Period of Performance Current End Date': 'period_of_performance_current_end_date',
    'Description': 'description',
    'Last Modified Date': 'last_modified_date',
    'Base Obligation Date': 'date_signed'
```

#### Possible Contract (and IDV) Fields w/ db mapping
```
    'Award ID': 'piid',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Contract Award Type': 'type_description',
 ```

#### Possible Grant Fields w/ db mapping
```
    'Award ID': 'fain',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Award Type': 'type_description',
    'SAI Number': 'sai_number',
    'CFDA Number': 'cfda_number'
```

#### Possible Loan Fields w/ db mapping
```
    'Award ID': 'fain',
    'Issued Date': 'action_date',
    'Loan Value': 'total_loan_value',
    'Subsidy Cost': 'total_subsidy_cost',
    'SAI Number': 'sai_number',
    'CFDA Number': 'cfda_number'
```

#### Possible Direct Payment Fields w/ db mapping
```
    'Award ID': 'fain',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Award Type': 'type_description',
    'SAI Number': 'sai_number',
    'CFDA Number': 'cfda_number'
```

#### Possible Other Award Fields w/ db mapping
```
    'Award ID': 'fain',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Award Type': 'type_description',
    'SAI Number': 'sai_number',
    'CFDA Number': 'cfda_number'
```

### Response (JSON) - Award

```
{
    "limit": 10,
    "results": [
        {
            "internal_id": 1018950,
            "Award ID": null,
            "Recipient Name": "MULTIPLE RECIPIENTS",
            "Start Date": null,
            "End Date": null,
            "Award Amount": 1573663,
            "Awarding Agency": "Social Security Administration",
            "Awarding Sub Agency": "Social Security Administration",
            "Award Type": "10",
            "Funding Agency": "Social Security Administration",
            "Funding Sub Agency": "Social Security Administration"
        }
    ],
    "page_metadata": {
        "page": 1,
        "hasNext": true
    }
}

```

### Response (JSON) - Subaward

```
{
    "limit": 10,
    "results": [
        {
            "internal_id": 4712890,
            "Subaward ID": null,
            "Subaward Type": "procurement",
            "Subawardee Name": null,
            "Subaward Date": null,
            "Subaward Amount": 1573663,
            "Awarding Agency": "Social Security Administration",
            "Awarding Sub Agency": "Social Security Administration",
            "Prime Award ID": null,
            "Prime Recipient Name": "MULTIPLE RECIPIENTS"
        }
    ],
    "page_metadata": {
        "page": 1,
        "hasNext": true
    }
}

```

**page_metadata Descriptions**

**page** - The current page number of results.

**hasNext** - Boolean object. If true, there is another page of results.


### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 422 : Award Type code for subawards is not a code for contracts, IDVs, or grants
```
{
    "detail": "Award Type codes limited for Subawards. Only contracts ['A', 'C', 'D', 'B'], IDVs ['IDV_A', 'IDV_B_A', 'IDV_D', 'IDV_B', 'IDV_B_C', 'IDV_B_B', 'IDV_C', 'IDV_E'], or grants ['05', '03', '04', '02'] are available"
}
```
* 500 : All other errors

```
{
    "detail": "Sample error message"
}
```

