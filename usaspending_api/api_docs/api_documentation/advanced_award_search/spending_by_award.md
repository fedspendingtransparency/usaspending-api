## [Spending by Award](#spending-by-award)
**Route:** `/api/v2/search/spending_by_award/`

**Method:** `POST`

This route takes award filters and fields, and returns the fields of the filtered awards.

### Request
fields: Defines what award variables are returned in an array using the Fields provided below.

filters: Defines how the awards are filtered.  The filter object is defined here.  Each top-level key in the filter object is compounded together using AND logic. However, when multiple values are provided for a specific key, those values are compounded using OR logic.

[Filter Object](../search_filters.md)

subawards: boolean value.  True when you want to group by Subawards instead of Awards.  Defaulted to False.

limit (**OPTIONAL**): how many results are returned. If no limit is specified, the limit is set to 10.

page (**OPTIONAL**):  The page number that is currently returned.

sort (**OPTIONAL**): Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first `field` provided. Example: ['Award ID']

order (**OPTIONAL**): Optional parameter indicating what direction results should be sorted by. Valid options include `asc` for ascending order or `desc` for descending order. Defaults to `asc`.

```
{
  "filters": {
       "award_type_codes": ["10"],
       "agencies": [
            {
                 "type": "awarding",
                 "tier": "toptier",
                 "name": "Social Security Administration"
            },
            {
                 "type": "awarding",
                 "tier": "subtier",
                 "name": "Social Security Administration"
            },
            {
                 "type": "funding",
                 "tier": "toptier",
                 "name": "Social Security Administration"
            },
            {
                 "type": "funding",
                 "tier": "subtier",
                 "name": "Social Security Administration"
            }
       ],
       "legal_entities": [779928],
       "recipient_scope": "domestic",
       "recipient_locations": [650597],
       "recipient_type_names": ["Individual"],
       "place_of_performance_scope": "domestic",
       "place_of_performance_locations": [60323],
       "award_amounts": [
              {
                  "lower_bound": 1500000.00,
                  "upper_bound": 1600000.00
              }
       ],
       "award_ids": [1018950]
  },
  "subawards": "False",
  "fields": ["Award ID", "Recipient Name", "Start Date", "End Date", "Award Amount", "Awarding Agency", "Awarding Sub Agency", "Award Type", "Funding Agency", "Funding Sub Agency"],
  "sort": "Recipient Name",
  "order": "desc"
}
```
### Fields
The possible fields returned are split by contracts or assistace awards (loans, grants, etc.)

#### Possible Contract Fields w/ db mapping
```
    'Award ID': 'piid',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Contract Award Type': 'type_description',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Sub Agency': 'funding_subtier_agency_name'
 ```

#### Possible Grant Fields w/ db mapping
```
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Sub Agency': 'funding_subtier_agency_name'
```

#### Possible Loan Fields w/ db mapping
```
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Issued Date': 'action_date',
    'Loan Value': 'face_value_loan_guarantee',
    'Subsidy Cost': 'original_loan_subsidy_cost',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Sub Agency': 'funding_subtier_agency_name'
```

#### Possible Direct Payment Fields w/ db mapping
```
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Sub Agency': 'funding_subtier_agency_name'
```

#### Possible Other Award Fields w/ db mapping
```
    'Award ID': 'fain',
    'Recipient Name': 'recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Sub Agency': 'funding_subtier_agency_name'
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

**page_metadata Descriptions**

**page** - The current page number of results.

**hasNext** - Boolean object. If true, there is another page of results.

```

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```
