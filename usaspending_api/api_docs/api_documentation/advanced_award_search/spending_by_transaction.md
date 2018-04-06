## [Spending by Transaction](#spending-by-transaction)
**Route:** `/api/v2/search/spending_by_transaction/`

**Method:** `POST`

This route takes keyword search terms and returns awards where a certain subset of fields match against the term. For more information about which fields are searched, refer to https://usaspending-help.zendesk.com/hc/en-us/articles/360001255774-Keyword-Search-Question-How-does-the-Keyword-Search-work-
### Request
fields: An array of string column names to return. See Fields list below.

filters: An object with `keyword` and `award_type_codes` keys. `keyword` should be a string that you are performing a keyword search operation with. `award_type_codes` is an array of strings of the award type codes that should be searched within.

A list of award type codes can be found at http://fedspendingtransparency.github.io/whitepapers/types/
[Filter Object](../search_filters.md)

limit(**OPTIONAL**): how many results are returned

page (**OPTIONAL**):  what page of results are returned

sort (**OPTIONAL**): Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first `field` provided.

order (**OPTIONAL**): Optional parameter indicating what direction results should be sorted by. Valid options include `asc` for ascending order or `desc` for descending order. Defaults to `asc`.

```
{
    "filters": {
        "keyword": "money",
        "award_type_codes": [
            "A",
            "B",
            "C",
            "D"
        ]
    },
    "fields": [
        "Award ID",
        "Mod",
        "Recipient Name",
        "Action Date",
        "Transaction Amount",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Award Type"
    ],
    "page": 1,
    "limit": 35,
    "sort": "Transaction Amount",
    "order": "desc"
}
```

### Response (JSON)

```
{
    "limit": 10,
    "results": [
        {
            "internal_id": "9359566",
            "Award ID": "W15P7T06CN401",
            "Recipient Name": "MULTIPLE RECIPIENTS",
            "Action Date": "2018-12-23",
            "Transaction Amount": "481.00",
            "Awarding Agency": "Social Security Administration",
            "Awarding Sub Agency": "Social Security Administration",
            "Funding Agency": "Social Security Administration",
            "Funding Sub Agency": "Social Security Administration",
            "Mod": "0",
        }
    ],
    "page_metadata": {
        "page": 1,
        "hasNext": true,
    }
}
```

### Fields
The possible fields returned are split by contracts or assistace awards (loans, grants, etc.)

#### Possible Contract Fields w/ es mapping
```
    'Award ID': 'piid',
    'Recipient Name': 'recipient_name',
    'Action Date' : action_date,
    'Transaction Amount': 'transaction_amount',
    'Contract Award Type': 'type_description',
    'Awarding Agency': 'awarding_toptier_agency_name',
    'Awarding Sub Agency': 'awarding_subtier_agency_name',
    'Funding Agency': 'funding_toptier_agency_name',
    'Funding Sub Agency': 'funding_subtier_agency_name'

 ```
### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 422 : if the request is technically valid but doesn't conform with all constraints
* 500 : All other errors
