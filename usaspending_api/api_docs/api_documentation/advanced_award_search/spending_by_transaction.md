## [Spending by Transaction](#spending-by-transaction)
**Route:** `/api/v2/search/spending_by_transaction/`

**Method:** `POST`

This route takes keyword search fields, and returns the fields of the searched term.

### Request
field: Defines what award variables are returned.

filter: Defines how the transactions are filtered.  Search term and pannel-name are required. Transaction type is one of pane they are looking at. Can be one of the following- ["Contracts", "Grants", "Direct Payments", "Loans", "Other"]

https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation

limit(**OPTIONAL**): how many results are returned

page (**OPTIONAL**):  what page of results are returned

sort (**OPTIONAL**): Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first `field` provided.

order (**OPTIONAL**): Optional parameter indicating what direction results should be sorted by. Valid options include `asc` for ascending order or `desc` for descending order. Defaults to `asc`.

```
{
  "filters": {
       "keyword": "Booz Allen",
       "award_type_codes" : ["10"],
       },
  "fields": ["Award ID", "Recipient Name", "Action Date", "Transaction Amount", "Awarding Agency", "Awarding Sub Agency", "Award Type", "Funding Agency", "Funding Sub Agency", "Mod"],
  "sort": "Recipient Name",
  "order": "desc",
  "page": 1,
  "limit": 30,
}
```

### Response (JSON)

```
{
    "limit": 10,
    "results": [
        {
            "internal_id": "9359566,
            "Award ID": "9359566"",
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
* 500 : All other errors
