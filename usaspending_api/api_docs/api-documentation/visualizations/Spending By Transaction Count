## [Spending by Transaction Count](#transaction-count)
**Route:** `/api/v2/search/spending_by_transaction_count/`

**Method:** `POST`

This route takes keyword search fields, and returns the fields of the searched term.

### Request
field: Defines what award variables are returned.

keyword: search-phrase user submitted to search on

```
{
    "filters": {
        "keyword": "Education",
        "award_type": "prime", // out of scope
        "description_only": true // out of scope
    }
}
```

### Response (JSON)

```
{
    "results": {
        "grants": 0,
        "loans": 0,
        "contracts": 0,
        "direct_payments": 1,
        "other": 0
    }
}
```

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors
