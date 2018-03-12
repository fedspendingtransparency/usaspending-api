## [Spending by Transaction Count](#transaction-count)
**Route:** `/api/v2/search/spending_by_transaction_count/`

**Method:** `POST`

This route takes keyword search fields, and returns the fields of the searched term.

### Request
**field** - Defines what award variables are returned.

**keyword** - search term used to query the database.

```
{
    "filters": {
        "keyword": "Education",
        "award_type": "prime", // future enhancement
        "description_only": true // future enhancement
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
* 422 : if the request is technically valid but doesn't conform with all constraints
* 500 : All other errors
