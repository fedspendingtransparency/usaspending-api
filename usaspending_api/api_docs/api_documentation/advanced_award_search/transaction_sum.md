## [Transaction Sum](#transaction-sum) DEPRECIATED
**Route:** `/api/v2/search/transaction_sum/`

**Method:** `POST`

This route takes keyword search fields, and returns the fields of the searched term.

### Request
field: Defines what award variables are returned.

keyword: search-phrase user submitted to search on

```
{
    "filters": {
        "keyword": "DOD"
    }
}
```

### Response (JSON)

```
{
    "results": {
        "transaction_sum": {
            "value": 2123391.470062971
        }
    }
}
```

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
