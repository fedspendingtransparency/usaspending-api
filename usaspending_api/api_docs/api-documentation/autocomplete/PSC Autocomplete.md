## [PSC Autocomplete](#usaspending-api-documentation)
**Route:** `/api/v2/autocomplete/psc/`

**Method:** `POST`

This route sends a request to the backend to retrieve product or service (PSC) codes and their descriptions based on a search string. This may be the 4-character PSC code or a description string.

### Request examples

```
{
    "search_text": "rail"
}
```

```
{
    "search_text": "V213"
}
```

### Request Parameters Description

* `search_text` - **required** - a string that contains the search term(s) on which to search for the award IDs
* `limit` - **optional** - an integer representing the number of desired entries per budget title type. It can be an integer or a string representation of an integer. Defaults to 10.

### Response example

```
{
    "results": [
        {
            "product_or_service_code": "2220",
            "psc_description": "RAIL CARS"
        },
        {
            "product_or_service_code": "AS30",
            "psc_description": "R&D-TRANS-RAIL"
        }
    ]
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
```
