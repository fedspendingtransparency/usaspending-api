## [Toptier Agency Autocomplete](#usaspending-api-documentation)
**Route:** `/api/v2/autocomplete/toptier_agency/`

**Method:** `POST`

This route sends a request to the backend to retrieve toptier agency names matching the specified search text.

### Request example

```
{
    "search_text": "test",
    "limit": 10
}
```

### Request Parameters Description

* `search_text` - **required** - a string that contains the search term(s) on which to search for the toptier agency name
* `limit` - **optional** - an integer representing the number of desired entries per budget title type. It can be an integer or a string representation of an integer. If it is not provided, all results will be returned.

### Response (JSON)

```
{
    "results": {
        [
            {
                "agency_id": 1,
                "agency_name": "Department of Justice"
            }
            ...
        ]
    }
}
```

## API Response Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```
