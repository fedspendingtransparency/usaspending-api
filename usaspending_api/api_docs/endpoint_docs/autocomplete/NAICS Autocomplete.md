## [NAICS Autocomplete](#usaspending-api-documentation)
**Route:** `/api/v2/autocomplete/naics/`

**Method:** `POST`

This route sends a request to the backend to retrieve NAICS objects matching the specified search text.

### Request example

```
{
    "search_text": "OFFICE ADMINISTRATIVE SER",
    "limit": 10
}
```

### Request Parameters Description

* `search_text` - **required** - a string that contains the search term(s) on which to search for the NAICS objects
* `limit` - **optional** - an integer representing the number of desired entries per budget title type. It can be an integer or a string representation of an integer. Defaults to 10.

### Response (JSON)

```
{
    "results": [
        {
            "naics": "561110",
            "naics_decription": "OFFICE ADMINISTRATIVE SERVICES"
        },
        ....

    ],

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
