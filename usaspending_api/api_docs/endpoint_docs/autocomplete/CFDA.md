## [CFDA Autocomplete](#usaspending-api-documentation)
**Route:** `/api/v2/autocomplete/cfda/`

**Method:** `POST`

This route sends a request to the backend to retrieve CFDA programs matching the specified search text.

### Request example

```
{
    "search_text": "561",
    "limit": 10
}
```

### Request Parameters Description

* `search_text` - **required** - a string that contains the search term(s) on which to search for the CFDA programs
* `limit` - **optional** - an integer representing the number of desired entries per budget title type. It can be an integer or a string representation of an integer. Defaults to 10.

### Response (JSON)

```
{
    "results": [
        {
            "program_number": "561110",
                "program_title": "title",
                "popular_name": "pop name"
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
