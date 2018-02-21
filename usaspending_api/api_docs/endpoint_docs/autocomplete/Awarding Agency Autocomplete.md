## [Awarding Agency Autocomplete](#usaspending-api-documentation)
**Route:** `/api/v2/autocomplete/awarding_agency/`

**Method:** `POST`

This route sends a request to the backend to retrieve awarding agencies matching the specified search text.

### Request example

```
{
    "search_text": "test",
    "limit": 10
}
```

### Request Parameters Description

* `search_text` - **required** - a string that contains the search term(s) on which to search for the awarding agencies
* `limit` - **optional** - an integer representing the number of desired entries. It can be an integer or a string representation of an integer. Defaults to 10.

### Response (JSON)

```
{
    "results": {
        [
            {
                "id": 1301,
                "toptier_flag": false,
                "toptier_agency": {
                    "cgac_code": "097",
                    "fpds_code": "9700",
                    "abbreviation": "DOD",
                    "name": "Department of Defense"
                },
                "subtier_agency": {
                    "subtier_code": "5703",
                    "abbreviation": "",
                    "name": "Air Force Operational Test and Evaluation Center "
                },
                "office_agency": null
            },
            ...
        ]
    }
}
```
