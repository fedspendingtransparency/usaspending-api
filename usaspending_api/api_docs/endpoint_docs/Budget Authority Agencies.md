
## Budget Athority Agencies (DEPRECIATED) 
**Route:** `/api/v2/budget_authority/agencies/(?P<cgac>\w+)/$'`

**Method:** `GET`

This route sends a request to the backend to retrieve the last updated date for the Award data.

### Response (JSON)

```
{
    "page_metadata": {
        "count": 47,
        "page": 1,
        "has_next_page": false,
        "has_previous_page": false,
        "next": null,
        "current": "http://localhost:8000/api/v2/budget_authority/agencies/091/?limit=100&page=1",
        "previous": null
    },
    "results": [
        {
            "year": 2001,
            "total": 39733000000
        },
        {
            "year": 2002,
            "total": 55836000000
        },
        {
            "year": 2003,
            "total": 63000000000
        },
        {
            "year": 2004,
            "total": 67160000000
        },
        {
            "year": 2005,
            "total": 74475000000
        },
        {
            "year": 2006,
            "total": 100018000000
        },
        {
            "year": 2007,
            "total": 68264000000
        },
        {
            "year": 2008,
            "total": 65398000000
        },
        {
            "year": 2009,
            "total": 131891000000
        },
        {
            "year": 2010,
            "total": 62911000000
        },
        {
            "year": 2011,
            "total": 43628000000
        },
        {
            "year": 2012,
            "total": 57458000000
        },
        {
            "year": 2013,
            "total": 39495000000
        },
        {
            "year": 2014,
            "total": 55200000000
        },
        {
            "year": 2015,
            "total": 87258000000
        },
        {
            "year": 2016,
            "total": 76977000000
        },
        {
            "year": 2017,
            "total": 114800000000
        },
        {
            "year": 2018,
            "total": 60971000000
        },
        {
            "year": 2019,
            "total": 65952000000
        },
        {
            "year": 2020,
            "total": 63758000000
        },
        {
            "year": 2021,
            "total": 63818000000
        },
        {
            "year": 2022,
            "total": 62320000000
        }
    ]
}
```

`last_updated` will be an empty string if there is no Award data available to check against.


### Errors
Possible HTTP Status Codes:
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```
