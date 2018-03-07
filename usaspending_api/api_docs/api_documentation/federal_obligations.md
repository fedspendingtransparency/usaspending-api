## Federal Obligations
**Route:** `v2/federal_obligations?fiscal_year=[year]&federal_account_id=[id]`

**Example:** `v2/federal_obligations?fiscal_year=2017&funding_agency_id=1068`


**Method:** `GET`

This route sends a request to the backend to retrieve federal obligations.

### Query Parameters Description

* `year` - **REQUIRED** - Required parameter indicating what fiscal year to retrieve federal obligations.
* `id` - **REQUIRED** - Required parameter indicating what Agency to retrieve federal obligations for.
* `limit` - **OPTIONAL** - How many results are returned.
* `page` - **OPTIONAL** - What page of results are returned.

### Response (JSON) 

```
{
    "page_metadata": {
        "count": 37,
        "page": 1,
        "has_next_page": false,
        "has_previous_page": false,
        "next": null,
        "current": "http://localhost:8000/api/v2/federal_obligations/?fiscal_year=2017&funding_agency_id=1068&limit=100&page=1",
        "previous": null
    },
    "results": [
        {
            "id": "2497",
            "account_title": "Federal Direct Student Loan Program, Education",
            "obligated_amount": "45538408088.64"
        },
        {
            "id": "2484",
            "account_title": "Student Financial Assistance, Education",
            "obligated_amount": "33796974073.25"
        },
        {
            "id": "2509",
            "account_title": "Education for the Disadvantaged, Education",
            "obligated_amount": "16789619206.31"
        },
        {
            "id": "2500",
            "account_title": "Special Education, Education",
            "obligated_amount": "13063113992.31"
        },...

```

### Errors
Possible HTTP Status Codes:
* 200 with response: 
```
        {
            "message": "Missing required query parameters: fiscal_year & funding_agency_id"
        }
```

* 500 : All other errors
      ```
      {
        "detail": "Sample error message"
      }
      ```

