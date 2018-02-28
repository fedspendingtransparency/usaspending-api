## [Award Spending Award Category](#usaspending-api-documentation)
**Route**: `/api/v2/award_spending/award_category`

Retrieve Award Spending Amounts for all Award Category with Respective Top Tier and Sub Tier Agencies

**Method**: `GET`

This route sends a request to the backend to retrieve awarded amounts for all award categories for a specified awarding agency based on the fiscal year.

`Awarding agency may be a sub tier agency.`

**Query Parameters Description**

**-** `fiscal_year` - **required** - an integer representing the fiscal year to filter on.

**-** `awarding_agency_id` - **required** - an integer representing the toptier_agency_id to filter on.

**Request Example**

`http://localhost:8000/api/v2/award_spending/award_category/?awarding_agency_id=246&fiscal_year=2017`

**Response (JSON)**

HTTP Status Code: 200

```
{
    "page_metadata": {
        "count": 3,
        "page": 1,
        "has_next_page": false,
        "has_previous_page": false,
        "next": null,
        "current": "http://localhost:8000/api/v2/award_spending/award_category/?awarding_agency_id=246&fiscal_year=2017&limit=100&page=1",
        "previous": null
    },
    "results": [
        {
            "award_category": "contract",
            "obligated_amount": "1310545328.31"
        },
        {
            "award_category": null,
            "obligated_amount": "1426115.40"
        },
        {
            "award_category": "grant",
            "obligated_amount": "-10900170.33"
        }
    ]
}
```

**Errors**

Possible HTTP Status Codes:

- 400 : Missing parameters

- 500 : All other errors
```
{
    "message": "Missing required query parameters: fiscal_year & awarding_agency_id"
}
```
