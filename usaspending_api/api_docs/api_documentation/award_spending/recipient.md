## [Award Spending Recipient](#usaspending-api-documentation)
**Route**: `/api/v2/award_spending/recipient`

Retrieve Award Spending Amounts for all Recipients with Respective Top Tier and Sub Tier Agencies

**Method**: `GET`

This route sends a request to the backend to retrieve awarded amounts for all recipients for a specified awarding agency based on the fiscal year, and optional award category.

`Awarding Agency may be a Sub-Tier Agency.`

`Recipients ordered by Obligated Amount.`

**Query Parameters Description**

**-** `fiscal_year` - **required** - an integer representing the fiscal year to filter on.

**-** `awarding_agency_id` - **required** - an integer representing the toptier_agency_id to filter on.

**-** `award_category` - **optional** - contract, grant, loans, direct dayment, other, null. Default: All

**Request Example (without optional category)**

`/api/v2/award_spending/recipient?fiscal_year=2017&awarding_agency_id=246`

**Response (JSON)**

HTTP Status Code: 200

```
{
    "page_metadata": {
        "count": 6221,
        "page": 1,
        "has_next_page": true,
        "has_previous_page": false,
        "next": "http://localhost:8000/api/v2/award_spending/recipient/?awarding_agency_id=246&fiscal_year=2017&limit=100&page=2",
        "current": "http://localhost:8000/api/v2/award_spending/recipient/?awarding_agency_id=246&fiscal_year=2017&limit=100&page=1",
        "previous": null
    },
    "results": [
        {
            "award_category": "contract",
            "obligated_amount": "68887647.00",
            "recipient": {
                "recipient_id": 5466,
                "recipient_name": "J.E. DUNN CONSTRUCTION COMPANY"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "61667232.21",
            "recipient": {
                "recipient_id": 2618,
                "recipient_name": "CORRECTIONS CORPORATION OF AMERICA"
            }
        },
        {
            MORE RESULTS...{
                 ...

            }
        },
```

**Request Example (contract)**

`/api/v2/award_spending/recipient/?award_category=contract&awarding_agency_id=246&fiscal_year=2017&limit=100&page=1`

```
{
    "page_metadata": {
        "count": 5369,
        "page": 1,
        "has_next_page": true,
        "has_previous_page": false,
        "next": "http://localhost:8000/api/v2/award_spending/recipient/?award_category=contract&awarding_agency_id=246&fiscal_year=2017&limit=100&page=2",
        "current": "http://localhost:8000/api/v2/award_spending/recipient/?award_category=contract&awarding_agency_id=246&fiscal_year=2017&limit=100&page=1",
        "previous": null
    },
    "results": [
        {
            "award_category": "contract",
            "obligated_amount": "68887647.00",
            "recipient": {
                "recipient_id": 5466,
                "recipient_name": "J.E. DUNN CONSTRUCTION COMPANY"
            }
        }
```

**Request Example (grant)**

`/api/v2/award_spending/recipient/?award_category=grant&awarding_agency_id=246&fiscal_year=2017&limit=100&page=1`

```
{
    "page_metadata": {
        "count": 683,
        "page": 1,
        "has_next_page": true,
        "has_previous_page": false,
        "next": "http://localhost:8000/api/v2/award_spending/recipient/?award_category=grant&awarding_agency_id=246&fiscal_year=2017&limit=100&page=2",
        "current": "http://localhost:8000/api/v2/award_spending/recipient/?award_category=grant&awarding_agency_id=246&fiscal_year=2017&limit=100&page=1",
        "previous": null
    },
    "results": [
        {
            "award_category": "grant",
            "obligated_amount": "8192902.35",
            "recipient": {
                "recipient_id": 1717,
                "recipient_name": "Florida Department of Legal Affairs"
            }
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
