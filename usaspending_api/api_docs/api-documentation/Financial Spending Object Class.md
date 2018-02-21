
## Financial Spending Major Object Class
**Route**: `/api/v2/financial_spending/object_class?fiscal_year=[year]&funding_agency_id=[id]&major_object_class_code=[id]`

Retrieve Award Spending Amounts for all Recipients with Respective Top Tier and Sub Tier Agencies

**Method**: `GET`

This route sends a request to the backend to retrieve awarded amounts for all recipients for a specified awarding agency based on the fiscal year, and optional award category.

`Awarding Agency may be a Sub-Tier Agency.`

`Recipients ordered by Obligated Amount.`

**Query Parameters Description**

**-** `fiscal_year` - **required** - an integer representing the fiscal year to filter on.

**-** `funding_agency_id` - **required** - an integer representing the funding_agency_id to filter on.

**-** `major_object_class_code` - **required** - an integer representing the major_object_class_code to filter on.

**Request Example (without optional category)**

`/api/v2/financial_spending/major_object_class?fiscal_year=2017&funding_agency_id=1068&major_object_class_code=20
**Response (JSON)**

HTTP Status Code: 200

```
{
    "page_metadata": {
        "count": 12,
        "page": 1,
        "has_next_page": false,
        "has_previous_page": false,
        "next": null,
        "current": "http://localhost:8000/api/v2/financial_spending/object_class/?fiscal_year=2017&funding_agency_id=1068&limit=100&major_object_class_code=20&page=1",
        "previous": null
    },
    "results": [
        {
            "object_class_code": "251",
            "object_class_name": "Advisory and assistance services",
            "obligated_amount": "57662751.78"
        },
        {
            "object_class_code": "233",
            "object_class_name": "Communications, utilities, and miscellaneous charges",
            "obligated_amount": "805440.77"
        },...
    ]
}
```


**Errors**

Possible HTTP Status Codes:

- 400 : Missing one or more required query parameters: fiscal_year, funding_agency_id, major_object_class_code

- 500 : All other errors
```
{
    "message": "Missing required query parameters: fiscal_year, awarding_agency_id, major_object_class_code"
}
```
