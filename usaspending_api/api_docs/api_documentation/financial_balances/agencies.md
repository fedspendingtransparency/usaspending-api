## [Financial Balances](#usaspending-api-documentation)
**Route:** `/api/v2/financial_balances/agencies`

**Method:** `GET`

This route retrieves financial balance information by funding agency and fiscal year

### Sample Request

`/api/v2/financial_balances/agencies?funding_agency_id=775&fiscal_year=2017`

### Request Params

* `fiscal_year` - **required** - an integer representing the fiscal year to filter on
* `funding_agency_id` - **required** - an integer representing the funding agency ID to filter on (should be an agency_id flagged as top tier)

### Response (JSON)

```
{
    "page_metadata": {
        "count": 1,
        "page": 1,
        "has_next_page": false,
        "has_previous_page": false,
        "next": null,
        "current": "http://localhost:8000/api/v2/financial_balances/agencies/?fiscal_year=2017&funding_agency_id=775&limit=100&page=1",
        "previous": null
    },
"results": [
    {
      "budget_authority_amount": "120112040.58",
      "obligated_amount": "299640.00",
      "outlay_amount": "8063724.83"
    }
]
}
```
