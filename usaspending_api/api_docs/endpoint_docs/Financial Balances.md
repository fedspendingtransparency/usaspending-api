## [Financial Balances](#usaspending-api-documentation)
**Route:** `/api/v2/financial_balances/agencies`

**Method:** `GET`

This route retrieves financial balance information by funding agency and fiscal year

### Sample Request

`/api/v2/financial_balances/agencies?funding_agency=775&fiscal_year=2017`

### Request Params

* `fiscal_year` - **required** - an integer representing the fiscal year to filter on
* `funding_agency_id` - **required** - an integer representing the funding agency ID to filter on (should be an agency_id flagged as top tier)

### Response (JSON)

```
"results": [
    {
      "budget_authority_amount": "120112040.58",
      "obligated_amount": "299640.00",
      "outlay_amount": "8063724.83"
    }
]
```
