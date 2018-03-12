## Fiscal Year Snapshot
**Route:** `v2/federal_accounts/[federal_account_id]/fiscal_year_snapshot/[fiscal_year]/`
or
`v2/federal_accounts/[federal_account_id]/fiscal_year_snapshot/`

**Method:** `GET`

This route sends a request to the backend to retrieve budget information for a federal account.  If no fiscal year is supplied, the federal account's most recent fiscal year is used.

### Response (JSON) 

```
{
    "results": {
        "outlay": 10251146698.74,
        "budget_authority": 11321365505.59,
        "obligated": 11201187238.13,
        "unobligated": 120178267.46,
        "balance_brought_forward": 343155467.19,
        "other_budgetary_resources": 1737470909.68,
        "appropriations": 9240739128.72,
        "name": "Weapons Activities, National Nuclear Security Administration, Energy"
    }
}
```

### Errors
Possible HTTP Status Codes:
* 200 : {}  - no federal account with that id
* 500 : All other errors
      ```
      {
        "detail": "Sample error message"
      }
      ```
