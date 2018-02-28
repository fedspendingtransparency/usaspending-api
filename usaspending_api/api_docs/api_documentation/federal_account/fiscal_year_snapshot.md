## Agency
**Route:** `v2/federal_accounts/[federal_account_id]/fiscal_year_snapshot/[fiscal_year]/`
or
`v2/federal_accounts/[federal_account_id]/fiscal_year_snapshot/`

**Method:** `GET`

This route sends a request to the backend to retrieve budget information for a federal account.  If no fiscal year is used, the federal accounts most recent fiscal year is used.

### Response (JSON) 

```
{
    "results": {
        "outlay": 388068849934.85,
        "budget_authority": 422043215583.31,
        "obligated": 422042380839.31,
        "unobligated": 834744,
        "balance_brought_forward": 31752455111.18,
        "other_budgetary_resources": 941000000,
        "appropriations": 389349760472.13,
        "name": "Grants to States for Medicaid, Centers for Medicare and Medicaid Services, Health and Human Services"
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
