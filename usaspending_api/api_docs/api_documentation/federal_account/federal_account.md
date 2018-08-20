## Federal Account
**Route:** `v2/federal_accounts/`

**Method:** `POST`

This route sends a request to the backend to retrieve a list of federal accounts.

### Response (JSON) 

```
{
    "count": 786,
    "limit": 10,
    "page": 1,
    "fy": 2017,
    "next": 2,
    "previous": null,
    "hasNext": true,
    "hasPrevious": false,
    "keyword": null,
    "results": [
        {
            "agency_identifier": "075",
            "account_id": 2124,
            "account_number": "075-0512",
            "account_name": "Grants to States for Medicaid, Centers for Medicare and Medicaid Services, Health and Human Services",
            "budgetary_resources": 422043215583.31,
            "managing_agency": "Department of Health and Human Services",
            "managing_agency_acronym": "HHS"
        },
        {
            "agency_identifier": "075",
            "account_id": 2133,
            "account_number": "075-0580",
            "account_name": "Payments to Health Care Trust Funds, Centers for Medicare and Medicaid Services, Health and Human Services",
            "budgetary_resources": 321368198950.35,
            "managing_agency": "Department of Health and Human Services",
            "managing_agency_acronym": "HHS"
        },
        {
            "agency_identifier": "075",
            "account_id": 2237,
            "account_number": "075-8004",
            "account_name": "Federal Supplementary Medical Insurance Trust Fund-Treasury Managed, Health and Human Services",
            "budgetary_resources": 314543101084.43,
            "managing_agency": "Department of Health and Human Services",
            "managing_agency_acronym": "HHS"
        },...

```

### Errors
Possible HTTP Status Codes:
* 500 : All other errors
      ```
      {
        "detail": "Sample error message"
      }
      ```
