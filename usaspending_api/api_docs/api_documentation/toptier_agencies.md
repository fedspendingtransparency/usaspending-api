## [Toptier Agencies](#usaspending-api-documentation)
**Route:** `/api/v2/references/toptier_agencies/`

**Method:** `GET`

This route sends a request to the backend to retrieve all toptier agencies and related, relevant data.

### Request example

```
/api/v2/references/toptier_agencies/?sort=agency_name&order=asc
```

### Query Parameters Description

* `sort` - **OPTIONAL** - Optional parameter indicating what value results should be sorted by. Valid options are any of the keys in the JSON objects in the response. Defaults to `agency_name`.
* `order` - **OPTIONAL** - Optional parameter indicating what direction results should be sorted by. Valid options include `asc` for ascending order or `desc` for descending order. Defaults to `asc`.

### Response (JSON)

```
{
    "results": [
      {
        'agency_id': 4,
        'abbreviation': 'tta_abrev',
        'agency_name': 'tta_name',
        'active_fy': '2017',
        'active_fq': '2',
        'outlay_amount': 2.00,
        'obligated_amount': 2.00,
        'budget_authority_amount': 2.00,
        'current_total_budget_authority_amount': 3860000000.00,
        'percentage_of_total_budget_authority': 1.21231
      },
      ...
    ]
}
```

### Response Attributes Description

* `agency_id`: Agency ID. Used as a reference for other endpoints for agency related lookups.
* `abbreviation`: The agency's acronym
* `agency_name`: Agency name
* `active_fy`: Agency's most recent submission year
* `active_fq`: Agency's most recent submission quarter
* `outlay_amount`: Agency's outlay amount
* `obligated_amount`: Agency's obligated amount
* `budget_authority_amount`: Agency total budget authority
* `current_total_budget_authority_amount`: Govenment total budget authority for the respective fiscal year
* `percentage_of_total_budget_authority`: budget_authority_amount / current_total_budget_amount
