## [List Agencies](#download-status)
**Route:** `/api/v2/bulk_download/list_agencies/`

**Method:** `POST`

This route lists all the agencies and the subagencies or federal accounts associated under specific agencies.

### Request example

```
{
    "agency": 14
}
```

### Request Parameters Description

* `agency` - agency database id. If provided, populates `sub_agencies` and `federal_accounts`.

### Response (JSON)

When agency is not provided or agency is 0:
```
{
    "agencies": {
        "cfo_agencies": [
            {
                "name": "Department of Agriculture",
                "toptier_agency_id": 513,
                "toptier_code": "012"
            },
            ...
        ],
        "other_agencies": [
            {
                "name": "Abraham Lincoln Bicentennial Commission",
                "toptier_agency_id": 644,
                "toptier_code": "289"
            },
            ...
        ]
    },
    "sub_agencies": [],
    "federal_accounts": []
}
```
When agencies is provided with an agency id of `2`:
```
{
    "agencies": [],
    "sub_agencies": [
        {
            "subtier_agency_name": "The Legislative Branch"
        }
    ],
    "federal_accounts": [
        {
            "federal_account_name": "United States Capitol Police Memorial Fund, Capitol Police",
            "federal_account_id": 90
        },
        ...
    ]
}
```

The response includes 3 possible objects
* `agencies` - list of all agencies, provided if no `agency` is provided in the request
    * `name`
    * `toptier_agency_id` - database id
    * `toptier_code` - a unique code used by the government to identify agencies (Common Government-wide Accounting Classification)
* `sub_agencies`- list of subtier agencies associated with the agency id provided
    * `subtier_agency_name`
* `federal_accounts` - list of federal acounts associated with the agency id provided
    * `federal_account_name`
    * `federal_account_id` - database id
