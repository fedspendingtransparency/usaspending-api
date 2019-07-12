## [Spending Explorer](#spending-explorer)

**Route:** `/api/v2/spending/`

**Method:** `POST`

This route sends a request to the backend to retrieve spending data information through various types and filters.

### Request

```
{
    "type": "agency",
    "filters": {
        "fy": "2017"
    }
}
```
**Query Parameters Description**

**type** - `required` - a string that contains the type of spending to explore on. It must be one of these values: *budget_function*, *budget_subfunction*, *federal_account*, *program_activity*, *object_class*, *recipients*, *awards*, *agency*

**filters** - `required` - an object with the following key-value pairs that indicate what subset of results will be displayed. All values should be strings. The *quarter* field is cumulative and quarters are only available 45 days after their close. Also quarter data is not available before FY 2017 Q2.
  `filter options` - *budget_function*, *budget_subfunction*, *federal_account*, *program_activity*, *object_class*, *recipient*, *award*, *agency*, *fy*, *quarter*


### Response (JSON)

**HTTP Status Code**: 200

```
{
    "total": 3710586195160.03,
    "end_date": "2017-06-30",
    "results": [
        {
            "amount": 1208140717396.79,
            "id": 777,
            "type": "agency",
            "name": "Department of Health and Human Services",
            "code": "075",
            "total": 1208140717396.79
        },
        {
            "amount": 814453033538.42,
            "id": 462,
            "type": "agency",
            "name": "Department of the Treasury",
            "code": "020",
            "total": 814453033538.42
        },
        {
            "amount": 793681110475.13,
            "id": 552,
            "type": "agency",
            "name": "Social Security Administration",
            "code": "028",
            "total": 793681110475.13
        },
        {
            "amount": 144830556846.39,
            "id": 533,
            "type": "agency",
            "name": "Office of Personnel Management",
            "code": "024",
            "total": 144830556846.39
        },
        {
            "amount": 141781627513.91,
            "id": 577,
            "type": "agency",
            "name": "Department of Veterans Affairs",
            "code": "036",
            "total": 141781627513.91
        },
        ...
    ]
}
```

**NOTE**: For `type: federal_account` responses, the response object will also contain an `account_number` field containing the federal account code,. i.e.,

```
{
    "amount": 141781627513.91,
    "id": 577,
    "type": "federal_account",
    "name": "Department of Veterans Affairs",
    "code": "036",
    "total": 141781627513.91
    "account_number": "123-4567"
}
```

**Response Key Descriptions**

**id** - The Database Id for the requested *type*

**type** - The type of the response object.

**code** - Result determined by type. If the type is agency, the result is the agency CGAC.
If the type is recipient, the code is the recipient DUNS number.
If the type is award, the result is Award ID.

**amount**/**total** - The obligated amount expected by the type.

**name** - The name used to distinguish the object.


### Errors

Possible HTTP Status Codes:

- 400 : Missing/invalid parameters. ie. the FY or Quarter is not available.

- 500 : All other errors

**Sample Error**
```
{
    "details": "Incorrect fiscal year format, should be four digit year: YYYY"
}
```

