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

**type** - `required` - a string that contains the type of spending to explore on.
  `explorer types` - *budget_function*, *budget_subfunction*, *federal_account*, *program_activity*, *object_class*, *recipients*, *awards*, *agency*

**filters** - `optional` - integer or string index values to filter explorers on.
  `filter options` - *budget_function*, *budget_subfunction*, *federal_account*, *program_activity*, *object_class*, *recipient*, *award*, *agency*, *fy*

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

### Errors

Possible HTTP Status Codes:

- 400 : Missing parameters

- 500 : All other errors

**Sample Error**
```
{
    "details": "Incorrect fiscal year format, should be four digit year: YYYY"
}
```

