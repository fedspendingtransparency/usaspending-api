## List Budget Functions
**Route:** `/api/v2/budget_functions/list_budget_functions/`

**Method** `GET`

This route sends a request to the backend to retrieve a list of all Budget Functions ordered by their title.


## Response (JSON)

**HTTP Status Code**: 200

```
{
    "results": [
        {
            "budget_function_code": "000",
            "budget_function_title": "Governmental Receipts"
        },
        {
            "budget_function_code": "050",
            "budget_function_title": "National Defense"
        },
        {
            "budget_function_code": "150",
            "budget_function_title": "International Affairs"
        },
        ...
    ]
}

```

**Response Key Descriptions**

**budget_subfunction_title** - Title of the Budget Subfunction.

**budget_subfunction_code** - Code for the Budget Subfunction.

