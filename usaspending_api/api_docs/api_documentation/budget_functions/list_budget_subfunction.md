## List Budget Subfunctions
**Route:** `/api/v2/budget_functions/list_budget_subfunctions/`

**Method** `POST`

This route sends a request to the backend to retrieve a list of all Budget Subfunctions that can be filtered by Budget Function, ordered by their title.

### Request

```
{
    "budget_function":"050"
}
```
**Query Parameters Description**

**budget_function** - `optional` - a string that contains the budget function code to filter by


### Response (JSON)

**HTTP Status Code**: 200

```
{
    "results": [
        {
            "budget_subfunction_title": "Atomic energy defense activities",
            "budget_subfunction_code": "053"
        },
        {
            "budget_subfunction_title": "Defense-related activities",
            "budget_subfunction_code": "054"
        },
        {
            "budget_subfunction_title": "Department of Defense-Military",
            "budget_subfunction_code": "051"
        }
    ]
}
```

**Response Key Descriptions**

**budget_subfunction_title** - Title of the Budget Subfunction.

**budget_subfunction_code** - Code for the Budget Subfunction.
