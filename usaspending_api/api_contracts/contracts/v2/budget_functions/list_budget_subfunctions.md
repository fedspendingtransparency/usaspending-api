FORMAT: 1A
HOST: https://api.usaspending.gov

# List Budget Subfunctions [/api/v2/budget_functions/list_budget_subfunctions/]

## POST

This route sends a request to the backend to retrieve a list of all Budget Subfunctions that can be filtered by Budget Function, ordered by their title.
        
+ Request (application/json)
    + Attributes (object)
        + `budget_function` (optional, string)
    + Body

            {
                "budget_function": "800"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[BudgetSubfunctions], fixed-type)

# Data Structures

## BudgetSubfunctions (object)
+ `budget_subfunction_code` (required, string)
+ `budget_subfunction_title` (required, string)
