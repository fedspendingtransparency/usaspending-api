FORMAT: 1A
HOST: https://api.usaspending.gov

# List Budget Functions [/api/v2/budget_functions/list_budget_functions/]

## GET

This route sends a request to the backend to retrieve a list of all Budget Functions ordered by their title.

+ Response 200 (application/json)
    + Attributes
        + `results` (required, array)
            + (object)
                + `budget_function_code` (required, string)
                + `budget_function_title` (required, string)
