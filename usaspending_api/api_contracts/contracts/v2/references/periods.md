FORMAT: 1A
HOST: https://api.usaspending.gov

# Custom Account Data [/api/v2/download/accounts/submission_periods]

This endpoint returns a JSON Object containing an array of tuples consisting of fiscal year and period number for all valid periods. 

## GET

+ Response 200 (application/json)
    + Attributes (object)
        + `valid_periods` (required)
    + Body

            {
                "available_periods": [{"fy": 2020, "period": 10}, {"fy": 2020, "period": 11}, {"fy": 2020, "period": 12}, {"fy": 2021, "period": 1}, {"fy": 2021, "period": 2}]
            }
            
# Data Structures

## AdvancedFilterObject (object)
+ `available_periods` (required, array)
    + this is the list of tuples of valid periods
+ `fy`(required, number)
+ `period`(required, number)
       