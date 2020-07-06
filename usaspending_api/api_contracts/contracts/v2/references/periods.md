FORMAT: 1A
HOST: https://api.usaspending.gov

# Custom Account Data [/api/v2/download/accounts/submission_periods]

This endpoint returns a JSON Object containing an array of tuples consisting of fiscal year and period number for the last available period for each fiscal year. 

## GET

+ Response 200 (application/json)
    + Attributes (object)
        + `valid_periods` (required)
    + Body

            {
                "available_periods": [
                    {
                        "fy": 2017,
                        "period": 12
                    },
                    {
                        "fy": 2018,
                        "period": 12
                    },
                    {
                        "fy": 2019,
                        "period": 12
                    },
                    {
                        "fy": 2020,
                        "period": 8
                    }
                ]
            }
                        
# Data Structures

## AdvancedFilterObject (object)
+ `available_periods` (required, array)
    + this is the list of tuples of valid periods
+ `fy`(required, number)
+ `period`(required, number)
       