FORMAT: 1A
HOST: https://api.usaspending.gov

# DABS Submission Window Dates [/api/v2/references/submission_periods/{?use_cache}]

This endpoint provides a list of all fields in the "dabs_submission_window_schedule" table except 'id'.   

## GET

+ Parameters
    + `use_cache`: `false` (optional, boolean) - Whether or not to use a cache when retrieving values. Defaults to false. 

+ Response 200 (application/json)
    + Attributes (object)
        + `available_periods` (required, array[AvailablePeriod], fixed-type) 
    + Body

            {
                "available_periods": [
                    {
                        "period_start_date": "2016-10-01T00:00:00Z",
                        "period_end_date": "2016-12-31T00:00:00Z",
                        "submission_start_date": "2017-01-19T00:00:00Z",
                        "submission_due_date": "2017-02-19T00:00:00Z",
                        "certification_due_date": "2017-02-19T00:00:00Z",
                        "submission_reveal_date": "2017-02-20T00:00:00Z",
                        "submission_fiscal_year": 2017,
                        "submission_fiscal_quarter": 1,
                        "submission_fiscal_month": 3,
                        "is_quarter": true
                    },
                    {
                        "period_start_date": "2017-01-01T00:00:00Z",
                        "period_end_date": "2017-03-31T00:00:00Z",
                        "submission_start_date": "2017-04-19T00:00:00Z",
                        "submissison_due_date": "2017-05-19T00:00:00Z",
                        "certification_due_date": "2017-05-19T00:00:00Z",
                        "submission_reveal_date": "2017-05-20T00:00:00Z",
                        "submission_fiscal_year": 2017,
                        "submission_fiscal_quarter": 2,
                        "submission_fiscal_month": 6,
                        "is_quarter": true
                    }
                ]
            }
          
# Data Structures

## AvailablePeriod (object)
+ `period_start_date` (required, string) - Period start date for submission
+ `period_end_date` (required, string) - Period end date for submission
+ `submission_start_date` (required, string) - Submission window start date
+ `submission_due_date` (required, string) - Submission window due/end date
+ `certification_due_date` (required, string) - Certification due date
+ `submission_reveal_date` (required, string) - Submission reveal date, submission data must be revealed after this date 
+ `submission_fiscal_year` (required, number) - Fiscal year for submission
+ `submission_fiscal_quarter` (required, number) - Fiscal quarter for submission
+ `submission_fiscal_month` (required, number) - Fiscal month/period for submission
+ `is_quarter` (required, boolean) - Boolean to identify if record is a monthly or quarterly submission
