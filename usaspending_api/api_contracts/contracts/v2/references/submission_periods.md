FORMAT: 1A
HOST: https://api.usaspending.gov

# DABS Submission Window Dates [/api/v2/references/submission_periods/]

This endpoint provides a list of all fields in the "dabs_submission_window_schedule" table except 'id'.   

## GET

+ Response 200 (application/json)
    + Attributes (object)
        + `period_start_date` (required, string)
        + `period_end_date` (required, string)
        + `submission_start_date` (required, string)
        + `submission_due_date` (required, string)
        + `certification_due_date` (required, string)
        + `submission_reveal_date` (required, string)
        + `submission_fiscal_year` (required, number)
        + `submission_fiscal_quarter` (required, number)
        + `submission_fiscal_month` (required, number)
        + `is_quarter` (required, boolean)
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

