FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Publish Dates [/api/v2/reporting/agencies/{agency_code}/publish-dates?{fiscal_year,fiscal_period,search,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data submission history modal. This data can be used to better understand the ways agencies submit data.

## GET

This endpoint returns an overview of government agencies submission data.

+ Parameters
    + `agency_code`: `020` (required, string)
        The specific agency code.
    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period`: 10 (required, number)
        The fiscal period.

+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array[AgencyData], fixed-type)
    + Body

            {
                "results": [
                  {
                    "publish_date": "2020-10-11T11:59:21Z",
                    "certify_date": "2020-10-22T11:59:21Z",
                  },
                  {
                    "publish_date": "2020-07-10T11:59:21Z",
                    "certify_date": "2020-07-11T11:59:21Z",
                  }
                ]
            }

# Data Structures

## SubmissionHistory (object)
+ `publish_date` (required, string, nullable)
+ `certify_date` (required, string, nullable)
