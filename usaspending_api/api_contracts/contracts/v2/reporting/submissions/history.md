FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies [/api/v2/references/agencies/{agency_code}/submissions/history/?{fiscal_year,fiscal_period,search,page,limit,order,sort}]

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
                    "publish_date": "10/11/20 08:59:22",
                    "certify_date": "10/22/20 11:59:34",
                  },
                  {
                    "publish_date": "07/10/20 08:59:22",
                    "certify_date": "07/11/20 11:59:34",
                  }
                ]
            }

# Data Structures

## SubmissionHistory (object)
+ `publish_date`: (required, date)
+ `certify_date`: (required, date)
