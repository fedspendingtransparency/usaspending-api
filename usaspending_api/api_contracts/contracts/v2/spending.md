FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending Explorer [/api/v2/spending/]

These endpoints are used to power USAspending.gov's Spending Explorer. This data can be used to drill down into specific subsets of data by level of detail.

The Spending Explorer requires the top-level (or entry point) filter to be one of three types:

* Budget Function (`budget_function`)
* Agency (`agency`)
* Object Class (`object_class`)

## POST

**General Spending Explorer Data**

The general Spending Explorer response will contain only one filter (required), a fiscal year and quarter to limit the data to. The data will include _all_ quarters up to and including the specified quarter in the given fiscal year.

This data represents all government spending in the specified time period, grouped by the data type of your choice.

Note that data is not available prior to FY 2017 Q2.

Note that data for the latest complete quarter is not available until 45 days after the quarter's close.

**Specific Spending Explorer**

Using the response from the general Spending Explorer, you can drill down to more detailed grouping fields. However, you must limit the scope of your request to one of the top-level groups and, optionally, additional lower-level groups. Each of your groups will combine to become the scope of your request. For example, if you filter by "Department of Justice" and "Salaries and Expenses," you will only see spending breakdowns for "Salaries and Expenses" within "Department of Justice."

+ Request Specific Spending Explorer (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `type` (required, enum[string])
            + Members
                + `federal_account`
                + `object_class`
                + `recipient`
                + `award`
                + `budget_function`
                + `budget_subfunction`
                + `agency`
                + `program_activity`
        + `filters` (required, DetailedFilter, fixed-type)
    + Body

            {
                "type":"recipient",
                "filters": {
                    "fy":"2020",
                    "quarter":"1",
                    "budget_function":"500",
                    "budget_subfunction":"501",
                    "federal_account":"5901"
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `total` (required, number, nullable)
            Total should only be null when there are no results.
        + `end_date` (required, string)
            This is the "as-of" date for the data being returned.
        + `results` (required, array[SpendingExplorerDetailedResponse], fixed-type)
    + Body

            {
                "total": 10758018561.52,
                "end_date": "2019-12-31",
                "results": [
                    {
                        "amount": 1338785075,
                        "id": "EDUCATION, CALIFORNIA DEPARTMENT OF",
                        "type": "recipient",
                        "name": "EDUCATION, CALIFORNIA DEPARTMENT OF",
                        "code": "EDUCATION, CALIFORNIA DEPARTMENT OF",
                        "total": 1338785075
                    },
                    {
                        "amount": 1049216400,
                        "id": "TEXAS EDUCATION AGENCY",
                        "type": "recipient",
                        "name": "TEXAS EDUCATION AGENCY",
                        "code": "TEXAS EDUCATION AGENCY",
                        "total": 1049216400
                    },
                    {
                        "amount": 849061298,
                        "id": "EDUCATION DEPARTMENT, NEW YORK STATE",
                        "type": "recipient",
                        "name": "EDUCATION DEPARTMENT, NEW YORK STATE",
                        "code": "EDUCATION DEPARTMENT, NEW YORK STATE",
                        "total": 849061298
                    },
                    {
                        "amount": 638939299,
                        "id": "EDUCATION, FLORIDA DEPARTMENT OF",
                        "type": "recipient",
                        "name": "EDUCATION, FLORIDA DEPARTMENT OF",
                        "code": "EDUCATION, FLORIDA DEPARTMENT OF",
                        "total": 638939299
                    },
                    {
                        "amount": 449552712,
                        "id": "EDUCATION, ILLINOIS STATE BOARD OF",
                        "type": "recipient",
                        "name": "EDUCATION, ILLINOIS STATE BOARD OF",
                        "code": "EDUCATION, ILLINOIS STATE BOARD OF",
                        "total": 449552712
                    },
                    {
                        "amount": 421294862,
                        "id": "EDUCATION, PENNSYLVANIA DEPT OF",
                        "type": "recipient",
                        "name": "EDUCATION, PENNSYLVANIA DEPT OF",
                        "code": "EDUCATION, PENNSYLVANIA DEPT OF",
                        "total": 421294862
                    },
                    {
                        "amount": 392298566,
                        "id": "DEPARTMENT OF EDUCATION OHIO",
                        "type": "recipient",
                        "name": "DEPARTMENT OF EDUCATION OHIO",
                        "code": "DEPARTMENT OF EDUCATION OHIO",
                        "total": 392298566
                    },
                    {
                        "amount": 372229194,
                        "id": "EDUCATION, GEORGIA DEPARTMENT OF",
                        "type": "recipient",
                        "name": "EDUCATION, GEORGIA DEPARTMENT OF",
                        "code": "EDUCATION, GEORGIA DEPARTMENT OF",
                        "total": 372229194
                    }
                ]
            }

+ Request General Spending Explorer (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `type` (required, enum[string])
            + Members
                + `budget_function`
                + `agency`
                + `object_class`
        + `filters` (required, GeneralFilter, fixed-type)

    + Body

            {
                "type": "federal_account",
                "filters": {
                    "fy": 2019,
                    "quarter": "1"
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `total` (required, number, nullable)
            Total should only be null when there are no results.
        + `end_date` (required, string)
            This is the "as-of" date for the data being returned.
        + `results` (required, array[SpendingExplorerGeneralResponse, SpendingExplorerGeneralUnreportedResponse], fixed-type)
    + Body

            {
                "total": 1716262905344.08,
                "end_date": "2018-12-31",
                "results": [
                    {
                        "id": "050",
                        "code": "050",
                        "type": "budget_function",
                        "name": "National Defense",
                        "amount": 371478539473.82
                    },
                    {
                        "id": "570",
                        "code": "570",
                        "type": "budget_function",
                        "name": "Medicare",
                        "amount": 273180031194.15
                    },
                    {
                        "id": "650",
                        "code": "650",
                        "type": "budget_function",
                        "name": "Social Security",
                        "amount": 270988863288.42
                    }
                ]
            }


# Data Structures

## GeneralFilter (object)
+ `fy`: `2019` (required, string)
+ `quarter` (required, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`

## DetailedFilter (object)
+ `fy`: `2019` (required, string)
+ `quarter` (optional, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`
+ `period` (optional, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`
        + `5`
        + `6`
        + `7`
        + `8`
        + `9`
        + `10`
        + `11`
        + `12`
+ `agency` (optional, number)
    This value is the `id` returned in the general Spending Explorer response.
+ `federal_account` (optional, number)
    This value is the `id` returned in the previous specific Spending Explorer response.
+ `object_class` (optional, number)
+ `budget_function` (optional, number)
+ `budget_subfunction` (optional, number)
+ `recipient` (optional, number)
+ `program_activity` (optional, number)

## SpendingExplorerGeneralResponse (object)
+ `code` (required, string)
+ `id` (required, string)
+ `generated_unique_award_id` (optional, string)
    Durable identifier for an award.  This value only returned for award type.
+ `type` (required, string)
    The `type` will always be equal to the `type` parameter you provided in the request.
+ `name` (required, string)
+ `amount` (required, number)

### SpendingExplorerDetailedResponse (object)
+ `code` (required, string)
+ `id` (required, string)
+ `generated_unique_award_id` (optional, string)
    Durable identifier for an award.  This value only returned for award type requests.
+ `type` (required, string)
    The `type` will always be equal to the `type` parameter you provided in the request.
+ `name` (required, string)
+ `amount` (required, number)
+ `account_number` (optional, string)
    The response includes `account_number` when the requested `type` was `federal_account`.
+ `link` (optional, string)
    The response includes `link` when the requested `type` was `agency`

## SpendingExplorerGeneralUnreportedResponse (object)
+ `code` (optional, nullable)
+ `id` (optional, nullable)
+ `generated_unique_award_id` (optional, string)
    Durable identifier for an award.  This value only returned for award type.
+ `type` (required, string)
    The `type` will always be equal to the `type` parameter you provided in the request.
+ `name` (required, string)
+ `amount` (required, number)
