FORMAT: 1A
HOST: https://api.usaspending.gov

# Count of awards for Agencies [/api/v2/agency/awards/count{?fiscal_year,group}]

Returns the count of Awards grouped by Award Type under Agencies

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `group` (optional, enum[string])
            Use `cfo` to get results where CFO designated agencies are returned. Otherwise use `all`.
            + Default: `all`
            + Members
                + `cfo`
                + `all`
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY
        + `order` (optional, enum[string])
            Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
            + Default: `desc`
            + Members
                + `desc`
                + `asc`
        + `sort` (optional, enum[string])
            Optional parameter indicating what value results should be sorted by.
            + Default: `obligated_amount`
            + Members
                + `name`
                + `obligated_amount`
                + `gross_outlay_amount`
        + `page` (optional, number)
            The page number that is currently returned.
            + Default: 1
        + `limit` (optional, number)
            How many results are returned
            + Default: 10

+ Response 200 (application/json)
    + Attributes
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
        + `results` (required, array[AgencyResult], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "page_metadata": {
                    "limit": 1,
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": false,
                    "hasPrevious": false,
                    "count": 10
                },
                "results": [
                    {
                    "awarding_toptier_agency_name": "Department of Defense",
                    "awarding_toptier_agency_code": "079",
                    "contracts": 2724,
                    "idvs": 45,
                    "grants": 0,
                    "direct_payments": 0,
                    "loans": 0,
                    "other": 0
                    }
                ],
                "messages": []
            }

# Data Structures

## AgencyResult (object)
+ `award_types` (required, AwardTypes, fixed-type)
+ `awarding_toptier_agency_name` (required, string)
+ `awarding_toptier_agency_code` (required, string)

## AwardTypes (object)
+ `grants` (required, number)
+ `loans` (required, number)
+ `contracts` (required, number)
+ `direct_payments` (required, number)
+ `other` (required, number)
+ `idvs` (required, number)

## PageMetadata (object)
+ `limit` (required, number)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
