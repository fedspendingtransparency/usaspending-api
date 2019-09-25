FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Funding [/api/v2/awards/funding/]

Used to populate the Federal Account Funding tab on the Award V2 summary pages

## POST

Lists federal account financial data for the requested award

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AWD_0002_2800_SS001740003_2800` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `limit`: 5 (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page`: 1 (optional, number)
            The page of results to return based on `limit`.
            + Default: 1
        + `sort` (optional, enum[string])
            The field on which to order results.
            + Default: `reporting_fiscal_date`
            + Members
                + `account_title`
                + `object_class`
                    Object class code, object class name
                + `program_activity`
                    Program activity name, program activity code
                + `funding_agency_name`
                + `awarding_agency_name`
                + `reporting_fiscal_date`
                    Reporting fiscal year, reporting fiscal period
                + `transaction_obligated_amount`
        + `order` (optional, enum[string])
            The direction in which to order results. `asc` for ascending or `desc` for descending.
            + Default: `desc`
            + Members
                + `asc`
                + `desc`

+ Response 200 (application/json)
    + Attributes
        + `results` (required, array[AwardFundingResponse], fixed-type)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)

    + Body


            {
                "results": [
                    {
                        "transaction_obligated_amount": 6960.0,
                        "federal_account": "028-0400",
                        "account_title": "Office of the Inspector General, Social Security Administration",
                        "funding_agency_name": "Social Security Administration",
                        "funding_agency_id": 539,
                        "awarding_agency_name": null,
                        "awarding_agency_id": null,
                        "object_class": "254",
                        "object_class_name": "Operation and maintenance of facilities",
                        "program_activity_code": "0001",
                        "program_activity_name": "OFFICE OF INSPECTOR GENERAL (DIRECT)",
                        "reporting_fiscal_year": 2017,
                        "reporting_fiscal_quarter": 4
                    },
                    {
                        "transaction_obligated_amount": 70296.0,
                        "federal_account": "028-8704",
                        "account_title": "Salaries and Expenses, Social Security Administration",
                        "funding_agency_name": "Social Security Administration",
                        "funding_agency_id": 539,
                        "awarding_agency_name": null,
                        "awarding_agency_id": null,
                        "object_class": "254",
                        "object_class_name": "Operation and maintenance of facilities",
                        "program_activity_code": "0001",
                        "program_activity_name": "LAE PROGRAM DIRECT",
                        "reporting_fiscal_year": 2017,
                        "reporting_fiscal_quarter": 4
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "count": 2,
                    "next": null,
                    "previous": null,
                    "hasPrevious": false,
                    "hasNext": false
                }
            }

# Data Structures

## PageMetaDataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `count` (required, number)

## AwardFundingResponse (object)
+ `reporting_fiscal_year` (required, number, nullable)
    Fiscal year of the submission date.
+ `reporting_fiscal_quarter` (required, number, nullable)
    Fiscal quarter of the submission date.
+ `awarding_agency_id` (required, number, nullable)
    Internal surrogate identifier of the awarding agency.
+ `awarding_agency_name` (required, string, nullable)
+ `funding_agency_id` (required, number, nullable)
    Internal surrogate identifier of the funding agency.
+ `funding_agency_name` (required, string, nullable)
+ `federal_account` (required, string, nullable)
    Identifier of the federal account
+ `account_title`  (required, string, nullable)
    Federal Account Title
+ `program_activity_code` (required, string, nullable)
+ `program_activity_name`  (required, string, nullable)
+ `object_class` (required, string, nullable)
+ `object_class_name`  (required, string, nullable)
+ `transaction_obligated_amount` (required, number, nullable)
