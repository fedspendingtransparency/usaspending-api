FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Funding [/api/v2/awards/funding/]

Used to populate the Federal Account Funding tab on the Award V2 summary pages

## POST

List financial data for the requested award

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AWD_W31P4Q19F0034_9700_W31P4Q18D0002_9700` (required, string)
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
                        "transaction_obligated_amount": 251000.0,
                        "federal_account": "057-3600",
                        "account_title": "Research, Development, Test, and Evaluation, Air Force",
                        "funding_agency_name": "Department of the Air Force",
                        "funding_agency_id": 1173,
                        "awarding_agency_name": null,
                        "awarding_agency_id": null,
                        "object_class": "255",
                        "object_class_name": "Research and development contracts",
                        "program_activity_code": null,
                        "program_activity_name": null,
                        "reporting_fiscal_year": 2019,
                        "reporting_fiscal_quarter": 2
                    },
                    {
                        "transaction_obligated_amount": 0.0,
                        "federal_account": "097-0400",
                        "account_title": "Research, Development, Test, and Evaluation, Defense-Wide",
                        "funding_agency_name": "Department of Defense",
                        "funding_agency_id": 1173,
                        "awarding_agency_name": null,
                        "awarding_agency_id": null,
                        "object_class": "251",
                        "object_class_name": "Advisory and assistance services",
                        "program_activity_code": null,
                        "program_activity_name": null,
                        "reporting_fiscal_year": 2019,
                        "reporting_fiscal_quarter": 2
                    },
                    {
                        "transaction_obligated_amount": 250000.0,
                        "federal_account": "097-0400",
                        "account_title": "Research, Development, Test, and Evaluation, Defense-Wide",
                        "funding_agency_name": "Department of Defense",
                        "funding_agency_id": 1173,
                        "awarding_agency_name": null,
                        "awarding_agency_id": null,
                        "object_class": "251",
                        "object_class_name": "Advisory and assistance services",
                        "program_activity_code": null,
                        "program_activity_name": null,
                        "reporting_fiscal_year": 2019,
                        "reporting_fiscal_quarter": 2
                    },
                    {
                        "transaction_obligated_amount": 1000000.0,
                        "federal_account": "097-0400",
                        "account_title": "Research, Development, Test, and Evaluation, Defense-Wide",
                        "funding_agency_name": "Department of Defense",
                        "funding_agency_id": 1173,
                        "awarding_agency_name": null,
                        "awarding_agency_id": null,
                        "object_class": "255",
                        "object_class_name": "Research and development contracts",
                        "program_activity_code": null,
                        "program_activity_name": null,
                        "reporting_fiscal_year": 2019,
                        "reporting_fiscal_quarter": 2
                    }
                ],
                "page_metadata": {
                    "hasNext": false,
                    "hasPrevious": false,
                    "next": null,
                    "page": 1,
                    "previous": null
                }
            }

# Data Structures

## PageMetaDataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)

## AwardFundingResponse (object)
+ `reporting_fiscal_year` (required, number, nullable)
    Fiscal year of the submission date.
+ `reporting_fiscal_quarter` (required, number, nullable)
    Fiscal quarter of the submission date.
+ `awarding_agency_id`: 252 (required, number, nullable)
    Internal surrogate identifier of the awarding agency.
+ `awarding_agency_name` (required, string, nullable)
+ `funding_agency_id` (required, number, nullable)
    Internal surrogate identifier of the funding agency.
+ `funding_agency_name` (required, string, nullable)
+ `main_account_code`  (required, string, nullable)
+ `account_title`  (required, string, nullable)
    Federal Account Title
+ `program_activity_code` (required, string, nullable)
+ `program_activity_name`  (required, string, nullable)
+ `object_class` (required, string, nullable)
+ `object_class_name`  (required, string, nullable)
+ `transaction_obligated_amount` (required, number, nullable)
