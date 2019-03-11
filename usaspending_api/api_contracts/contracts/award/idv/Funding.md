FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/awards/idvs/funding/]

Returns File C financial data for an IDV (Indefinite Delivery Vehicle) award's descendant contracts.  Used to populate the Federal Account Funding tab on the IDV summary page.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AW_4730_-NONE-_GS23F0170L_-NONE-` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `piid`: `15B30518FTM230002` (optional, string)
            `Award ID` to further refine results.  All File C financial data for this award is returned if omitted.
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
                + `piid`
                + `program_activity`
                    Program activity name, program activity code
                + `reporting_agency_name`
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
        + results (required, array[IDVFundingResponse])
        + page_metadata (required, PageMetaDataObject, fixed-type)

    + Body

            {
                "results": [
                    {
                        "award_id": 5531118,
                        "generated_unique_award_id": "CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L",
                        "reporting_fiscal_year": 2018,
                        "reporting_fiscal_quarter": 4,
                        "piid": "15B30518FTM230002",
                        "reporting_agency_name": "Department of Justice",
                        "account_title": "Salaries and Expenses, Federal Prison System, Justice",
                        "program_activity_code": "0002",
                        "program_activity_name": "INSTITUTION SECURITY AND ADMINISTRATION",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 251.16
                    },
                    {
                        "award_id": 5531118,
                        "generated_unique_award_id": "CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L",
                        "reporting_fiscal_year": 2018,
                        "reporting_fiscal_quarter": 3,
                        "piid": "15B30518FTM230002",
                        "reporting_agency_name": "Department of Justice",
                        "account_title": "Salaries and Expenses, Federal Prison System, Justice",
                        "program_activity_code": "0002",
                        "program_activity_name": "INSTITUTION SECURITY AND ADMINISTRATION",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 193.96
                    },
                    {
                        "award_id": 5531118,
                        "generated_unique_award_id": "CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L",
                        "reporting_fiscal_year": 2018,
                        "reporting_fiscal_quarter": 2,
                        "piid": "15B30518FTM230002",
                        "reporting_agency_name": "Department of Justice",
                        "account_title": "Salaries and Expenses, Federal Prison System, Justice",
                        "program_activity_code": "0002",
                        "program_activity_name": "INSTITUTION SECURITY AND ADMINISTRATION",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 140.97
                    },
                    {
                        "award_id": 5531118,
                        "generated_unique_award_id": "CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L",
                        "reporting_fiscal_year": 2018,
                        "reporting_fiscal_quarter": 1,
                        "piid": "15B30518FTM230002",
                        "reporting_agency_name": "Department of Justice",
                        "account_title": "Salaries and Expenses, Federal Prison System, Justice",
                        "program_activity_code": "0002",
                        "program_activity_name": "INSTITUTION SECURITY AND ADMINISTRATION",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 192.64
                    }
                ],
                "page_metadata": {
                    "next": null,
                    "page": 1,
                    "hasPrevious": false,
                    "previous": null,
                    "hasNext": false
                }
            }

# Data Structures

## PageMetaDataObject (object)
+ page: 1 (required, number)
+ hasNext: false (required, boolean)
+ hasPrevious: false (required, boolean)
+ next: null (required, string, nullable)
+ previous: null (required, string, nullable)

## IDVFundingResponse (object)
+ `award_id` (required, number)
    Unique internal surrogate identifier for an award.  Deprecated.  Use `generated_unique_award_id`.
+ `generated_unique_award_id` (required, string)
    Unique internal natural identifier for an award.
+ `reporting_fiscal_year` (required, number, nullable)
    Fiscal year of the submission date.
+ `reporting_fiscal_period` (required, number, nullable)
    Fiscal period of the submission date.
+ `piid` (required, string, nullable)
    Procurement Instrument Identifier (PIID).
+ `reporting_agency_id` (required, string, nullable)
+ `reporting_agency_name` (required, string, nullable)
+ `account_title` (required, string, nullable)
    Federal Account Title
+ `program_activity_code` (required, string, nullable)
+ `program_activity_name` (required, string, nullable)
+ `object_class` (required, string, nullable)
+ `object_class_name` (required, string, nullable)
+ `transaction_obligated_amount` (required, number, nullable)
