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
            Award ID to further refine results.  All File C financial data for this award is returned if omitted.
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
        + `results` (required, array[IDVFundingResponse], fixed-type)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)

    + Body


            {
                "results": [
                    {
                        "award_id": 5531118,
                        "generated_unique_award_id": "CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L",
                        "reporting_fiscal_year": 2019,
                        "reporting_fiscal_quarter": 1,
                        "piid": "15B30518FTM230002",
                        "awarding_agency_id": 252,
                        "awarding_agency_name": "Department of Justice",
                        "funding_agency_id": 252,
                        "funding_agency_name": "Department of Justice",
                        "agency_id": "015",
                        "main_account_code": "1060",
                        "account_title": "Salaries and Expenses, Federal Prison System, Justice",
                        "program_activity_code": "0002",
                        "program_activity_name": "INSTITUTION SECURITY AND ADMINISTRATION",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": -53.82
                    },
                    {
                        "award_id": 5531118,
                        "generated_unique_award_id": "CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L",
                        "reporting_fiscal_year": 2018,
                        "reporting_fiscal_quarter": 4,
                        "piid": "15B30518FTM230002",
                        "awarding_agency_id": 252,
                        "awarding_agency_name": "Department of Justice",
                        "funding_agency_id": 252,
                        "funding_agency_name": "Department of Justice",
                        "agency_id": "015",
                        "main_account_code": "1060",
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
                        "awarding_agency_id": 252,
                        "awarding_agency_name": "Department of Justice",
                        "funding_agency_id": 252,
                        "funding_agency_name": "Department of Justice",
                        "agency_id": "015",
                        "main_account_code": "1060",
                        "account_title": "Salaries and Expenses, Federal Prison System, Justice",
                        "program_activity_code": "0002",
                        "program_activity_name": "INSTITUTION SECURITY AND ADMINISTRATION",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 193.96
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
+ `page`: 2 (required, number)
+ `hasNext`: `false` (required, boolean)
+ `hasPrevious`: `false` (required, boolean)
+ `next`: 3 (required, number, nullable)
+ `previous`: 1 (required, number, nullable)

## IDVFundingResponse (object)
+ `award_id`: 5531118 (required, number)
    Unique internal surrogate identifier for an award.  Deprecated.  Use `generated_unique_award_id`.
+ `generated_unique_award_id`: `CONT_AW_1540_4730_15B30518FTM230002_GS23F0170L` (required, string)
    Unique internal natural identifier for an award.
+ `reporting_fiscal_year`: 2018 (required, number, nullable)
    Fiscal year of the submission date.
+ `reporting_fiscal_quarter`: 3 (required, number, nullable)
    Fiscal quarter of the submission date.
+ `piid`: `15B30518FTM230002` (required, string, nullable)
    Procurement Instrument Identifier (PIID).
+ `awarding_agency_id`: 252 (required, number, nullable)
    Internal surrogate identifier of the awarding agency.
+ `awarding_agency_name`: `Department of Justice` (required, string, nullable)
+ `funding_agency_id`: 252 (required, number, nullable)
    Internal surrogate identifier of the funding agency.
+ `funding_agency_name`: `Department of Justice` (required, string, nullable)
+ `agency_id`: `015` (required, string, nullable)
    CGAC of the funding agency.
+ `main_account_code`: `1060` (required, string, nullable)
+ `account_title`: `Salaries and Expenses, Federal Prison System, Justice` (required, string, nullable)
    Federal Account Title
+ `program_activity_code`: `0002` (required, string, nullable)
+ `program_activity_name`: `INSTITUTION SECURITY AND ADMINISTRATION` (required, string, nullable)
+ `object_class`: `220` (required, string, nullable)
+ `object_class_name`: `Transportation of things` (required, string, nullable)
+ `transaction_obligated_amount`: 193.96 (required, number, nullable)
