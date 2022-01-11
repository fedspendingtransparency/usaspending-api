FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/idvs/funding/]

Used to populate the Federal Account Funding tab on the IDV summary page.

## POST

List File C financial data for an IDV award's descendant contracts

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_IDV_GS23F0170L_4730` (required, string)
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
                + `disaster_emergency_fund_code`
                + `gross_outlay_amount`
                + `object_class`
                    Object class code, object class name
                + `piid`
                + `program_activity`
                    Program activity name, program activity code
                + `reporting_agency_name`
                + `reporting_fiscal_date`
                    Reporting fiscal year, reporting fiscal month
                + `transaction_obligated_amount`
        + `order` (optional, enum[string])
            The direction in which to order results. `asc` for ascending or `desc` for descending.
            + Default: `desc`
            + Members
                + `asc`
                + `desc`
    + Body


            {
                "award_id": "CONT_IDV_TMHQ10C0040_2044",
                "page": 1,
                "sort": "piid",
                "order": "asc",
                "limit": 15
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[IDVFundingResponse], fixed-type)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
    + Body


            {
                "results": [
                    {
                        "award_id": 9338979,
                        "generated_unique_award_id": "CONT_AWD_AG3A94K160035_12G2_GS33FCA001_4732",
                        "reporting_fiscal_year": 2017,
                        "reporting_fiscal_quarter": 4,
                        "reporting_fiscal_month": 12,
                        "is_quarterly_submission": true,
                        "disaster_emergency_fund_code": null,
                        "piid": "AG3A94K160035",
                        "awarding_agency_id": 166,
                        "awarding_toptier_agency_id": 123,
                        "awarding_agency_slug": "department-of-agriculture",
                        "awarding_agency_name": "Department of Agriculture",
                        "funding_agency_id": 166,
                        "funding_toptier_agency_id": 123,
                        "funding_agency_slug": "department-of-agriculture",
                        "funding_agency_name": "Department of Agriculture",
                        "agency_id": "012",
                        "main_account_code": "3700",
                        "account_title": "Food Safety and Inspection Service, Agriculture",
                        "program_activity_code": "0001",
                        "program_activity_name": "SALARIES AND EXPENSES",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 680000.0,
                        "gross_outlay_amount": 0.0
                    },
                    {
                        "award_id": 9338979,
                        "generated_unique_award_id": "CONT_AWD_AG3A94K160035_12G2_GS33FCA001_4732",
                        "reporting_fiscal_year": 2017,
                        "reporting_fiscal_quarter": 3,
                        "reporting_fiscal_month": 9,
                        "is_quarterly_submission": true,
                        "disaster_emergency_fund_code": null,
                        "piid": "AG3A94K160035",
                        "awarding_agency_id": 166,
                        "awarding_toptier_agency_id": 123,
                        "awarding_agency_slug": "department-of-agriculture",
                        "awarding_agency_name": "Department of Agriculture",
                        "funding_agency_id": 166,
                        "funding_toptier_agency_id": 123,
                        "funding_agency_slug": "department-of-agriculture",
                        "funding_agency_name": "Department of Agriculture",
                        "agency_id": "012",
                        "main_account_code": "3700",
                        "account_title": "Food Safety and Inspection Service, Agriculture",
                        "program_activity_code": "0001",
                        "program_activity_name": "SALARIES AND EXPENSES",
                        "object_class": "220",
                        "object_class_name": "Transportation of things",
                        "transaction_obligated_amount": 680000.0,
                        "gross_outlay_amount": 0.0
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false
                }
            }

# Data Structures

## PageMetaDataObject (object)
+ `page`: 2 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next`: 3 (required, number, nullable)
+ `previous`: 1 (required, number, nullable)

## IDVFundingResponse (object)
+ `award_id`: 5531118 (required, number)
    Unique internal surrogate identifier for an award.  Deprecated.  Use `generated_unique_award_id`.
+ `generated_unique_award_id`: `CONT_AWD_15B30518FTM230002_1540_GS23F0170L_4730` (required, string)
    Unique internal natural identifier for an award.
+ `reporting_fiscal_year`: 2018 (required, number, nullable)
    Fiscal year of the submission date.
+ `reporting_fiscal_quarter`: 3 (required, number, nullable)
    Fiscal quarter of the submission date.
+ `reporting_fiscal_month` (required, number, nullable)
    Fiscal month of the submission date.
+ `is_quarterly_submission` (required, boolean, nullable)
    True if the submission is quarterly.  False if the submission is monthly.
+ `disaster_emergency_fund_code` (required, boolean, nullable)
    Code indicating whether or not the funding record is associated with a disaster.
+ `piid`: `15B30518FTM230002` (required, string, nullable)
    Procurement Instrument Identifier (PIID).
+ `awarding_agency_id`: 252 (required, number, nullable)
    Internal surrogate identifier of the awarding agency.
+ `awarding_toptier_agency_id` (required, number, nullable)
+ `awarding_agency_slug` (required, string, nullable)
+ `awarding_agency_name`: `Department of Justice` (required, string, nullable)
+ `funding_agency_id`: 252 (required, number, nullable)
    Internal surrogate identifier of the funding agency.
+ `funding_toptier_agency_id` (required, number, nullable)
+ `funding_agency_slug` (required, string, nullable)
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
+ `gross_outlay_amount`: 792.90 (required, number, nullable)
