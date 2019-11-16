FORMAT: 1A
HOST: https://api.usaspending.gov

# Transactions [/api/v2/transactions/]

This endpoint is used to power the Transaction History tables on USAspending.gov's award summary pages. This data can be used to better understand the details of specific transactions on a given award.

## POST

This endpoint returns a list of transactions, their amount, type, action date, action type, modification number, and description.

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `ASST_NON_NNX17AJ96A_8000` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `limit`: 15 (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page`: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + `sort`: action_date (optional, enum[string])
            The field results are sorted by.
            + Default: `action_date`
            + Members
                + `modification_number`
                + `action_date`
                + `federal_action_obligation`
                + `face_value_loan_guarantee`
                + `original_loan_subsidy_cost`
                + `action_type_description`
                + `description`
        + `order`: desc (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: `desc`

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (array[TransactionResult], fixed-type)
        + `page_metadata` (PageMetaDataObject)

# Data Structures

## TransactionResult (object)
+ `action_date`: `1999-01-15` (required, string)
    Action date in the format `YYYY-MM-DD`.
+ `action_type`: `C` (required, string, nullable)
    Action type code
+ `action_type_description`: `description` (required, string, nullable)
+ `description`: MANAGEMENT AND OPERATIONS (required, string, nullable)
+ `face_value_loan_guarantee`: 1234.56 (required, number, nullable)
    Face value of the loan. Null for results with award type codes that **do not** correspond to loans.
+ `federal_action_obligation`: 1234.56 (required, number, nullable)
    Monetary value of the transaction. Null for results with award type codes that correspond to loans.
+ `id`: `1` (required, string)
    The internal transaction id.
+ `modification_number`: `0` (required, string)
+ `original_loan_subsidy_cost`: 234.12 (required, number, nullable)
    Original subsidy cost of the loan. Null for results with award type codes that **do not** correspond to loans.
+ `type`: `A` (required, string)
    Award type code
+ `type_description`: `BPA` (required, string, nullable)

## PageMetaDataObject (object)
+ `page`: 1 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
