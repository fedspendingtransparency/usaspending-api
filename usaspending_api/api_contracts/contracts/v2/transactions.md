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
        + `limit` (optional, number)
            The desired page size of results. (Min: 1 / Max: 5000)
            + Default: 10
        + `page` (optional, number)
            The page of results to return based on the page size provided in `limit`.
            + Default: 1
        + `sort` (optional, enum[string])
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
        + `order` (optional, enum[string])
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: `desc`
            + Members
                + `desc`
                + `asc`
    + Body
            
            
            {
                "award_id": "CONT_AWD_N0001917C0015_9700_-NONE-_-NONE-",
                "page": 1,
                "sort": "federal_action_obligation",
                "order": "asc",
                "limit": 5000
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (array[TransactionResult], fixed-type)
        + `page_metadata` (PageMetaDataObject)

    + Body

            {
                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false
                },
                "results": [
                    {
                        "id": "ASST_TX_7022_4337DRFLP00000001_01CPX2002005995_97.036_-NONE-",
                        "type": "04",
                        "type_description": "PROJECT GRANT (B)",
                        "action_date": "2020-01-30",
                        "action_type": "A",
                        "action_type_description": "NEW",
                        "modification_number": null,
                        "description": "GRANT TO LOCAL GOVERNMENT FOR REPAIR OR REPLACEMENT OF DISASTER DAMAGED FACILITIES",
                        "federal_action_obligation": 11041.0,
                        "face_value_loan_guarantee": null,
                        "original_loan_subsidy_cost": null,
                        "cfda_number": "12.345"
                    }
                ]
            }

# Data Structures

## TransactionResult (object)
+ `action_date` (required, string)
    Action date in the format `YYYY-MM-DD`.
+ `action_type` (required, string, nullable)
    Action type code
+ `action_type_description` (required, string, nullable)
+ `description` (required, string, nullable)
+ `face_value_loan_guarantee` (required, number, nullable)
    Face value of the loan. Null for results with award type codes that **do not** correspond to loans.
+ `federal_action_obligation` (required, number, nullable)
    Monetary value of the transaction. Null for results with award type codes that correspond to loans.
+ `id` (required, string)
    The internal transaction id.
+ `modification_number` (required, string)
+ `original_loan_subsidy_cost` (required, number, nullable)
    Original subsidy cost of the loan. Null for results with award type codes that **do not** correspond to loans.
+ `type` (required, string)
    Award type code
+ `type_description` (required, string, nullable)
+ `cfda_number` (optional, string, nullable)

## PageMetaDataObject (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
