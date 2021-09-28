FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Award Amounts [/api/v2/idvs/amounts/{award_id}/]

Used to populate the Federal Account Funding tab on the IDV summary page.

## GET

Get aggregated award counts and funding amounts for IDV contracts 

+ Parameters
    + `award_id`: `CONT_IDV_FA304715A0037_9700` (required, string)
         Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.

+ Response 200 (application/json)
    + Attributes (object)
        + `award_id`: 68841198 (required, number)
        + `generated_unique_award_id`: `CONT_IDV_GS10F0201R_4730` (required, string)
        * `child_idv_count`: 2 (required, number)
        * `child_award_count`: 25 (required, number)
        * `child_award_total_obligation`: 363410.59 (required, number)
        * `child_award_base_and_all_options_value`: 297285.59 (required, number)
        * `child_award_base_exercised_options_val`: 297285.59 (required, number)
        * `child_total_account_outlay`: 0 (required, number)
        * `child_total_account_obligation`: 0 (required, number)
        * `child_award_total_outlay`: 0 (required, number, nullable)
        * `child_account_outlays_by_defc`: [] (required, array[DEFCAmount], fixed-type)
        * `child_account_obligations_by_defc`: [] (required, array[DEFCAmount], fixed-type)
        * `grandchild_award_count`: 54 (required, number)
        * `grandchild_award_total_obligation`: 377145.57 (required, number)
        * `grandchild_award_base_and_all_options_value`: 306964.49 (required, number)
        * `grandchild_award_base_exercised_options_val`: 311020.57 (required, number)
        * `grandchild_total_account_outlay`: 0 (required, number)
        * `grandchild_total_account_obligation`: 0 (required, number)
        * `grandchild_award_total_outlay`: 0 (required, number, nullable)
        * `grandchild_account_outlays_by_defc`: [] (required, array[DEFCAmount], fixed-type)
        * `grandchild_account_obligations_by_defc`: [] (required, array[DEFCAmount], fixed-type)

    + Body

            {
                "award_id": 68841198,
                "generated_unique_award_id": "CONT_IDV_GS10F0201R_4730",
                "child_idv_count": 2,
                "child_award_count": 25,
                "child_award_total_obligation": 363410.59,
                "child_award_base_and_all_options_value": 297285.59,
                "child_award_base_exercised_options_val": 297285.59,
                "grandchild_award_count": 54,
                "grandchild_award_total_obligation": 377145.57,
                "grandchild_award_base_and_all_options_value": 306964.49,
                "grandchild_award_base_exercised_options_val": 311020.57
            }

# Data Structures

## DEFCAmount (object)
+ `code` (required, string)
+ `amount` (required, number)