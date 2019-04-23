FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Award Amounts [/api/v2/awards/idvs/amounts/{award_id}/]

Returns aggregated award counts and funding amounts for IDV (Indefinite Delivery Vehicle) contracts.  Used to populate the Federal Account Funding tab on the IDV summary page.

## GET

+ Parameters
    + `award_id`: `CONT_AW_4730_-NONE-_GS23F0170L_-NONE-` (required, string)
         Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.

+ Response 200 (application/json)
    + Attributes
        + `award_id`: 68841198 (required, number)
        + `generated_unique_award_id`: `CONT_AW_4730_-NONE-_GS10F0201R_-NONE-` (required, string)
        * `child_idv_count`: 2 (required, number)
        * `child_award_count`: 25 (required, number)
        * `child_award_total_obligation`: 363410.59 (required, number)
        * `child_award_base_and_all_options_value`: 297285.59 (required, number)
        * `child_award_base_exercised_options_val`: 297285.59 (required, number)
        * `grandchild_award_count`: 54 (required, number)
        * `grandchild_award_total_obligation`: 377145.57 (required, number)
        * `grandchild_award_base_and_all_options_value`: 306964.49 (required, number)
        * `grandchild_award_base_exercised_options_val`: 311020.57 (required, number)

    + Body

            {
                "award_id": 68841198,
                "generated_unique_award_id": "CONT_AW_4730_-NONE-_GS10F0201R_-NONE-",
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
