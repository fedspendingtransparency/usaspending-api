FORMAT: 1A
HOST: https://api.usaspending.gov

# Transaction Count [/api/v2/awards/count/transaction/{award_id}/]

This endpoint is used for the transaction history tab on the awards page.

## GET

This endpoint returns the number of transactions associated with the given award.

+ Request (application/json)
    A request with an award (contract or assistance) id
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `award_id`: `ASST_NON_NNX17AJ96A_8000` (required, string)

+ Response 200 (application/json)
    + Attributes
        + `transactions` (required, number)
     + Body

            {
                "transactions": 32
            }
