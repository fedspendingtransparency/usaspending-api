FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Accounts Count [/api/v2/awards/count/federal_account/{award_id}/]

This endpoint is used for the federal accounts tab on the awards page.

## GET

This endpoint returns the number of federal accounts associated with the given award.

+ Request (application/json)
    A request with a award (contract or assistance) id 
    + Parameters
        + `award_id`: `ASST_NON_NNX17AJ96A_8000`

+ Response 200 (application/json)
    + Attributes 
        + `federal_accounts` (required, number)
     + Body
    
            {
                "federal_accounts": 32
            }