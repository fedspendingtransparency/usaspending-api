FORMAT: 1A
HOST: https://api.usaspending.gov

# Subaward Count [/api/v2/awards/subaward_count/{award_id}/]

This endpoint is used for the subawards tab on the awards page.

## GET

This endpoint returns the number of subawards associated with the given award.

+ Request (application/json)
    A request with a award (contract or assistance) id
    + Parameters
        + `award_id`: `ASST_NON_NNX17AJ96A_8000`

+ Response 200 (application/json)
    + Attributes 
        + `subawards` (required, number)
     + Body
    
            {
                "subawards": 32
            }