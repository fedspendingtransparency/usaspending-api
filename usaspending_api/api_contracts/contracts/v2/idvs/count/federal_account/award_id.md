FORMAT: 1A
HOST: https://api.usaspending.gov
            
# Federal Accounts Count [/api/v2/idvs/count/federal_account/{award_id}/{?piid}]

This endpoint is used for the federal accounts tab on the idv summary page.

## GET

This endpoint returns the number of federal accounts associated with the given IDV and it's children and grandchildren.

+ Request (application/json)
    A request with a award (contract or assistance) id 
    + Parameters        
        + `award_id`: `CONT_IDV_NNK14MA74C_8000` (required, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
        + `piid`: `NNK17MA01T` (optional, string)
            Award ID to further refine results.  All File C financial data for this award is returned if omitted.

+ Response 200 (application/json)
    + Attributes 
        + `count` (required, number)
     + Body
    
            {
                "count": 2
            }