FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding Rollup [/api/v2/idvs/funding_rollup/]

This endpoint returns award metadata summing the total transaction obligations and counting awarding agencies, funding agencies, and federal accounts for an IDV's children and grandchildren.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_IDV_N0018918D0057_9700` (required, string)
    + Body


            { "award_id":"CONT_IDV_TMHQ10C0040_2044" }

+ Response 200 (application/json)
    + Attributes (object)
        + `total_transaction_obligated_amount` (required, number)
        + `awarding_agency_count` (required, number)
        + `funding_agency_count` (required, number)
        + `federal_account_count` (required, number)
    + Body


            {
                "total_transaction_obligated_amount": 42946881.56,
                "awarding_agency_count": 27,
                "funding_agency_count": 28,
                "federal_account_count": 47
            }