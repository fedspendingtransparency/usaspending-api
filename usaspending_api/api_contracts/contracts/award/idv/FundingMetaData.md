FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding Roll-up [/api/v2/awards/idvs/funding_rollup/]

This end point returns award metadata specifying the total transaction obligations, awarding agencies, and federal accounts.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AW_9700_-NONE-_N0018918D0057_-NONE-` (required, string)
+ Response 200 (application/json)
    + Attributes (IDVFundingRollUpResponse)

# Data Structures

## IDVFundingRollUpResponse (object)
+ total_transaction_obligated_amount: 42946881.56 (required, number)
+ awarding_agency_count: 27 (required, number)
+ federal_account_count: 47 (required, number)
