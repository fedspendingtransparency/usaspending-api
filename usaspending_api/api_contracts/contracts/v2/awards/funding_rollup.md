FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Funding Rollup [/api/v2/awards/funding_rollup/]

This endpoint returns the total transaction obligations and count of awarding agencies, funding agencies, and federal accounts for an award.

## POST

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AWD_DEAC5206NA25396_8900_-NONE-_-NONE-` (required, string)
    + Body

            {
                "award_id":"CONT_AWD_N0001902C3002_9700_-NONE-_-NONE-",
                "page":1,
                "sort":"reporting_fiscal_date",
                "order":"asc",
                "limit":15
            }
+ Response 200 (application/json)
    + Attributes
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
