## IDV Funding Rollup (Aggregation)
**Route:** `/api/v2/awards/idvs/funding_rollup/`

**Method:** `POST`

Returns aggregated count of awarding agencies, federal accounts, and total transaction obligated amount for all contracts under an IDV.

## Request Parameters

- award_id: (required) ID of award to retrieve. This can either be `generated_unique_award_id` or `id` from awards table.


### Response (JSON)

```
{
    "total_transaction_obligated_amount": 212038930.38,
    "awarding_agency_count": 3,
    "funding_agency_count": 3,
    "federal_account_count": 4
}
```



### Errors
Possible HTTP Status Codes:

* 200: On success.
* 400 or 422 for various types of invalid POST data.
