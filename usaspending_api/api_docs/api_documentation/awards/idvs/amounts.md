## IDV Award Amounts
**Route:** `/api/v2/awards/idvs/amounts/<requested_award>/`

**Method:** `GET`

Returns counts and dollar figures for a specific Indefinite Delivery Vehicle award (IDV).

## Request Parameters

- requested_award: (required) ID of award to retrieve. This can either be `generated_unique_award_id` or `id` from awards table

### Response (JSON)

```
{
    "idv_count": 0,
    "contract_count": 2,
    "rollup_total_obligation": "106321.10",
    "rollup_base_and_all_options_value": "106321.10",
    "rollup_base_exercised_options_val": "0.00",
    "award_id": "CONT_AW_1540_-NONE-_DJB30605051_-NONE-"
}
```

### Response Fields

- `idv_count`: Count of child IDVs.
- `contract_count`: Count of direct child contracts.  Does not include grandchildren.
- `rollup_total_obligation`: Sum of `total_obligation` for all child and grandchild contracts.
- `rollup_base_and_all_options_value`: Sum of `base_and_all_options_value` for all child and grandchild contracts.
- `rollup_base_exercised_options_val`: Sum of `base_exercised_options_val` for all child and grandchild contracts.
- `award_id`: Natural key for the award.


### Errors
Possible HTTP Status Codes:

* 404: For malformed `requested_award` id.
* 200: For non-existent `requested_award`.  Returns:
```
{
    "message": "No IDV award found with this id"
}
```
