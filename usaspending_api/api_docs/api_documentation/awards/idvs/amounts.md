## IDV Award Amounts
**Route:** `/api/v2/awards/idvs/amounts/<requested_award>/`

**Method:** `GET`

Returns counts and dollar figures for a specific Indefinite Delivery Vehicle award (IDV).

## Request Parameters

- requested_award: (required) ID of award to retrieve. This can either be `generated_unique_award_id` or `id` from awards table.

### Response (JSON)

```
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
```

### Response Fields

- `award_id`: Internal primary key of Award.
- `generated_unique_award_id`: Natural key of Award.
- `child_idv_count`: Count of child IDVs.
- `child_award_count`: Count of child awards.
- `child_award_total_obligation`: Sum of `total_obligation` for child awards.
- `child_award_base_and_all_options_value`: Sum of `base_and_all_options_value` for child awards.
- `child_award_base_exercised_options_val`: Sum of `base_exercised_options_val` for child awards.
- `grandchild_award_count`: Count of grandchild awards.
- `grandchild_award_total_obligation`: Sum of `total_obligation` for grandchild awards.
- `grandchild_award_base_and_all_options_value`: Sum of `base_and_all_options_value` for grandchild awards.
- `grandchild_award_base_exercised_options_val`: Sum of `base_exercised_options_val` for grandchild awards.


### Errors
Possible HTTP Status Codes:

* 200: On success.
* 404: For malformed or otherwise invalid `requested_award` id.
