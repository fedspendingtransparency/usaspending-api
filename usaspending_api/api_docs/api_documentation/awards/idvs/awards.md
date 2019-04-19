## IDV Related Awards
**Route:** `/api/v2/awards/idvs/awards/`

**Method:** `POST`

Returns IDVs or contracts related to the requested Indefinite Delivery Vehicle award (IDV).

## Request Parameters

- award_id: (required) ID of award to retrieve. This can either be `generated_unique_award_id` or `id` from awards table.
- type: (optional, default: "child_idvs") "child_idvs" to return child IDVs of the award indicated, "child_awards" to return child awards, or "grandchild_awards" to return grandchild awards.
- limit: (optional, default: 10) The number of records to return.
- page: (optional, default: 1) The page number to return.
- sort: (optional, default: `period_of_performance_start_date`) The field on which to sort results.  Can be one of: `award_type`, `description`, `funding_agency`, `last_date_to_order`, `obligated_amount`, `period_of_performance_current_end_date`, `period_of_performance_start_date`, or `piid`.
- order:  (optional, default `desc`) The sort order.  Can be `desc` or `asc`.

### Response (JSON)

```
{
    "results": [
        {
            "award_id": 8330000,
            "award_type": "DO",
            "description": "4524345064!OTHER GROCERY AND R",
            "funding_agency": "DEPARTMENT OF DEFENSE (DOD)",
            "funding_agency_id": 1219,
            "generated_unique_award_id": "CONT_AW_9700_9700_71T0_SPM30008D3155",
            "last_date_to_order": null,
            "obligated_amount": 4080.71,
            "period_of_performance_current_end_date": "2013-05-06",
            "period_of_performance_start_date": "2013-04-28",
            "piid": "71T0"
        }
    ],
    "page_metadata": {
        "hasPrevious": false,
        "next": 2,
        "hasNext": true,
        "previous": null,
        "page": 1
    }
}
```

### Response Fields

- `award_id`: Internal primary key of Award.
- `award_type`: Type of the award.  See https://fedspendingtransparency.github.io/whitepapers/types/ for a better description.
- `description`: Description of the award as provided by the funding agency.
- `funding_agency`: Name of the agency that paid/is paying for the award.
- `funding_agency_id`: Internal surrogate key of the agency.
- `generated_unique_award_id`: Natural key of Award.
- `last_date_to_order`: The date after which no more orders may be placed against the award.
- `obligated_amount`: The amount of money agreed upon for this award.
- `period_of_performance_current_end_date`: The date after which no additional costs may be incurred.  May be extended.
- `period_of_performance_start_date`: The date before which no costs may be incurred.
- `piid`: Procurement instrument identifier for the award.
- `hasPrevious`: True if there's a previous page.  False if not.
- `hasNext`: True if there's a next page.  False if not.
- `previous`: The previous page number.
- `next`: The next page number.
- `page`: The current page number.


### Errors
Possible HTTP Status Codes:

* 200: On success.
* 400 or 422 for various types of invalid POST data.
