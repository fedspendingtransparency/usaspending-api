## [Download Count](#usaspending-api-documentation)

**Route:** ` /v2/download/count/`

**Method:** `POST`

Returns the number of transactions that would be included in a download request for the given filter set.

### Request

POST a JSON body:

```
{
    "filters": {
        "time_period": [
            {
                "start_date": "2001-01-01",
                "end_date": "2001-01-31"
            }
        ]
    }
}
```

#### Request Parameters Description

* `filters` - *required* - a standard [Search v2 JSON](https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation) filter object

### Response

```
{
    "transaction_rows_gt_limit": true
}
```

* `transaction_rows_gt_limit` is a boolean returning whether the transaction count is over the maximum row limit.

**Note:** This endpoint will only count the rows for _transactions_. Frontend will disable the download button whenever transaction row count exceeds 500,000 regardless of the award row count. We'll name the key `transaction_rows` in case we need to add award counts in the future.
