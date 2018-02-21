## [Spending over Time](#spending-over-time)
**Route:** `/api/v2/search/spending_over_time/`

**Method:** `POST`

This route takes award filters, and returns spending by time.  The amount of time is denoted by the "group" value.

### Request
group: how the data is broken up.  ex: `"quarter", "fiscal_year", "month"`

filter: how the awards are filtered.  The filter object is defined here.

https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation

```
{
    "group": "quarter",
    "filters": {
        "award_type_codes": ["A", "B", "03"],
        "award_ids": [1, 2, 3],
        "award_amounts": [
              {
            "lower_bound": 1000000.00,
            "upper_bound": 25000000.00
              },
              {
            "upper_bound": 1000000.00
              },
              {
            "lower_bound": 500000000.00
              }
             ],
    }
}
```


### Response (JSON)

```
{
    "group": "quarter"
    "results": [
        {
            "time_period": {"fiscal_year": "2017", "quarter": "3"},
                "aggregated_amount": "200000000"
        },
        ....

    ]
}
```

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```
