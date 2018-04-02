## [Spending over Time](#spending-over-time)
**Route:** `/api/v2/search/spending_over_time/`

**Method:** `POST`

This route takes award filters, and returns spending by time.  The amount of time is denoted by the "group" value.

### Request
group: the unit of time that awards are aggregated by.  You must use one of: month, quarter, fiscal_year.

filters: how the awards are filtered.  The filter object is defined here.

subawards (**OPTIONAL**): boolean value.  True when you want to group by Subawards instead of Awards.  Defaulted to False.

[Filter Object](../search_filters.md)

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
    },
    "subawards": false
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

**Response results Descriptions**

**time_period** - an object containing what time period the awards amount is filtered by.  This keys in this object are defined by the group request variable.

**hasNext** - Boolean object. If true, there is another page of results.


### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
    "detail": "Sample error message"
}
```
