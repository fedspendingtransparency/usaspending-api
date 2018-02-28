## [Transaction Spending Summary](#transaction-spending-summary)
**Route:** `/api/v2/search/transaction_spending_summary/`

**Method:** `POST`

This route takes award filters, and returns the number of transactions and summation of federal action obligations.

MVP is to only surface prime awards. Later on sub-awards will be added

### Request

filter: Defines how the awards are filtered.  The filter object is defined here.

https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation

#### Example

```
{
    "filters": {
        "keyword": "booz allen",
        "agencies": [
            {
                 "type": "awarding",
                 "tier": "toptier",
                 "name": "Social Security Administration"
            },
            {
                 "type": "awarding",
                 "tier": "subtier",
                 "name": "Social Security Administration"
            },
            {
                 "type": "funding",
                 "tier": "toptier",
                 "name": "Social Security Administration"
            },
            {
                 "type": "funding",
                 "tier": "subtier",
                 "name": "Social Security Administration"
            }
       ],
       "award_amounts": [
              {
                  "lower_bound": 1500000.00,
                  "upper_bound": 1600000.00
              }
       ]
    }
}
```


### Response (JSON)

```
{
    "results": {
        {
            "prime_awards_count": 111111,
            "prime_awards_obligation_amount": 222222.22
        }
    }
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

### Other Search Filters
https://github.com/fedspendingtransparency/usaspending-website/wiki/Award-Search-Visualizations

