## [Spending by Award Count](#spending-by-award-count)
**Route:** `/api/v2/search/spending_by_award_count/`

**Method:** `POST`

This route takes award filters, and returns the number of awards in each award type (Contracts, Loans, Gran.

### Request

filter: Defines how the awards are filtered.  The filter object is defined here.

https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation


```
{
  "filters": {
       "award_type_codes": ["10"],
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
       "legal_entities": [779928],
       "recipient_scope": "domestic",
       "recipient_locations": [650597],
       "recipient_type_names": ["Individual"],
       "place_of_performance_scope": "domestic",
       "place_of_performance_locations": [60323],
       "award_amounts": [
              {
                  "lower_bound": 1500000.00,
                  "upper_bound": 1600000.00
              }
       ],
       "award_ids": [1018950]
  }
}
```


### Response (JSON)

```
{
    "results": {
        "grants": 0,
        "loans": 0,
        "contracts": 0,
        "direct_payments": 1,
        "other": 0
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
