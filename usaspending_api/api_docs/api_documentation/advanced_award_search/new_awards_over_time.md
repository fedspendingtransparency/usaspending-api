# State Data

**Route:** /api/v2/search/new_awards_over_time/

**Method:** POST

This route returns a list of time periods with the new awards in the appropriate period within the provided time range
### Request

group (**REQUIRED**): String value. Parameter indicating the time period type ( fiscal year, fiscal quarter, fiscal month)
* `fiscal_year`
* `quarter`
* `month`

filters (**REQUIRED**): accepts a subset of the Advanced Search Filter. See [Filter Object](../search_filters.md) on these two fields:
* `recipient_id`
* `time_period`


## Response Example

```
{
    "group": "quarter",
    "results": [
        {
            "time_period": {
                "fiscal_year": 2015,
                "fiscal_quarter": 1
            },
            "new_award_count_in_period": 1
        },
        {
            "time_period": {
                "fiscal_year": 2011,
                "fiscal_quarter": 3
            },
            "new_award_count_in_period": 1
        },
        {
            "time_period": {
                "fiscal_year": 2011,
                "fiscal_quarter": 1
            },
            "new_award_count_in_period": 1
        },
        {
            "time_period": {
                "fiscal_year": 2010,
                "fiscal_quarter": 4
            },
            "new_award_count_in_period": 1
        },
        {
            "time_period": {
                "fiscal_year": 2010,
                "fiscal_quarter": 3
            },
            "new_award_count_in_period": 3
        },
        {
            "time_period": {
                "fiscal_year": 2010,
                "fiscal_quarter": 1
            },
            "new_award_count_in_period": 1
        },
        {
            "time_period": {
                "fiscal_year": 2008,
                "fiscal_quarter": 1
            },
            "new_award_count_in_period": 2
        }
    ]
}
```

* `group`: The group which was requested
* `time_period`: Object of the period dependent on the requested group.
* `new_award_count_in_period`: The count of new awards records for the recipient in that time period