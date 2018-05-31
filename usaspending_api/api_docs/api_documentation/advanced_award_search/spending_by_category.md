## [Spending by Award](#spending-by-category)
**Route:** `/api/v2/search/spending_by_category/`

**Method:** `POST`

These endpoints return data that is grouped in preset units to support the various data visualizations on USAspending.gov's Advanced Search page.
### Request

filters (**REQUIRED**): The filters to find with said category. The filter object is defined here: [Filter Object](../search_filters.md).

category (**REQUIRED**): String value. Parameter indicating which category to aggregate and return. Below are the following categories available.

* `awarding_agency`
* `awarding_subagency`
* `recipient_duns`
* `cfda`
* `psc`
* `naics`
* `county`
* `district`

limit (**OPTIONAL**): How many results are returned. If no limit is specified, the limit is set to 5.

page (**OPTIONAL**): The page number that is currently returned. Default is 1.



```
{
  "category": "awarding_agency",
  "filters": {
    "time_period": [
      {
        "start_date": "2016-10-01",
        "end_date": "2017-09-30",
        "date_type": "action_date"
      }
	]
  },
  "limit": 5,
  "page": 1
}
```

### Response (JSON) - Award

```
{
    "category": "awarding_agency",
    "limit": 5,
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    },
    "results": [
        {
            "amount": 384412705.5,
            "name": "Social Security Administration",
            "code": null,
            "id": 28
        },
        {
            "amount": 109671063.48,
            "name": "Department of Defense",
            "code": null,
            "id": 126
        },
        {
            "amount": 95022169.05,
            "name": "Department of Defense",
            "code": null,
            "id": 126
        },
        {
            "amount": 60785689.61,
            "name": "Department of Justice",
            "code": null,
            "id": 17
        },
        {
            "amount": 38284431,
            "name": "Department of Health and Human Services",
            "code": null,
            "id": 68
        }
    ]
}
```

**results Descriptions**

**amount**: summed total obligation/total_subsidy_cost (number)

**name**: Human readable name (string)

**code**: Any additional code associated with the result (string)

**id**: Database ID associated with result (number)

**page_metadata Descriptions**

**page** - The current page number of results.

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
