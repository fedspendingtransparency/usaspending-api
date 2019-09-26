## [Spending by Geography](#spending-by-geography)
**Route:** `/api/v2/search/spending_by_geography/`

**Method:** `POST`

This route takes award filters, and returns spending by state code, county code, or congressional district code.

### Request
scope: What type of data will be returned. Must be either: `place_of_performance` or `recipient_location`.

subawards (**OPTIONAL**): boolean value.  True when you want to group by Subawards instead of Awards.  Defaulted to False.

geo_layer: Defines which geographical level should be returned in the request. Options include: "state", "county", "district"

geo_layer_filter (**OPTIONAL**): Defines a filter for a specific geographic area correlating to the geo_layer. It is a list of strings that are the unique identifiers for the geographic location.

- When `geo_layer` is `"state"` then the `geo_layer_filters` should be an array of state codes ex: `["MN", "WA", "DC"]`.
- When `geo_layer` is `"county"` then the `geo_layer_filters` should be an array of county codes. County codes are the county's state FIPS code concatenated with the county's FIPS code. ex: `["51041", "51117", "51179"]`.
- When `geo_layer` is `"district"` then the `geo_layer_filters` should be an array of congressional district codes. The congressional district code is a concatenation of the state FIPS code + the Congressional District code including any leading zeros. ex: `["5109", "5109", "5109"]`.

filters: how the awards are filtered.  The filter object is defined here: [Filter Object](../search_filters.md)


```JSON
{
    "scope": "place_of_performance",
    "geo_layer": "state",
    "geo_layer_filters": ["MN", "WA", "DC"],
    "filters": {
        "award_type_codes": ["A", "B", "03"],
        "award_ids": ["1", "2", "3"],
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
        ]
    },
    "subawards": false
}
```


### Response (JSON) - State

```JSON
{
    "scope": "place_of_performance",
    "geo_layer": "state",
    "results": [
        {
            "shape_code": "MN",
            "display_name": "Minnesota",
            "aggregated_amount": 0
        },
        {
            "shape_code": "DC",
            "display_name": "District of Columbia",
            "aggregated_amount": 6393118.28
        },
        {
            "shape_code": "VA",
            "display_name": "Virginia",
            "aggregated_amount": 73700
        }
    ]
}

```

### Response (JSON) - County

```JSON
{
    "scope": "place_of_performance",
    "geo_layer": "county",
    "results": [
        {
            "shape_code": "51041",
            "display_name": "Chesterfield County",
            "aggregated_amount": 73700.27
        }
    ]
}

```

### Response (JSON) - Congressional District

```JSON
{
    "scope": "place_of_performance",
    "geo_layer": "district",
    "results": [
        {
            "shape_code": "5109",
            "display_name": "VA-09",
            "aggregated_amount": 47283.82
        }
    ]
}

```

### Response Fields
* `scope`: Choices are `place_of_performance` or `recipient_location` based on user's request
* `geo_layer`: Choices are `state`, `country`, or `district`  that is based on user's request 
* `shape_code`: Identifier used for mapping that is based on the `geo_layer`
* `display_name`: Display name for `shape code` for labels on map
* `aggregated_amount`: Sum of `federal_action_obligation` from the filtered transactions 

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
    "detail": "Sample error message"
}
```
