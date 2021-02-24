<ul class="nav nav-stacked" id="sidebar">
  <li><a href="/docs/intro-tutorial">Introductory Tutorial</a></li>
  <li><a href="/docs/using-the-api">Using this API</a></li>
  <li><a href="/docs/endpoints">Endpoints</a></li>
  <li><a href="/docs/recipes">Request Recipes</a>
  <!--<ul>
    <li><a href="#award-recipes">Award Recipes</a></li>
    <li><a href="#postman">Postman Collection</a></li>
  </ul>-->
  </li>

</ul>


[//]: # (Begin Content)

# Award Recipes <a name="award-recipes"></a>

These example requests are for the `/api/v1/awards/` endpoint.


###### Get all Awards for a Specific Agency

This request will find all awards awarded by the Department of Defense, which is a top-tier agency with a CGAC code of '097'.

GET
`/api/v1/awards/?awarding_agency__toptier_agency__toptier_code=097`

POST
```
{
  "filters": [
    {
      "field": "awarding_agency__toptier_agency__toptier_code",
      "operation": "equals",
      "value": "097"
    }
  ]
}
```


###### Get all Awards with a Type of A, B, C, or D

This request will find all awards with types 'A' (BPA Call), 'B' (Purchase Order), 'C' (Delivery Order), or 'D' (Definitive Contract)

POST
```
{
  "filters": [
    {
      "field": "type",
      "operation": "in",
      "value": ["A", "B", "C", "D"]
    }
  ]
}
```


###### Get all Awards which have associated contract data

This request will find all awards with transactions that provide contract data.

POST
```
{
  "filters": [
    {
      "field": "transaction__contract_data",
      "operation": "is_null",
      "value": false
    }
  ]
}
```


###### Get all Awards where the Place of Performance is _not_ in New Jersey

This request will find all awards whose place of performance is not in a location with a state code of 'NJ' (New Jersey)

POST
```
{
  "filters": [
    {
      "field": "place_of_performance__state_code",
      "operation": "not_equals",
      "value": "NJ"
    }
  ]
}
```


# Postman Collections <a name="postman"></a>

[Postman](https://www.getpostman.com/) is a free app for making easy API requests. You can also use it to import and inspect a collection of pre-generated API requests. [Here is a postman collection](https://raw.githubusercontent.com/fedspendingtransparency/usaspending-api/master/usaspending_api/static_doc_files/docs/usaspending_searchpage_postmancollection.json) you can use to see how we generate the visualizations on [the search page](https://www.usaspending.gov/search/).
