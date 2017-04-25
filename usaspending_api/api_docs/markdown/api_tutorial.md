<ul class="nav nav-stacked" id="sidebar">
  <li><a href="/docs/intro-tutorial">Introductory Tutorial</a>
  <!--<ul class="">
    <li><a href="#introduction">Introduction</a></li>
    <li><a href="#whats-an-api">What's an API?</a></li>
    <li><a href="#using-the-api">Using the API</a></li>
    <li><a href="#endpoint-overview">Endpoint Overview</a></li>
    <li><a href="#data-endpoints">Data Endpoints</a></li>
    <li><a href="#get-vs-post">GET vs POST Requests</a></li>
    <li><a href="#filtering">Filtering</a></li>
    <li><a href="#ordering">Ordering Responses</a></li>
    <li><a href="#pagination">Pagination</a></li>
    <li><a href="#aggregation">Aggregation</a></li>
    <li><a href="#other">Other Information</a></li>
  </ul>-->
  </li>
  <li><a href="/docs/using-the-api">Using this API</a></li>
  <li><a href="/docs/endpoints">Endpoints</a></li>
  <li><a href="/docs/data-dictionary">Data Dictionary</a></li>
  <li><a href="/docs/recipes">Request Recipes</a></li>

</ul>

[//]: # (Begin Content)

# Introductory Tutorial <a name="introduction"></a>

Welcome to the introductory USASpending API tutorial. This tutorial is designed for people who aren't familiar with APIs and how to use them. If you already know what an "endpoint" is and the difference between GET and POST, you'll want to visit [Using the API](/docs/using-the-api), or jump right into the data with some [request recipes](/docs/recipes/).

## What's an API? <a name="whats-an-api"></a>

"API" stands for "Application Programmer Interface." APIs make it easy for computer programs to request and recieve information in a format they can understand.

If you're looking for federal spending data that's designed to be read by humans instead of computer programs, you should head to <a href="https://www.usaspending.gov/Pages/Default.aspx">usaspending.gov</a>, or visit <a href="https://openbeta.usaspending.gov">openbeta.usaspending.gov</a> for information on the updated version of the site that's currently in development.

## Using the API <a name="using-the-api"></a>

Over the next few sections, we will discuss the different methods for accessing the API, how to filter the data, how to use autocomplete endpoints, and how to find more information.

You do not need to complete this tutorial in its entirety to get started. Feel free to stop and experiment with your own ideas as you progress.

## Endpoint Overview <a name="endpoint-overview"></a>

When you type a url into your browser, it usually returns a web page: a document that your browser knows how to display for you to read. APIs use urls, too--but instead of returning formated web pages, API urls return data structured to be easy for computers to parse. API urls are called "endpoints." Just as many webpages make up a web site, many endpoints make up an API.

The USASpending API supports a number of endpoints. For example `/api/v1/awards/` is our awards endpoint.

Our endpoints are broken into a few groups:

* Data endpoints - These endpoints return records from our database of federal spending information.
* Aggregation endpoints - These endpoints work like formulas in spreadsheet programs, allowing you to add and count records in the database that match your filter parameters.
* Autocomplete endpoints - We won't be covering autocomplete endpoints in this guide, as they're meant for advanced users. Developers who wish to incorporate our autocomplete functionality into their user interfaces should refer to [Using the API](/docs/using-the-api) for details.

### Data Endpoints <a name="data-endpoints"></a>

In this section, we will be discussing _Data Endpoints_.

Each data endpoint accesses a different subset of the total universe of the data stored on USASpending. For example, the endpoint `/api/v1/awards/` accesses information at the <span title="An award is comprised of multiple actions (known as transactions)">award<sup>?</sup></span> level; whereas `/api/v1/transactions/` accesses information on individual <span title="A transaction represents a specific contract or assistance action">transactions<sup>?</sup></span>.

For a comprehensive list of endpoints and their data, please see the [USASpending API Data Dictionary](/docs/data-dictionary).

#### Responses

Responses for data endpoints are json objects and follow the same structure, generally speaking:

```
{
  "total_metadata": {
      "count": 500              // The total number of records matching your query
  },
  "page_metadata": {
      "num_pages": 5,           // Total number of pages for this query
      "page_number": 1,         // The page you're currently on
      "count": 100              // The number of objects on this page
  },
  "results": [ . . . ]          // An array of records matching your query
}
```

For more information on the record objects for each endpoint, check out the [data dictionary](/docs/data-dictionary).

#### GET vs POST requests <a name="get-vs-post"></a>

Most endpoints support both GET and POST methods for making a request.

Requests for a specific record where the <span title="This is a numerical identifier referencing the specific item">identifier<sup>?</sup></span> is known are done via a GET request. For example, a request to `/api/v1/awards/1234` would retreive the award with identifier `1234`.

Simple filters can also be used in a GET request. An example of this would be `/api/v1/awards/?awarding_agency=1788`[<sup>Try it!</sup>](/api/v1/awards/?awarding_agency=1788) would return all awards where the <span title="The government department, agency, or office which awarded the associated award">`awarding_agency`<sup>?</sup></span> was set to `1788`.

POST requests are used when more advanced filtering is required. For example, if we wanted to search for awards with signing dates between June 1st 2016 and June 1st 2017, we would need to construct a complex filter and POST it to the `/api/v1/awards/` endpoint. This example is created in the filtering section of this tutorial.


#### Filtering <a name="filtering"></a>

##### GET Filtering
Filtering on GET requests is done by specifying a field in the URL, and what value that field should have. Multiple filters can be chained with `&`.

`/api/v1/awards/?type=A&piid=LB01` [<sup>Try it!</sup>](/api/v1/awards/?type=A&piid=LB01)

This GET request would return awards where the <span title="A code identifying the type of award, for example, a BPA call or Direct Loan">award type<sup>?</sup></span> is `A` and the <span title="A type of award identifier used for contracts">piid<sup>?</sup></span> is 'LB01'.

##### POST Filtering

Complex filters can be constructed using POST requests. Let's construct a json object we can POST to search for awards with signing dates between June 1st 2016 and June 1st 2017. First, let's take a look at an empty post request.

```
{
    "filters": []
}
```

We see here an empty post request. The `filters` parameter is an array of filters. When multiple filters are present in this array, they are joined together via a logical AND. That is, if we specify two (or more!) filters, records must match all filters to be returned. We will cover 'OR'ing filters later in this section.

The first filter we will create is to check if the signing date is on or after June 1st. The reference we will use to construct this filter is the [Using the API](/docs/using-the-api) documentation. A filter is comprised of three separate parts: the `field`, the `operation`, and the `value`. The `field` is the particular field we want to filter on. The filter uses the specified `operation` to compare the `value` given to the value of each record stored in the specified `field`. We're interested in the signed date for these awards, so we will used the field `date_signed`. (We found that in the [data dictionary](/docs/data-dictionary)!)

For our operation, we will use `greater_than_or_equal` because we want to know if our signed date is on or after June 1st 2016. Which makes our value `2016-06-01`, the API uses the standard <span title="ISO format is YYYY-MM-DD">ISO date format<sup>?</sup></span>. Now that we have all of our pieces, let's put it all together.

```
{
    "filters": [
      {
        "field": "date_signed",
        "operation": "greater_than_or_equal",
        "value": "2016-06-01"
      }
    ]
}
```

Not bad! This request will get us all award records with a signing date after June 1st 2016. To try it, we can navigate to [/api/v1/awards/](/api/v1/awards/) and paste our request into the 'Raw Data' form at the bottom and then clicking 'POST'.

We're only halfway done with our request. To establish the upper bound on our date range, we will need another filter. It will look similar to our first one, except our operation will now be `less_than` and our value will be `2017-06-01`. We can simply add this filter into our list.

```
{
    "filters": [
      {
        "field": "date_signed",
        "operation": "greater_than_or_equal",
        "value": "2016-06-01"
      },
      {
        "field": "date_signed",
        "operation": "less_than",
        "value": "2017-06-01"
      }
    ]
}
```

There we have it: a post request that finds all award records from 2016-06-01 to 2017-06-01. This is just the start, we can combine many different operations to construct very versatile filters. Check out the [request recipes](/docs/recipes) for some ideas.

##### OR filters via POST

Sometimes we don't want to match all of our filters, but we want to match any of them. For this case, we can use a special filter parameter called `combine_method`, which is also documented in [Using the API](/docs/using-the-api).

A filter with `combine_method` is special because it does not specify the usual parameters of `field`, `operation`, and `value`. Instead, a filter with a `combine_method` contains within itself another array of filters which should be combined with that method. For example, the following request matches award records with type 'A' _or_ type 'B':

```
{
    "filters": [
        {
          "combine_method": "OR",
          "filters": [
            {
              "field": "type",
              "operation": "equals",
              "value": "A"
            },
            {
              "field": "type",
              "operation": "equals",
              "value": "B"
            }
          ]
        }
    ]
}
```

These special filters can be nested inside or beside one another, allowing us to create logically complex filters. For example, let's look at a request that would find award records which:

* Have a signing date on or after June 1st, 2016
* Have a type of A *OR* a type of B, but only if the signed date is before June 1st, 2017

```
{
    "filters": [
        {
          "field": "date_signed",
          "operation": "greater_than_or_equal",
          "value": "2016-06-01"
        },
        {
          "combine_method": "OR",
          "filters": [
            {
              "field": "type",
              "operation": "equals",
              "value": "A"
            },
            {
              "combine_method": "AND",
              "filters": [
                {
                  "field": "type",
                  "operation": "equals",
                  "value": "B"
                },
                {
                  "field": "date_signed",
                  "operation": "less_than",
                  "value": "2017-06-01"
                }
              ]
            }
          ]
        }
    ]
}
```

This method of filtering is a little complex, but very flexible.

##### Fields and Nested Objects

Filtering on <span title="Fields belonging to the type of record directly matching the endpoint, instead of a referenced object">top level fields<sup>?</sup></span> is nice, but the real power of the API is in linking data together. As we use the API, you may notice that the <span title="The data returned by an endpoint after a request">response objects<sup>?</sup></span> have objects nested within them. These are other records referenced by the record matching your query, and are included for convenience. However, you _can_ filter on them! Let's look at an example of that in both a GET and POST request.

If we look at `/api/v1/awards/` we can see that most award records have a recipient - the company or entity who received the award. Let's make a filter to find all contracts awarded to `GENERAL ELECTRIC COMPANY`. When we want to traverse into a nested object, we use a double underscore `__` and attach the nested object's field. So, in this case, we want to use the `recipient_name` field from the nested object called `recipient`, so our filter field is `recipient__recipient_name`.

(This is known as "foreign key traversal" throughout the API documentation. Most of the time, if you are specifying a field to the API, you can use foreign key traversal).

Let's perform this request using GET:

`/api/v1/awards/?recipient__recipient_name=GENERAL%20ELECTRIC%20COMPANY`[<sup>Try it!</sup>](/api/v1/awards/?recipient__recipient_name=GENERAL%20ELECTRIC%20COMPANY)

Since this is a get request, we had to encode our spaces as `%20`, but this request will find all award records where the recipient's name is `GENERAL ELECTRIC COMPANY`. The same filter, but using POST looks like:

```
{
    "filters": [
      {
        "field": "recipient__recipient_name",
        "operation": "equals",
        "value": "GENERAL ELECTRIC COMPANY"
      }
    ]
}
```

We can try this out the same way by opening [`/api/v1/awards/`](/api/v1/awards/) and pasting that request into the 'Raw Data' form at the bottom.

##### Other POST request options

The POST method supports many other options. For instance, if you only want to view the description and recipients of an awards, you can send the following request via POST to `/api/v1/awards/`

```
{
  "fields": ["description", "recipient"]
}
```

Or, if you want to get ever field _except_ the type:

```
{
  "exclude": ["type"]
}
```

By default, most endpoints only show you a subset of generally useful information. If you want to get _every_ field on the record, without requesting them manually, you can specify your request as being `verbose`:

```
{
  "verbose": true
}
```

You can even combine these with filters! Here's a request that gets only the type of the award records for awards where the signed date is after June 1st, 2016:

```
{
  "fields": ["type"],
  "filters": [
    {
      "field": "date_signed",
      "operation": "greater_than",
      "value": "2016-06-01"
    }
  ]
}
```


#### Ordering Responses <a name="ordering"></a>

One of the most powerful extra POST request parameters is the `order` parameter. This allows you to order the response by any field you wish to specify. For example, to order a request to `/api/v1/awards/` by recipient name:

```
{
  "order": ["recipient__recipient_name"]
}
```

To reverse the order, simply add a `-` before the field.

```
{
  "order": ["-recipient__recipient_name"]
}
```

You can order by multiple fields, just like in a spreadsheet. A request with `"order" = ["recipient__location__country_code", "recipient__recipient_name"]` would first order by each recipient's location's country code, and then by recipient name.

#### Pagination <a name="pagination"></a>

The amount of data in the API is quite enormous, and to support the usability of the data the API provides pagination on all requests. The default page limit is 100 entries per page, though you may specify a larger or smaller amount. Let's look at a GET request that changes these values.

`/api/v1/awards/?page=5&limit=10` [<sup>Try it!</sup>](/api/v1/awards/?page=5&limit=10)

The request above sets the `limit` (i.e. number of entries per page) to 10, and requests page 5. Likewise, the same request via POST looks like the following request.

```
{
  "page": 5,
  "limit": 10
}
```

You can combine these POST parameters with any other POST parameter or filters.


### Aggregation Requests <a name="aggregation"></a>

Aggregation endpoints allow you to perform simple aggregations on the data. For example, let's say you want to get the sum of all <span title="The amount of money obligated by the federal government">total obligations<sup>?</sup></span> for each award record, summed up by fiscal year. Sure, you could query the data, gather it all up, and process it - or we can use an aggregation endpoint.

Currently, they only support POST requests.

Let's try it with `/api/v1/awards/total`.

```
{
    "field": "total_obligation",
    "group": "date_signed__fy"
}
```

Here we specify the `field` as `total_obligation` and that we're grouping by each record's signing date's fiscal year. The response we recieve looks something like this:

```
{
  "results": [
    {
      "item": "2017",               // The "item" is the entry for your grouping
      "aggregate": "583609881.96"   // The aggregate here is the sum
    },
    {
      "item": "2016",
      "aggregate": "1147421567.66"
    },
    {
      "item": "2014",
      "aggregate": "5126678045.39"
    },
    {
      "item": "2015",
      "aggregate": "2006587943.39"
    }
  ],
  "total_metadata": {
    "count": 5
  },
  "page_metadata": {
    "num_pages": 1,
    "page_number": 1,
    "count": 5
  }
}
```

We can get even fancier, and count how many records is signed in each month by using `date_part` and `aggregate`:

```
{
    "field": "total_obligation",
    "group": "date_signed",
    "aggregate": "count",
    "date_part": "month"
}
```

By specifying our `aggregate` to `count`, we're telling the API to count how many records match out filters. Specifying the `date_part` breaks our grouping out into months. Our response looks something like:

```
{
  "results": [
    {
      "item": "10",               // The numerical month (October!)
      "aggregate": "34510.00"     // Here, this is the number of awards signed in this month
    },
    {
      "item": "9",
      "aggregate": "340.00"
    },
    {
      "item": "1",
      "aggregate": "1204.00"
    },
    {
      "item": "12",
      "aggregate": "1712.00"
    },
    {
      "item": "11",
      "aggregate": "2266.00"
    },
    {
      "item": "2",
      "aggregate": "544.00"
    }
  ],
  "total_metadata": {
    "count": 7
  },
  "page_metadata": {
    "num_pages": 1,
    "page_number": 1,
    "count": 7
  }
}
```

Why are so many more awards signed in October? It's the start of the fiscal year.

This isn't the full extent of what the aggregation endpoints can do. For more information, consult [Using the API](/docs/using-the-api)

### Other Information  <a name="other"></a>

If you have made it this far, hopefully you feel comfortable making GET and POST requests to the API, and constructing your own filters. For more information and documentation, visit our [documentation hub](/docs/).
