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
  <li><a href="/docs/endpoints">Endpoints</a></li>

</ul>

[//]: # (Begin Content)

# Introductory Tutorial <a name="introduction"></a>

Welcome to the introductory USAspending API tutorial. This tutorial is designed for people who aren't familiar with APIs and how to use them. If you already know what an _endpoint_ is and the difference between `GET` and `POST`, you'll want to visit [Endpoints](/docs/endpoints). The [Documentation Index](https://api.usaspending.gov/docs/) provides links to introductory resources and tutorials to help you get started.

## What's an API? <a name="whats-an-api"></a>

_API_ stands for _Application Programmer Interface_. APIs make it easy for computer programs to request and receive information in a format they can understand.

If you're looking for federal spending data that's designed to be read by humans instead of computers, you should head to <a href="https://www.usaspending.gov">our website</a>.

## Using the API <a name="using-the-api"></a>

The next few sections will cover different ways to access the API, how to filter the data, how to use autocomplete endpoints, and how to find more information.

You do not need to complete this tutorial in its entirety to get started. Feel free to stop and experiment with your own ideas as you progress.

## Endpoint Overview <a name="endpoint-overview"></a>

When you type a URL into your browser, it usually returns a web page: a document that your browser knows how to display for you to read. APIs use URLs too, but instead of returning formatted web pages, API URLs return data that is structured so computers can easily parse it. API URLs are called _endpoints_. Just as many pages make up a website, many endpoints make up an API.

The USAspending API supports a number of endpoints. For example `/api/v2/search/spending_by_award/` is our Advanced Award Search endpoint for the Spending by Award table.


#### GET vs POST requests <a name="get-vs-post"></a>

Most endpoints support both GET and POST methods for making a request.

Requests for a specific record (with a known numerical identifier) are made via a GET request. For example, a request to `/api/v2/references/agency/456/` would retrieve the agency's metadata with identifier `456`.

You can also use simple filters in a GET request. An example of this would be `/api/v2/financial_balances/agencies?funding_agency=775&fiscal_year=2017`, which would retrieves financial balance information by funding agency, with the identifier of `775` and a fiscal year of `2017`.

POST requests are leveraged in situations where more advanced filtering is required. For example, `/api/v2/search/spending_by_award/` would have filters as such:
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
  },
  "fields": ["Award ID", "Recipient Name", "Start Date", "End Date", "Award Amount", "Awarding Agency", "Awarding Sub Agency", "Award Type", "Funding Agency", "Funding Sub Agency"],
  "sort": "Recipient Name",
  "order": "desc"
}
```

For more details on USAspending API requests, please continue to the full API documentation [here](/docs/endpoints)
