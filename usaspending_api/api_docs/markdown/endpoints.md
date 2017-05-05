
<ul class="nav nav-stacked" id="sidebar">
  <li><a href="/docs/intro-tutorial">Introductory Tutorial</a></li>
  <li><a href="/docs/using-the-api">Using this API</a></li>
  <li><a href="/docs/endpoints">Endpoints</a>
  <!--<ul>
    <li><a href="#status-codes">Status Codes</a></li>
    <li><a href="#endpoints-and-methods">Endpoints and Methods</a></li>
    <li><a href="#endpoint-index">Endpoint Index</a></li>
  </ul>-->  
  </li>
  <li><a href="/docs/data-dictionary">Data Dictionary</a></li>
  <li><a href="/docs/recipes">Request Recipes</a></li>

</ul>

[//]: # (Begin Content)

# API Endpoints

This page is intended for users who are already familiar with APIs. If you're not sure what "endpoint" means, and what GET and POST requests are, you may find the [introductory tutorial](/docs/intro-tutorial) more useful to start.

While the API is under development, we are gradually increasing the amount of available data, which is currently limited to a few federal agency submissions and small slices of USAspending.gov historical data.

Endpoints do not currently require any authorization.

### Status Codes <a name="status-codes"></a>
In general, status codes returned are as follows:

* 200 if successful
* 400 if the request is malformed
* 500 for server-side errors

## Endpoints and Methods <a name="endpoints-and-methods"></a>

The currently available endpoints are listed below. Our [data dictionary](/docs/data-dictionary) provides more comprehensive definitions of the technical terms and government-specific language we use in the API.

To reduce unnecessary data transfer, most endpoints return a default set of information about the items being requested. To override the default field list, use the `fields`, `exclude`, and `verbose` options (see [POST Requests](#post-requests) for more information).

## Endpoint Index <a name="endpoint-index"></a>

| Endpoint | Methods | Response Object | Data
| -------- | ---: | ------ | ------ |
| [/api/v1/awards/](/api/v1/awards/) | GET, POST | <a href="#award">Awards</a> | Returns a list of award records |
| /api/v1/awards/:id | GET, POST | <a href="#award">Award</a> | Returns a single award records with all fields |
| [/api/v1/awards/autocomplete/](/api/v1/awards/autocomplete/) | POST | Autocomplete (see [Using the API](/docs/using-the-api))| Supports autocomplete on award records |
| [/api/v1/awards/total/](/api/v1/awards/total/) | POST |  Aggregate (see [Using the API](/docs/using-the-api)) | Supports aggregation on award records |
| [/api/v1/federal_accounts/](/api/v1/federal_accounts/) | GET, POST | <a href="#federal-account">Federal Account</a> | Returns a list of federal accounts |
| [/api/v1/federal_accounts/autocomplete/](/api/v1/federal_accounts/autocomplete/) | POST | Autocomplete (see [Using the API](/docs/using-the-api))| Supports autocomplete on federal account records |
| [/api/v1/tas/balances/](/api/v1/tas/balances/) | GET, POST | <a href="#appropriation-account">Yearly Appropriation Account Balances</a> | Returns a list of appropriation account balances by fiscal year |
| [/api/v1/tas/balances/total/](/api/v1/tas/balances/total/) | POST |  Aggregate (see [Using the API](/docs/using-the-api)) | Supports aggregation on appropriation account records |
| [/api/v1/tas/balances/quarters/](/api/v1/tas/balances/quarters/) | GET, POST | <a href="#appropriation-account-balances-quarterly">Quarterly Appropriation Account Balances</a> | Returns a list of appropriation account balances by fiscal quarter|
| [/api/v1/tas/balances/quarters/total/](/api/v1/tas/balances/quarters/total/) | POST |  Aggregate (see [Using the API](/docs/using-the-api)) | Supports aggregation on quarterly appropriation account records |
| [/api/v1/tas/categories/](/api/v1/tas/categories/) | GET, POST | <a href="#accounts-prg-obj">Yearly Appropriation Account Balances (by Category)</a> | Returns a list of appropriation account balances by fiscal year broken up by program activities and object class |
| [/api/v1/tas/categories/total/](/api/v1/tas/categories/total/) | POST |  Aggregate (see [Using the API](/docs/using-the-api)) | Supports aggregation on appropriation account (by category) records |
| [/api/v1/tas/categories/quarters/](/api/v1/tas/categories/quarters/) | GET, POST | <a href="#accounts-prg-obj-quarterly">Quarterly Appropriation Account Balances (by Category)</a> | Returns a list of appropriation account balances by fiscal quarter broken up by program activities and object class |
| [/api/v1/tas/categories/quarters/total/](/api/v1/tas/categories/quarters/total/) | POST |  Aggregate (see [Using the API](/docs/using-the-api)) | Supports aggregation on quarterly appropriation account (by category) records |
| [/api/v1/tas/](/api/v1/tas/) | GET, POST | <a href="#tas">Treasury Appropriation Account</a> | Returns a list of treasury appropriation accounts, by TAS |
| [/api/v1/tas/autocomplete/](/api/v1/tas/autocomplete/) | POST | Autocomplete (see [Using the API](/docs/using-the-api))| Supports autocomplete on TAS records |
| [/api/v1/accounts/awards/](/api/v1/accounts/awards/) | GET, POST | <a href="#accounts-by-award">Financial Accounts (by Award)</a> | Returns a list of financial account data grouped by TAS and broken up by Program Activity and Object Class codes |
| /api/v1/accounts/awards/:id | GET, POST | <a href="#accounts-by-award">Financial Account (by Award)</a> | Returns a single financial account record, grouped by TAS, with all fields |
| [/api/v1/transactions/](/api/v1/transactions/) | GET, POST | <a href="#transaction">Transaction</a> | Returns a list of transactions - contracts, grants, loans, etc. |
| /api/v1/transactions/:id | GET, POST | <a href="#transaction">Transaction</a> | Returns a single transaction record with all fields |
| [/api/v1/transactions/total/](/api/v1/transactions/total/) | POST | Aggregate (see [Using the API](/docs/using-the-api)) | Supports aggregation on transaction records |
| [/api/v1/references/locations/](/api/v1/references/locations/) | POST | <a href="#locations">Location</a> | Returns a list of locations - places of performance or vendor locations |
| [/api/v1/references/locations/geocomplete/](/api/v1/references/locations/geocomplete/) | POST | Location Hierarchy (see [Using the API](/docs/using-the-api)) | Supports geocomplete queries, see [Using the API](/docs/using-the-api) |
| [/api/v1/references/agency/](/api/v1/references/agency/) | GET, POST | <a href="#agencies">Agency</a> | Returns a list of agency records |
| [/api/v1/references/agency/autocomplete/](/api/v1/references/agency/autocomplete/) | POST | Autocomplete (see [Using the API](/docs/using-the-api)) | Supports autocomplete on agency records |
| [/api/v1/references/cfda/](/api/v1/references/cfda/) | GET, POST | <a href="#cfda-programs">CFDA Programs</a> | Returns a list of CFDA Programs |
| /api/v1/references/cfda/:id | GET, POST | <a href="#cfda-programs">CFDA Program</a> | Returns a single CFDA program, with all fields |
| [/api/v1/references/recipients/autocomplete/](/api/v1/references/recipients/autocomplete/) | POST | Autocomplete (see [Using the API](/docs/using-the-api)) | Supports autocomplete on recipient records |
| [/api/v1/submissions/](/api/v1/submissions/) | GET, POST | <a href="#submissions">SubmissionAttributes</a> | Returns a list of submissions |
| [/api/v1/guide/](/api/v1/guide/) | GET, POST | <a href="#definitions">Definition</a> | Returns descriptions of commonly used terms |
| /api/v1/guide/:term/ | GET, POST | <a href="#definitions">Definition</a> | Returns a single description of a commonly used term |
