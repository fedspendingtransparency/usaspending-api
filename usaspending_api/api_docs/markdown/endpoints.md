
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

This page is intended for users who are already familiar with APIs. If you're not sure what _endpoint_ means, and what `GET` and `POST` requests are, you may find the [introductory tutorial](/docs/intro-tutorial) more useful to start.

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

| Endpoint | Methods | Description |
| -------- | ------- | ----------- |
| [/api/v1/accounts/awards/](/api/v1/accounts/awards/) | GET, POST | Returns a list of financial account data by treasury account symbol, program activity, object class, and award |
| /api/v1/accounts/awards/:id | GET, POST | Returns a single treasury account symbol/program activity/object class/award record with all fields |
| [/api/v1/accounts/awards/total/](/api/v1/accounts/awards/total/) | POST | Supports aggregation on treasury account symbol/program activity/object class/award records |
| [/api/v1/awards/](/api/v1/awards/) | GET, POST | Returns a list of award records |
| /api/v1/awards/:id | GET, POST | Returns a single award record with all fields |
| [/api/v1/awards/total/](/api/v1/awards/total/) | POST | Supports aggregation on award records |
| [/api/v1/federal_accounts/](/api/v1/federal_accounts/) | GET, POST | Returns a list of federal accounts |
| /api/v1/federal_accounts/:id | GET, POST | Returns a single federal account record with all fields |
| [/api/v1/federal_accounts/autocomplete/](/api/v1/federal_accounts/autocomplete/) | POST | Supports autocomplete on federal account records |
| [/api/v2/financial_balances/agencies/](/api/v2/financial_balances/agencies/) | GET | Returns financial balance information for a specified fiscal year and funding agency |
| [/api/v1/tas/](/api/v1/tas/) | GET, POST | Returns a list of treasury appropriation accounts (TAS) |
| /api/v1/tas/:id | GET, POST | Returns a single treasury appropriation account record with all fields |
| [/api/v1/tas/autocomplete/](/api/v1/tas/autocomplete/) | POST | Supports autocomplete on TAS records |
| [/api/v1/tas/balances/](/api/v1/tas/balances/) | GET, POST | Returns a list of appropriation account balances by fiscal year |
| [/api/v1/tas/balances/total/](/api/v1/tas/balances/total/) | POST | Supports aggregation on appropriation account records |
| [/api/v1/tas/balances/quarters/](/api/v1/tas/balances/quarters/) | GET, POST | Returns a list of appropriation account balances by fiscal quarter|
| [/api/v1/tas/balances/quarters/total/](/api/v1/tas/balances/quarters/total/) | POST | Supports aggregation on quarterly appropriation account records |
| [/api/v1/tas/categories/](/api/v1/tas/categories/) | GET, POST | Returns a list of appropriation account balances by fiscal year broken up by program activities and object class |
| [/api/v1/tas/categories/total/](/api/v1/tas/categories/total/) | POST | Supports aggregation on appropriation account (by category) records |
| [/api/v1/tas/categories/quarters/](/api/v1/tas/categories/quarters/) | GET, POST | Returns a list of appropriation account balances by fiscal quarter broken up by program activities and object class |
| [/api/v1/tas/categories/quarters/total/](/api/v1/tas/categories/quarters/total/) | POST | Supports aggregation on quarterly appropriation account (by category) records |
| [/api/v1/subawards/](/api/v1/subawards/) | GET, POST | Returns a list of subaward records |
| /api/v1/subawards/:id | GET, POST | Returns a single subaward record with all fields |
| [/api/v1/subawards/autocomplete/](/api/v1/subawards/autocomplete/) | POST | Supports autocomplete on subawards |
| [/api/v1/subawards/total/](/api/v1/subawards/total/) | POST | Supports aggregation on subawards |
| [/api/v1/transactions/](/api/v1/transactions/) | GET, POST | Returns a list of transactions - contracts, grants, loans, etc. |
| /api/v1/transactions/:id | GET, POST | Returns a single transaction record with all fields |
| [/api/v1/transactions/total/](/api/v1/transactions/total/) | POST | Supports aggregation on transaction records |
| [/api/v1/references/agency/](/api/v1/references/agency/) | GET, POST | Returns a list of agency records |
| [/api/v1/references/agency/autocomplete/](/api/v1/references/agency/autocomplete/) | POST | Supports autocomplete on agency records |
| [/api/v1/references/cfda/](/api/v1/references/cfda/) | GET, POST | Returns a list of CFDA Programs |
| /api/v1/references/cfda/:id | GET, POST | Returns a single CFDA program, with all fields |
| [/api/v1/references/glossary/autocomplete/](/api/v1/references/glossary/autocomplete/) | POST | Supports autocomplete on recipient records |
| [/api/v1/references/locations/](/api/v1/references/locations/) | POST | Returns a list of locations - places of performance or vendor locations |
| [/api/v1/references/locations/geocomplete/](/api/v1/references/locations/geocomplete/) | POST | Supports geocomplete queries, see [Using the API](/docs/using-the-api) |
| [/api/v1/references/recipients/](/api/v1/references/recipients/) | GET, POST | Returns a list of recipient records |
| [/api/v1/references/recipients/:id](/api/v1/references/recipients/) | GET, POST | Returns a specific, detailed recipient record |
| [/api/v1/references/recipients/autocomplete/](/api/v1/references/recipients/autocomplete/) | POST | Supports autocomplete on recipient records |
| [/api/v1/submissions/](/api/v1/submissions/) | GET, POST | Returns a list of submissions |
| [/api/v2/awards/last_updated/](/api/v2/awards/last_updated/) | GET | Returns the last updated date for the Award data |
| [/api/v2/award_spending/award_category](/api/v2/award_spending/award_category) | GET | Returns Award Spending Amounts for all Award Category with Respective Top Tier and Sub Tier Agencies |
| [/api/v2/award_spending/recipient](/api/v2/award_spending/recipient) | GET | Returns Award Spending Amounts for all Recipients with Respective Top Tier and Sub Tier Agencies |
| /api/v2/budget_authority/agencies/:cgac | GET | Returns an agency's budget authority over the years|
| [/api/v2/bulk_download/list_agencies/](/api/v2/bulk_download/list_agencies/) | POST | This route lists all the agencies and the subagencies or federal accounts associated under specific agencies. |
| [/api/v2/bulk_download/list_monthly_files/](/api/v2/bulk_download/list_monthly_files/) | POST | This route lists the monthly files associated with the requested params. |
| [/api/v2/bulk_download/awards/](/api/v2/bulk_download/awards/) | POST | This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download. |
| [/api/v2/download/count/](/api/v2/download/count/) | POST | Returns the number of transactions that would be included in a download request for the given filter set. |
| [/api/v2/download/status/](/api/v2/download/status/) | POST | This route gets the current status of a download job that was earlier requested with the v2/download/awards/ or v2/download/transaction/ endpoint. |
| [/api/v2/download/awards/](/api/v2/download/awards/) | POST | This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download. |
| [/api/v2/download/transactions/](/api/v2/download/transactions/) | POST | This route sends a request to the backend to begin generating a zipfile of transaction data in CSV form for download. |
| [/api/v2/federal_accounts/](/api/v2/references/federal_accounts/) | POST | Returns a list of federal accounts |
| /api/v2/federal_accounts/:federal_account_id/available_object_classes | GET | Returns minor object classes rolled up under major classes filtered by federal account |
| /api/v2/federal_accounts/:federal_account_id/available_object_classes | GET | Returns minor object classes rolled up under major classes filtered by federal account |
| /api/v2/federal_accounts/:federal_account_id/fiscal_year_snapshot/:fiscal_year/ | GET | Returns  budget information for a federal account. If no fiscal year is used, the federal accounts most recent fiscal year is used |
| /api/v2/federal_accounts/:federal_account_id/spending_by_category/ | POST | Returns the data reqired to visualized the Spending By Category graphic |
| /api/v2/federal_accounts/:federal_account_id/spending_over_time/| POST | Returns the data reqired to visualized the spending over time graphic |
| /api/v2/federal_obligations?fiscal_year=:fiscal_year&federal_account=:id | GET | Returns an agency's federal obligations |
| [/api/v2/financial_balances/agencies](/api/v2/financial_balances/agencies) | GET | Returns financial balance information by funding agency and fiscal year |
| /api/v2/financial_spending/major_object_class?fiscal_year=:fiscal_year&funding_agency_id=:id | GET | Returns Award Spending Amounts for all Recipients with Respective Top Tier and Sub Tier Agencies |
| /api/v2/financial_spending/object_class?fiscal_year=:fiscal_year&funding_agency_id=:id&major_object_class_code=:id | GET | Returns Award Spending Amounts for all Recipients with Respective Top Tier and Sub Tier Agencies |
| /api/v2/references/agency/:agency_id/ | GET | Returns agency information |
| [/api/v2/references/toptier_agencies/](/api/v2/references/toptier_agencies/) | GET | Returns all toptier agencies and related, relevant data |
| [/api/v2/search/spending_by_award/](/api/v2/search/spending_by_award/) | POST | Returns the fields of the filtered awards |
| [/api/v2/search/spending_by_award_count/](/api/v2/search/spending_by_award_count/) | POST | Returns the number of awards in each award type |
| [/api/v2/search/spending_by_transaction/](/api/v2/search/spending_by_transaction/) | POST | Returns the fields of the searched term |
| [/api/v2/search/spending_by_transaction_count/](/api/v2/search/spending_by_transaction_count/) | POST | Returns the fields of the searched term |
| [/api/v2/search/spending_over_time/](/api/v2/search/spending_over_time/) | POST | returns spending by time. The amount of time is denoted by the "group" value. |
| [/api/v2/search/transaction_spending_summary/](/api/v2/search/transaction_spending_summary/) | POST | Returns returns the number of transactions and summation of federal action obligations |
| [/api/v2/spending/](/api/v2/spending/) | POST | Returns spending data information through various types and filters |
