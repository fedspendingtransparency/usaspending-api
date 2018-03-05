
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

</ul>

[//]: # (Begin Content)

# API Endpoints

This page is intended for users who are already familiar with APIs. If you're not sure what _endpoint_ means, and what `GET` and `POST` requests are, you may find the [introductory tutorial](/docs/intro-tutorial) more useful to start.

Endpoints do not currently require any authorization.


### Status Codes <a name="status-codes"></a>
In general, status codes returned are as follows:

* 200 if the request is successful
* 400 if the request is malformed
* 500 for server-side errors

## Endpoints and Methods <a name="endpoints-and-methods"></a>

The currently available endpoints are listed in the following table. Our [Data Dictionary](/docs/data-dictionary) provides more comprehensive definitions of the technical terms and government-specific language we use in the API.

To reduce unnecessary data transfer, most endpoints return a default set of information about the items being requested. To override the default field list, use the `fields`, `exclude`, and `verbose` options (see [POST Requests](#post-requests) for more information).

## Endpoint Index <a name="endpoint-index"></a>

| Endpoint | Methods | Description |
| -------- | ------- | ----------- |
| [/api/v2/awards/last_updated/](/api/v2/awards/last_updated/) | GET | Returns the last-updated date for the Award data. |
| [/api/v2/bulk_download/list_agencies/](/api/v2/bulk_download/list_agencies/) | POST | This route lists all the agencies and subagencies or federal accounts associated under specific agencies. |
| [/api/v2/bulk_download/list_monthly_files/](/api/v2/bulk_download/list_monthly_files/) | POST | This route lists the monthly files associated with the requested parameters. |
| [/api/v2/bulk_download/awards/](/api/v2/bulk_download/awards/) | POST | This route sends a request to the backend to begin generating a ZIP file of award data (in CSV format) for download. |
| [/api/v2/download/count/](/api/v2/download/count/) | POST | Returns the number of transactions that would be included in a download request for the given filter set. |
| [/api/v2/download/status/](/api/v2/download/status/) | POST | This route gets the current status of a download job that was previously requested with the v2/download/awards/ or v2/download/transaction/ endpoint. |
| [/api/v2/download/awards/](/api/v2/download/awards/) | POST | This route sends a request to the backend to begin generating a ZIP file of award data (in CSV format) for download. |
| [/api/v2/download/transactions/](/api/v2/download/transactions/) | POST | This route sends a request to the backend to begin generating a ZIP file of transaction data (in CSV format) for download. |
| [/api/v2/federal_accounts/](/api/v2/federal_accounts/) | POST | Returns a list of federal accounts. |
| /api/v2/federal_accounts/:federal_account_id/available_object_classes | GET | Returns minor object classes rolled up under major classes, filtered by federal account. |
| /api/v2/federal_accounts/:federal_account_id/fiscal_year_snapshot/:fiscal_year/ | GET | Returns  budget information for a federal account. If no fiscal year is used, the federal account's most recent fiscal year is used as the default. |
| /api/v2/federal_accounts/:federal_account_id/spending_over_time/| POST | Returns the data required to visualized the Spending Over Time graphic. |
| /api/v2/federal_obligations?fiscal_year=:fiscal_year&federal_account=:id | GET | Returns an agency's federal obligations. |
| [/api/v2/financial_balances/agencies](/api/v2/financial_balances/agencies) | GET | Returns financial balance information by funding agency and fiscal year. |
| /api/v2/financial_spending/major_object_class?fiscal_year=:fiscal_year&funding_agency_id=:id | GET | Returns award spending amounts for all recipients with respective top tier and sub tier agencies. |
| /api/v2/financial_spending/object_class?fiscal_year=:fiscal_year&funding_agency_id=:id&major_object_class_code=:id | GET | Returns award spending amounts for all recipients with respective top tier and sub tier agencies. |
| /api/v2/references/agency/:agency_id/ | GET | Returns agency information. |
| [/api/v2/references/toptier_agencies/](/api/v2/references/toptier_agencies/) | GET | Returns all toptier agencies and relevant data. |
| [/api/v2/search/spending_by_award/](/api/v2/search/spending_by_award/) | POST | Returns the fields of the filtered awards. |
| [/api/v2/search/spending_by_award_count/](/api/v2/search/spending_by_award_count/) | POST | Returns the number of awards in each award type. |
| [/api/v2/search/spending_by_geography/](/api/v2/search/spending_by_geography/) | POST | This route takes award filters and returns spending by state code, county code, or congressional district code. |
| [/api/v2/search/spending_by_transaction/](/api/v2/search/spending_by_transaction/) | POST | Returns the fields of the searched term. |
| [/api/v2/search/spending_by_transaction_count/](/api/v2/search/spending_by_transaction_count/) | POST | Returns the fields of the searched term. |
| [/api/v2/search/spending_over_time/](/api/v2/search/spending_over_time/) | POST | Returns spending by time. The amount of time is denoted by the "group" value. |
| [/api/v2/search/transaction_spending_summary/](/api/v2/search/transaction_spending_summary/) | POST | Returns the number of transactions and summation of federal action obligations |
| [/api/v2/spending/](/api/v2/spending/) | POST | Returns spending data information through various types and filters |
