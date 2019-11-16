
<ul class="nav nav-stacked" id="sidebar">
  <li><a href="/docs/intro-tutorial">Introductory Tutorial</a></li>
  <li><a href="/docs/endpoints">Endpoints</a></li>
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

The currently available endpoints are listed in the following table.

## Endpoint Index <a name="endpoint-index"></a>

| Endpoint | Methods | Description |
| -------- | ------- | ----------- |
|[/api/v2/autocomplete/accounts/a/](/api/v2/autocomplete/accounts/a/)|POST| Returns Treasury Account Symbol Availability Type Code (A) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/accounts/aid/](/api/v2/autocomplete/accounts/aid/)|POST| Returns Treasury Account Symbol/Federal Account Agency Identifier (AID) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/accounts/ata/](/api/v2/autocomplete/accounts/ata/)|POST| Returns Treasury Account Symbol Allocation Transfer Agency Identifier (ATA) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/accounts/bpoa/](/api/v2/autocomplete/accounts/bpoa/)|POST| Returns Treasury Account Symbol Beginning Period of Availability (BPOA) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/accounts/epoa/](/api/v2/autocomplete/accounts/epoa/)|POST| Returns Treasury Account Symbol Ending Period of Availability (EPOA) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/accounts/main/](/api/v2/autocomplete/accounts/main/)|POST| Returns Treasury Account Symbol/Federal Account Main Account Code (MAIN) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/accounts/sub/](/api/v2/autocomplete/accounts/sub/)|POST| Returns Treasury Account Symbol Sub-Account Code (SUB) filtered by other components provided in the request filter | 
|[/api/v2/autocomplete/awarding_agency/](/api/v2/autocomplete/awarding_agency/)|POST| Returns awarding agencies matching the specified search text|
|[/api/v2/autocomplete/cfda/](/api/v2/autocomplete/cfda/)|POST| Returns CFDA programs matching the specified search text|
|[/api/v2/autocomplete/city/](/api/v2/autocomplete/city/)|POST| Returns city names matching the search text, sorted by relevance|
|[/api/v2/autocomplete/funding_agency/](/api/v2/autocomplete/funding_agency/)|POST| Returns funding agencies matching the specified search text|
|[/api/v2/autocomplete/glossary/](/api/v2/autocomplete/glossary/)|POST| Returns glossary terms matching provided search text|
|[/api/v2/autocomplete/naics/](/api/v2/autocomplete/naics/)|POST| Returns NAICS objects matching the specified search text|
|[/api/v2/autocomplete/psc/](/api/v2/autocomplete/psc/)|POST| Returns product or service (PSC) codes and their descriptions based on a search string. This may be the 4-character PSC code or a description string.|
|[/api/v2/award_spending/recipient/](/api/v2/award_spending/recipient/?fiscal_year=2016&awarding_agency_id=183)|GET| Returns all award spending by recipient for a given fiscal year and agency id|
|[/api/v2/awards/<AWARD_ID\>/](/api/v2/awards/CONT_IDV_TMHQ10C0040_2044/)|GET| Returns details about specific award|
|[/api/v2/awards/accounts/](/api/v2/awards/accounts/)|POST| Returns a list of federal accounts for the indicated award|
|[/api/v2/awards/funding_rollup](/api/v2/awards/funding_rollup)|POST| Returns aggregated count of awarding agencies, federal accounts, and total transaction obligated amount for an award|
|[/api/v2/awards/funding](/api/v2/awards/funding)|POST| Returns federal account, awarding agencies, funding agencies, and transaction obligated amount information for a requested award|
|[/api/v2/awards/last_updated/](/api/v2/awards/last_updated/)|GET| Returns date of last update|
|[/api/v2/awards/count/transaction/<AWARD_ID\>/](/api/v2/awards/count/transaction/CONT_IDV_TMHQ10C0040_2044/)|GET| Returns the number of transactions associated with the award|
|[/api/v2/awards/count/subaward/<AWARD_ID\>/](/api/v2/awards/count/subaward/66945037/)|GET| Returns the number of subawards associated with the award|
|[/api/v2/awards/count/subaward/<AWARD_ID\>/](/api/v2/awards/count/subaward/CONT_IDV_TMHQ10C0040_2044/)|GET| Returns the number of subawards associated with the award|
|[/api/v2/awards/count/federal_account/<AWARD_ID\>/](/api/v2/awards/count/federal_account/CONT_IDV_TMHQ10C0040_2044/)|GET| Returns the number of federal accounts associated with the award|
|[/api/v2/budget_functions/list_budget_functions/](/api/v2/budget_functions/list_budget_functions/)|GET| Returns all Budget Functions associated with a TAS, ordered by Budget Function code|
|[/api/v2/budget_functions/list_budget_subfunctions/](/api/v2/budget_functions/list_budget_subfunctions/)|POST| Returns all Budget Functions associated with a TAS, ordered by Budget Function code|
|[/api/v2/bulk_download/awards/](/api/v2/bulk_download/awards/)|POST| Generates zip file for download of award data in CSV format|
|[/api/v2/bulk_download/list_agencies/](/api/v2/bulk_download/list_agencies/)|POST| Lists all the agencies and the subagencies or federal accounts associated under specific agencies.|
|[/api/v2/bulk_download/list_monthly_files/](/api/v2/bulk_download/list_monthly_files/)|POST| Lists the monthly files associated with the requested params|
|[/api/v2/bulk_download/status/](/api/v2/bulk_download/status/)|GET| Returns the current status of a download job that that has been requested with the `v2/bulk_download/awards/` or `v2/bulk_download/transaction/` endpoint that same day.|
|[/api/v2/download/accounts/](/api/v2/download/accounts/)|POST| Generates zip file for download of account data in CSV format|
|[/api/v2/download/awards/](/api/v2/download/awards/)|POST| Generates zip file for download of award data in CSV format|
|[/api/v2/download/count/](/api/v2/download/count/)|POST| Returns the number of transactions that would be included in a download request for the given filter set|
|[/api/v2/download/idv/](/api/v2/download/idv/)|POST| Returns a zipped file containing IDV data|
|[/api/v2/download/contract/](/api/v2/download/contract/)|POST| Returns a zipped file containing Contract data|
|[/api/v2/download/assistance/](/api/v2/download/assistance/)|POST| Returns a zipped file containing Assistance data|
|[/api/v2/download/status/](/api/v2/download/status/)|GET| gets the current status of a download job that that has been requested with the `v2/download/awards/` or `v2/download/transaction/` endpoint that same day|
|[/api/v2/download/transactions/](/api/v2/download/transactions/)|POST|Generates zip file for download of award data in CSV format|
|[/api/v2/federal_accounts/<ACCOUNT_CODE\>/available_object_classes/](/api/v2/federal_accounts/4324/available_object_classes/)|GET| Returns financial spending data by object class based on account's internal ID|
|[/api/v2/federal_accounts/<ACCOUNT_CODE\>/fiscal_year_snapshot/<YEAR\>/](/api/v2/federal_accounts/4324/fiscal_year_snapshot/2017/)|GET| Returns budget information for a federal account for the year provided based on account's internal ID|
|[/api/v2/federal_accounts/<ACCOUNT_CODE\>/fiscal_year_snapshot/](/api/v2/federal_accounts/4324/fiscal_year_snapshot/)|GET| Returns budget information for a federal account for the most recent year based on account's internal ID|
|[/api/v2/federal_accounts/<ACCOUNT_CODE\>/](/api/v2/federal_accounts/020-0550/)|GET| Returns a federal account based on its federal account code|
|[/api/v2/federal_accounts/](/api/v2/federal_accounts/)|POST| Returns financial spending data by object class|
|[/api/v2/federal_obligations/](/api/v2/federal_obligations/?fiscal_year=2019&funding_agency_id=315&limit=10&page=1)|GET| Returns a paginated list of obligations for the provided agency for the provided year|
|[/api/v2/financial_balances/agencies/](/api/v2/financial_balances/agencies/?fiscal_year=2017&funding_agency_id=4324)|GET| Returns financial balances by agency and the latest quarter for the given fiscal year|
|[/api/v2/financial_spending/major_object_class/](/api/v2/financial_spending/major_object_class/?fiscal_year=2017&funding_agency_id=4324)|GET| Returns financial spending data by object class for the latest quarter based on the given fiscal year |
|[/api/v2/financial_spending/object_class/](/api/v2/financial_spending/object_class/?fiscal_year=2017&funding_agency_id=4324&major_object_class_code=20)|GET| Returns financial spending data by object class for the latest quarter based on the given fiscal year|
|[/api/v2/idvs/accounts/](/api/v2/idvs/accounts/)|POST| Returns a list of federal accounts for the indicated IDV|
|[/api/v2/idvs/activity/](/api/v2/idvs/activity/)|POST| Returns information about child awards and grandchild awards for a given IDV (Indefinite Delivery Vehicle).|
|[/api/v2/idvs/amounts/<AWARD_ID\>/](/api/v2/idvs/amounts/CONT_IDV_NNK14MA74C_8000/)|GET| Returns the direct children of an IDV|
|[/api/v2/idvs/awards/](/api/v2/idvs/awards/)|POST| Returns IDVs or contracts related to the requested Indefinite Delivery Vehicle award (IDV)|
|[/api/v2/idvs/funding/](/api/v2/idvs/funding/)|POST| Returns File C funding records associated with an IDV|
|[/api/v2/idvs/funding_rollup/](/api/v2/idvs/funding_rollup/)|POST| Returns aggregated count of awarding agencies, federal accounts, and total transaction obligated amount for all contracts under an IDV|
|[/api/v2/idvs/count/federal_account/<AWARD_ID\>/](/api/v2/idvs/count/federal_account/CONT_IDV_NNK14MA74C_8000/)|GET| Returns the number of federal accounts associated with children and grandchild awards of an IDV.|
|[/api/v2/recipient/children/<DUNS\>/](/api/v2/recipient/children/006928857/)|GET| Returns recipient details based on DUNS number|
|[/api/v2/recipient/duns/<HASH_VALUE\>/](/api/v2/recipient/duns/42c19cbe-ced7-5d41-2f80-cd27a22b1575-P/)|GET| Returns a high-level overview of a specific recipient, given its id|
|[/api/v2/recipient/duns/](/api/v2/recipient/duns/)|POST| Returns a list of recipients in USAspending DB|
|[/api/v2/recipient/state/<FIPS\>/](/api/v2/recipient/state/51/)|GET| Returns basic information about the specified state|
|[/api/v2/recipient/state/awards/<FIPS\>/](/api/v2/recipient/state/awards/51/)|GET| Returns award breakdown based on FIPS|
|[/api/v2/recipient/state/](/api/v2/recipient/state/)|GET| Returns basic information about the specified state|
|[/api/v2/references/agency/<AGENCY_ID\>/](/api/v2/references/agency/479/)|GET| Returns basic information about a federal agency|
|[/api/v2/references/data_dictionary/](/api/v2/references/data_dictionary/)|GET| Returns a JSON structure of the Schema team's Rosetta Crosswalk Data Dictionary|
|[/api/v2/references/glossary/](/api/v2/references/glossary/)|GET| Returns a list of glossary terms and definitions|
|[/api/v2/references/toptier_agencies/](/api/v2/references/toptier_agencies/)|GET|  Returns all toptier agencies and related, relevant data.|
|[/api/v2/references/naics/](/api/v2/references/naics/)|GET|  Returns all Tier 1 (2-digit) NAICS and related, relevant data.|
|[/api/v2/references/naics/](/api/v2/references/naics/?filter=forest)|GET| Filter returns NAICS at any level and their parents/grandparents.|
|[/api/v2/references/naics/<NAICS_CODE>/](/api/v2/references/naics/11/)|GET|  Returns the requested NAICS and immediate children, as well as related, relevant data.|
|[/api/v2/references/naics/<NAICS_CODE>/](/api/v2/references/naics/11/?filter=fruit)|GET|  Filter returns NAICS that match the filter and required id at any level and their parents/grandparents.|
|[/api/v2/search/new_awards_over_time/](/api/v2/search/new_awards_over_time/)|POST| Returns a list of time periods with the new awards in the appropriate period within the provided time range|
|[/api/v2/search/spending_by_award/](/api/v2/search/spending_by_award/)|POST| Returns the fields of the filtered awards|
|[/api/v2/search/spending_by_award_count/](/api/v2/search/spending_by_award_count/)|POST| Returns the number of awards in each award type (Contracts, IDV, Loans, Direct Payments, Grants, and Other)|
|[/api/v2/search/spending_by_category/](/api/v2/search/spending_by_category/)|POST| Returns data that is grouped in preset units to support the various data visualizations on USAspending.gov's Advanced Search page|
|[/api/v2/search/spending_by_geography/](/api/v2/search/spending_by_geography/)|POST| Returns spending by state code, county code, or congressional district code|
|[/api/v2/search/spending_by_transaction/](/api/v2/search/spending_by_transaction/)|POST| Returns awards where a certain subset of fields match against search term|
|[/api/v2/search/spending_by_transaction_count/](/api/v2/search/spending_by_transaction_count/)|POST| Returns awards where a certain subset of fields match against search term|
|[/api/v2/search/spending_over_time/](/api/v2/search/spending_over_time/)|POST| Returns spending by time|
|[/api/v2/search/transaction_spending_summary/](/api/v2/search/transaction_spending_summary/)|POST| Returns the number of transactions and the sum of federal action obligations for prime awards given a set of award of filters|
|[/api/v2/spending/](/api/v2/spending/)|POST| Returns spending data information through various types and filters|
|[/api/v2/subawards/](/api/v2/subawards/)|POST| Returns subawards either related, optionally, to a specific parent award, or for all parent awards if desired|
|[/api/v2/transactions/](/api/v2/transactions/)|POST| Returns transactions related to a specific parent award|
