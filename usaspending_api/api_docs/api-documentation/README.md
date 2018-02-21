
# USASpending API Documentation


The Data Accountability Transparency Act (DATA) Act created the USASpending API to advance the transparency of the Federal Government's spending data.  This API's primary use cases are to serve the USASpending.gov website, as well as US citizens as open API. This API's goal is to serve up to date spending data in a quick, accurate manner.



The USASpending API is in *V2*. V1 endpoints are currently DEPRECIATED. 



  Although download api endpoints are accessible, PREGENERATED FILES ALREADY EXIST. They are located at:  https://beta.usaspending.gov/#/bulk_download/award_data_archive or https://usaspending.gov/#/bulk_download/award_data_archive






### Examples to get started:
#### Find a federal account's fiscal spending for 2017
* Get a list of federal accounts using v2/references/federal_accounts/ [DOCUMENTATION](https://github.com/fedspendingtransparency/data-act-documentation/blob/master/usaspending/api-documentation/federal%20account/federal%20account.md) 
* Find an interesting federal account from the results.
* Use one of the federal account ids in the `fiscal year snapshot` endpoint: v2/federal_accounts/[federal_account_id]/fiscal_year_snapshot/2017/ [DOCUMENTATION](https://github.com/fedspendingtransparency/data-act-documentation/blob/master/usaspending/api-documentation/federal%20account/Fiscal%20Year%20Snapshot.md)


#### Download an award's transactions and subawards
* (this documentation is currently in progress, as the download code is fluid atm)
