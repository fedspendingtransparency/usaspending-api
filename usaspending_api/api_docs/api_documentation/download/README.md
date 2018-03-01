# Download

The following endpoints are involved with generating files that reflect the site's underlying data. 

## Award Data Archive

On a monthly basis, the website pre-generates a series of commonly used files based on the agency, fiscal year, and award type. These can be found at [Award Data Archive](https://beta.usaspending.gov/#/download_center/award_data_archive), or, if you prefer, you can also use the API's [List Downloads Endpoint](list_downloads.md).

## Generating Download Files

**Reminder**: Before using these endpoints, check the  [Award Data Archive](https://usaspending.gov/#/download_center/award_data_archive) for pre-generated files

There are several downloadable endpoints, all with different features/constraints. 

### Row Constraint Downloads

These downloads have a row constraint on them, meaning they have a hard limit to the number of records to include (currently that limit is `500,000` rows). The main benefit of using these endpoints, however, is that they allow various filters that are not supported by the Year Constraint Downloads.

For downloading transactions, please use [Advanced_Search_Transaction_Download](advanced_search_transaction_download.md).

For downloading awards, please use [Advanced_Search_Award_Download](advanced_search_award_download.md).

### Year Constraint Downloads

These downloads do not have a row constraint but they are limited to only a certain number of filters and the time range filter is limited to `1 year` at most. This allows you to download millions of rows for larger requests.

For downloading transactions and subawards, please use [Custom_Award_Data_Download](custom_award_data_download.md).

## Checking the status of the Download Generation

The responses of these endpoints includes a `file_name`, which will be used to check on the status of the requested download. For example, the response will look something like:
```
{
   ...
   "file_name":"5757660_968336105_awards.zip",
   ...
}
```

To check to see whether that request is complete, use the [Status Endpoint](download_status.md) using `5757660_968336105_awards.zip`. Once complete, you can follow the `url` provided in the status response to finally download your data.
