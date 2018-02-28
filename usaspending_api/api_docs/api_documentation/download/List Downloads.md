## [List Downloads](#list-downloads)
**Route:** `/v2/bulk_download/list_monthly_files/`

**Method:** `POST`

This route lists the monthly files associated with the requested params.

### Request example

```
{
    "agency": 425,
    "fiscal_year": 2015,
    "type": "contracts"
}
```

### Request Parameters Description

* `agency` - specific agency database id. Retrived from the `list_agencies` endpoint.
* `fiscal_year` - specific fiscal year to pull
* `type` - specific type of awards/transactions ('contracts' or 'assistance')

### Response (JSON)

```
{
    "monthly_files": [
        {
            "agency_name": "Department of Energy",
            "agency_acronym": "DOE",
            "file_name": "2017_010_Contracts_Full_20171212.zip",
            "updated_date": "2017-12-12",
            "type": "contracts",
            "fiscal_year": "2018",
            "url": "https://s3-us-gov-west-1.amazonaws.com:443/usaspending-monthly-downloads/2017_010_Contracts_Full_20171212.zip"
        },
        ...
    ]
}
```

* `monthly_files` - list of all files
    * `agency_name` - name of the agency, null if all
    * `agency_acronym` - acronym of agency
    * `fiscal_year` - fiscal year associated with the file
    * `type` - the download type
    * `file_name` - name of the file
    * `url` - url where the file can be downloaded
    * `updated_date` - date when the file was updated
