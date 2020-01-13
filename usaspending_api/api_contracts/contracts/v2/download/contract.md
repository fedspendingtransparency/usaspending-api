FORMAT: 1A
HOST: https://api.usaspending.gov

# Contract Download [/api/v2/download/contract/]

## POST

Creates a new download job for the requested award and returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AWD_UZ02_9700_SPM2DV11D9200_9700` (required, string)
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
    + Body

            {
                "award_id":"CONT_AWD_N0002404C2105_9700_-NONE-_-NONE-"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `status_url` (required, string)
            The endpoint used to get the status of a download.
        + `file_name` (required, string)
            Is the name of the zipfile containing CSVs that will be generated (file_name is timestamp followed by `_transactions` or `_awards`).
        + `file_url` (required, string)
            The URL for the file.
        + `download_request` (required, object)
            The JSON object used when processing the download.
    + Body

            {
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=CONT_N0002404C2105_2020-01-13_H21M07S31205632.zip",
                "file_name": "CONT_N0002404C2105_2020-01-13_H21M07S31205632.zip",
                "file_url": "/csv_downloads/CONT_N0002404C2105_2020-01-13_H21M07S31205632.zip",
                "download_request": {
                    "account_level": "treasury_account",
                    "award_id": 25885000,
                    "columns": [],
                    "download_types": [
                        "contract_federal_account_funding",
                        "contract_transactions",
                        "sub_contracts"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "award_id": 25885000,
                        "award_type_codes": [
                            "B",
                            "C",
                            "A",
                            "D"
                        ]
                    },
                    "include_data_dictionary": true,
                    "include_file_description": {
                        "destination": "ContractAwardSummary_download_readme.txt",
                        "source": "/usaspending-api/usaspending_api/data/ContractSummary_download_readme.txt"
                    },
                    "is_for_assistance": false,
                    "is_for_contract": true,
                    "is_for_idv": false,
                    "limit": 500000,
                    "piid": "N0002404C2105",
                    "request_type": "contract"
                }
            }