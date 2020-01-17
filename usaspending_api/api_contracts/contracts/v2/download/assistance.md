FORMAT: 1A
HOST: https://api.usaspending.gov

# Assistance Download [/api/v2/download/assistance/]

## POST

Creates a new download job for the requested award and returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `ASST_NON_12FA00PY52375933_12D2` (required, string)
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
    + Body

            {
                "award_id":"ASST_NON_H79TI081692_7522"
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
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=ASST_H79TI081692_2020-01-13_H21M03S28186746.zip",
                "file_name": "ASST_H79TI081692_2020-01-13_H21M03S28186746.zip",
                "file_url": "/csv_downloads/ASST_H79TI081692_2020-01-13_H21M03S28186746.zip",
                "download_request": {
                    "account_level": "treasury_account",
                    "assistance_id": "H79TI081692",
                    "award_id": 69724000,
                    "columns": [],
                    "download_types": [
                        "assistance_federal_account_funding",
                        "assistance_transactions",
                        "sub_grants"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "award_id": 69724000,
                        "award_type_codes": [
                            "02",
                            "06",
                            "04",
                            "05",
                            "08",
                            "07",
                            "10",
                            "03",
                            "11",
                            "09"
                        ]
                    },
                    "include_data_dictionary": true,
                    "include_file_description": {
                        "destination": "AssistanceAwardSummary_download_readme.txt",
                        "source": "/usaspending-api/usaspending_api/data/AssistanceSummary_download_readme.txt"
                    },
                    "is_for_assistance": true,
                    "is_for_contract": false,
                    "is_for_idv": false,
                    "limit": 500000,
                    "request_type": "assistance"
                }
            }