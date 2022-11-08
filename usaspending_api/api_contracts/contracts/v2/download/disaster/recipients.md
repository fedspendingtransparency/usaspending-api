FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Recipient Download [/api/v2/download/disaster/recipients/]

## POST

Creates a new download job for the requested filters. Returns a link to a zipped file containing the generated data files.

+ Request (application/json)
    + Attributes (object)
        + `filters` (optional, Filters, fixed-type)
        + `file_format` (optional, enum[string])
    + Body

            {
                "filters": {
                    "def_codes": ["L", "M", "N", "O", "P", "U"],
                    "award_type_codes": ["A", "B", "C", "D"]
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `status_url` (required, string)
            The endpoint used to get the status of a download.
        + `file_name` (required, string)
            Is the name of the zipfile containing CSVs that will be generated (with a timestamp suffix).
        + `file_url` (required, string)
            The URL for downloading the zip file containing the generated data files. It might not resolve to a file if the download is in-progress.
        + `download_request` (required, object)
            The JSON object used when processing the download.
    + Body

            {
                "status_url": "https://api.usaspending.gov/api/v2/download/status?file_name=COVID-19_Profile_2020-07-09_H17M39S27272793.zip",
                "file_name": "COVID-19_Profile_2020-07-09_H17M39S27272793.zip",
                "file_url": "https://files.usaspending.gov/generated_downloads/COVID-19_Profile_2020-07-09_H17M39S27272793.zip",
                "download_request": {
                    "award_category": "Contracts",
                    "columns": [
                        "recipient",
                        "award_obligations",
                        "award_outlays",
                        "number_of_awards"
                    ],
                    "download_types": [
                        "disaster_recipient"
                    ],
                    "file_format": "csv",
                    "filters": {
                    "award_type_codes": [
                            "A",
                            "B",
                            "C",
                            "D"
                        ],
                        "def_codes": [
                            "L",
                            "M",
                            "N",
                            "O",
                            "P"
                        ]
                    }
                }
            }

# Data Structures

## Filters (object)
+ `def_codes` (required, array[DEFC], fixed-type)
+ `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)
+ `query` (optional, string)
    A "keyword" or "search term" to filter down results based on this text snippet

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)

## AwardTypeCodes (enum[string])
List of procurement and assistance award type codes supported by USAspending.gov

### Members
+ `02`
+ `03`
+ `04`
+ `05`
+ `06`
+ `07`
+ `08`
+ `09`
+ `10`
+ `11`
+ `A`
+ `B`
+ `C`
+ `D`
+ `IDV_A`
+ `IDV_B_A`
+ `IDV_B_B`
+ `IDV_B_C`
+ `IDV_B`
+ `IDV_C`
+ `IDV_D`
+ `IDV_E`