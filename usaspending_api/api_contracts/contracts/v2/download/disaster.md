FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Funding and Spending Download [/api/v2/download/disaster/]

## POST

Creates a new download job for the requested account and award. Returns a link to a zipped file containing the generated data files.

+ Request (application/json)
    + Attributes (object)
        + `filters` (optional, Filters, fixed-type)
            Currently ignored, download will only be generated for COVID-19 spending data
        + `file_format` (optional, enum[string])
            Currently ignored, download will only be generated with CSV data files
    + Body

            {
                "filters": {
                    "def_codes": ["L", "M", "N", "O", "P", "R"]
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
                    "filters": {
                        "def_codes": [
                            "L",
                            "M",
                            "N",
                            "O",
                            "P"
                        ],
                        "latest_fiscal_period": "8",
                        "latest_fiscal_year": "2020",
                        "start_date": "2020-04-01"
                    }
                }
            }

# Data Structures

## Filters (object)
+ `def_codes` (required, array[DEFC], fixed-type)

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing

### Members
+ `A`
+ `B`
+ `C`
+ `D`
+ `E`
+ `F`
+ `G`
+ `H`
+ `I`
+ `J`
+ `K`
+ `L`
+ `M`
+ `N`
+ `O`
+ `P`
+ `Q`
+ `R`
+ `S`
+ `T`
+ `9`