FORMAT: 1A
HOST: https://api.usaspending.gov

# DEF Code Types [/api/v2/references/def_codes/]

This endpoint returns a JSON object describing all Disaster and Emergency Funding (DEF) Codes

## GET
+ Response 200 (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Attributes (object)
        + `codes` (array[DEFC], fixed-type, required)
    + Body

            {
                "codes": [
                    {
                        "title" : "Supplemental Appropriations for Disaster Relief Requirements Act, 2017",
                        "disaster" : null,
                        "code" : "A"
                    },
                    {
                        "title" : "Additional Supplemental Appropriations for Disaster Relief Requirements Act, 2017",
                        "disaster" : null,
                        "code" : "B"
                    },
                    {
                        "title" : "Bipartisan Budget Act of 2018",
                        "disaster" : null,
                        "code" : "C"
                    },
                    {
                        "title" : "FAA Reauthorization Act of 2018",
                        "disaster" : null,
                        "code" : "D"
                    },
                    {
                        "title" : "Additional Supplemental Appropriations for Disaster Relief Act, 2019.",
                        "disaster" : null,
                        "code" : "E"
                    },
                    {
                        "title" : "EMERGENCY SUPPLEMENTAL APPROPRIATIONS FOR HUMANITARIAN ASSISTANCE AND SECURITY AT THE SOUTHERN BORDER ACT, 2019",
                        "disaster" : null,
                        "code" : "F"
                    },
                    {
                        "title" : "Consolidated Appropriations Act, 2020",
                        "disaster" : null,
                        "code" : "G"
                    },
                    {
                        "title" : "Consolidated Appropriations Act, 2020",
                        "disaster" : null,
                        "code" : "H"
                    },
                    {
                        "title" : "Further Consolidated Appropriations Act, 2020",
                        "disaster" : null,
                        "code" : "I"
                    },
                    {
                        "title" : "Further Consolidated Appropriations Act, 2020",
                        "disaster" : null,
                        "code" : "J"
                    },
                    {
                        "title" : "United States-Mexico-Canada Agreement Implementation Act",
                        "disaster" : null,
                        "code" : "K"
                    },
                    {
                        "title" : "Coronavirus Preparedness and Response Supplemental Appropriations Act, 2020",
                        "disaster" : "covid_19",
                        "code" : "L"
                    },
                    {
                        "title" : "Families First Coronavirus Response Act",
                        "disaster" : "covid_19",
                        "code" : "M"
                    },
                    {
                        "title" : "Coronavirus Aid, Relief, and Economic Security Act or the CARES Act",
                        "disaster" : "covid_19",
                        "code" : "N"
                    },
                    {
                        "title" : "Coronavirus Aid, Relief, and Economic Security Act or the CARES Act",
                        "disaster" : "covid_19",
                        "code" : "O"
                    },
                    {
                        "title" : "Paycheck Protection Program and Health Care Enhancement Act)",
                        "disaster" : "covid_19",
                        "code" : "P"
                    },
                    {
                        "title" : null,
                        "disaster" : null,
                        "code" : "Q"
                    },
                    {
                        "title" : "Future Disaster and P.L. To Be Determined",
                        "disaster" : null,
                        "code" : "R"
                    },
                    {
                        "title" : "Future Disaster and P.L. To Be Determined",
                        "disaster" : null,
                        "code" : "S"
                    },
                    {
                        "title" : "Future Disaster and P.L. To Be Determined",
                        "disaster" : null,
                        "code" : "T"
                    },
                    {
                        "title" : "DEFC of '9' Indicates that the data for this row is not related to a COVID-19 P.L. (not DEFC = L, M, N, O, or P), but that the agency has declined to specify which other DEFC (or combination of DEFCs, in the case that the money hasn't been split out like it would be with a specific DEFC value) applies.",
                        "disaster" : null,
                        "code" : "9"
                    }
                ]
            }

## Data Structures
### DEFC (object)
+ `code` (required, string)
+ `title` (required, string)
+ `disaster` (required, string, nullable)
