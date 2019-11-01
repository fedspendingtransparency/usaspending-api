FORMAT: 1A
HOST: https://api.usaspending.gov

# List Monthly Files [/api/v2/bulk_download/list_monthly_files/]

## POST

Returns a list of the current versions of generated archive files for a given fiscal year and agency.
        
+ Request (application/json)
    + Attributes (object)
        + `agency` (required, number) Agency database ID or `all`.
        + `fiscal_year` (required, number) 
        + `type` (optional, enum[string])
            + Members
                + `assistance`
                + `contracts`
    + Body

            {
                "agency": 50,
                "fiscal_year": 2018,
                "type": "assistance"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `monthly_files` (required, array[MonthlyFile], fixed-type)

# Data Structures

## MonthlyFile (object)
+ `agency_name` (required, string)
+ `agency_acronym` (required, string, nullable)
+ `file_name` (required, string)
+ `fiscal_year` (required, number)
+ `type` (required, enum[string])
    + Members
        + `assistance`
        + `contracts`
+ `updated_date` (required, string)
+ `url` (required, string)
