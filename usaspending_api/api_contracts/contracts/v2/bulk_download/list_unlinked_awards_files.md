FORMAT: 1A
HOST: https://api.usaspending.gov

# List Unlinked Awards Files [/api/v2/bulk_download/list_unlinked_awards_files/]

## POST

Returns a list which contains links to the latest versions of unlinked awards files for an agency.

+ Request (application/json)
    + Attributes (object)
        + `toptier_code`: 012 (required, number)
            The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + Body
            {
                "toptier_code": 50
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `files` (required, array[File], fixed-type)

# Data Structures

## File (object)
+ `agency_name` (required, string)
+ `agency_acronym` (required, string, nullable)
+ `file_name` (required, string)
+ `url` (required, string)
