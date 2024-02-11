FORMAT: 1A
HOST: https://api.usaspending.gov

# List Unlinked Awards Files [/api/v2/bulk_download/list_unlinked_awards_files/]

## POST

Returns a list which contains links to the latest versions of unlinked awards files for an agency.

+ Request (application/json)
    + Attributes (object)
        + `toptier_code`: (required, string)
            The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + Body

            {
                "toptier_code": "086"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `files` (required, array[File], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.


    + Body

            {
                "files": [
                    {
                        "agency_name": "Department of Veterans Affairs",
                        "toptier_code": "036",
                        "agency_acronym": "VA",
                        "file_name": "Department_of_Veterans_Affairs_unlinked_awards_2024-02-09_H21M34S39790417.zip",
                        "url": "https://files.usaspending.gov/unlinked_awards_downloads/Department_of_Veterans_Affairs_unlinked_awards_2024-02-09_H21M34S39790417.zip"
                    }
                ],
                "messages": []
            }
# Data Structures

## File (object)
+ `agency_name` (required, string)
+ `toptier_code` (required, string)
+ `agency_acronym` (required, string, nullable)
+ `file_name` (required, string)
+ `url` (required, string)
