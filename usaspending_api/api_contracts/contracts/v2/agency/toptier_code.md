FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Overview [/api/v2/agency/{toptier_code}/{?fiscal_year}]

Returns some basic information regarding the agency for the fiscal year specified.

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `toptier_code`: 012 (required, number)
            The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
        + `fiscal_year` (optional, number)
            The desired "as of" fiscal year. Defaults to the current fiscal year.

+ Response 200 (application/json)
    + Attributes
        + `fiscal_year` (required, number)
        + `toptier_code` (required, string)
        + `name` (required, string)
        + `abbreviation` (required, string, nullable)
        + `agency_id` (required, number)
        + `icon_filename` (required, string, nullable)
        + `mission` (required, string, nullable)
        + `website` (required, string, nullable)
        + `congressional_justification_url` (required, string, nullable)
        + `about_agency_data` (required, string, nullable)
        + `subtier_agency_count` (required, number)
        + `def_codes` (required, array[DEFC], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "fiscal_year": 2019,
                "toptier_code": "020",
                "name": "Department of the Treasury",
                "abbreviation": "TREAS",
                "agency_id": 22,
                "icon_filename": "DOT.jpg",
                "mission": "Maintain a strong economy and create economic and job opportunities...",
                "website": "https://www.treasury.gov/",
                "congressional_justification_url": "https://www.treasury.gov/cj",
                "about_agency_data": null,
                "subtier_agency_count": 10,
                "def_codes": [
                    {
                        "code": "N",
                        "public_law": "Emergency P.L. 116-136",
                        "title": "Coronavirus Aid, Relief, and Economic Security Act or the CARES Act",
                        "urls": [
                            "https://www.congress.gov/116/bills/hr748/BILLS-116hr748enr.pdf"
                        ],
                        "disaster": "covid_19"
                    }
                ]
                "messages": []
            }

# Data Structures
## DEFC (object)
+ `code` (required, string)
    Disaster Emergency Fund Code
+ `public_law` (required, string)
    Name of the public law which created the DEF or a placeholder description
+ `disaster` (required, string, nullable)
    Internal field to group several DEFC together for correlated relief efforts
+ `title` (required, string, nullable)
    Official title of the public law or a placeholder description of the DEFC
+ `urls` (required, array[string], nullable, fixed-type)
    Hyperlink(s) to official documentation on the DEFC legislation
