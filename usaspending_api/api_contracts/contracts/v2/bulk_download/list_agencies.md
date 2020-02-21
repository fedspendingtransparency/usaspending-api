FORMAT: 1A
HOST: https://api.usaspending.gov

# List Agencies [/api/v2/bulk_download/list_agencies/]

## POST

This route returns one of three result set flavors.  For "account_agencies" requests, it returns a list
of all toptier agencies with at least one DABS submission.  For "award_agencies" requests, it returns a
list of all user selectable flagged toptier agencies with at least one subtier agency.  For specific agency
requests it returns a list of all user selectable flagged subtier agencies.

+ Request (application/json)
    + Attributes (object)
        + `type` (required, enum[string])
            + Members
                + `account_agencies`
                + `award_agencies`
        + `agency` (required, optional)

+ Response 200 (application/json)
    + Attributes (object)
        + `agencies` (required, object)
            + `cfo_agencies` (required, array[Agency], fixed-type)
            + `other_agencies` (required, array[Agency], fixed-type)
        + `sub_agencies` (required, array[SubAgency], fixed-type)

# Data Structures

## Agency (object)
For "account_agencies" requests, it returns a list of all toptier agencies with at least one DABS submission.
For "award_agencies" requests, it returns a list of all user selectable flagged toptier agencies with at least
one subtier agency. 
+ `toptier_code` (required, string)
+ `name` (required, string)
+ `toptier_agency_id` (required, number)

## SubAgency (object)
Returns a list of all user selectable flagged subtier agencies for the agency specified in the request.
+ `subtier_agency_name` (required, string)
