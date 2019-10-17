FORMAT: 1A
HOST: https://api.usaspending.gov

# List Agencies [/api/v2/bulk_download/list_agencies/]

## POST

This route lists all the agencies and the subagencies or federal accounts associated under specific agencies.
        
+ Request (application/json)
    + Attributes (object)
        + `agency` (required, number)

+ Response 200 (application/json)
    + Attributes (object)
        + `agencies` (required, object)
            + `cfo_agencies` (required, array[Agency])
            + `other_agencies` (required, array[Agency])
        + `federal_accounts` (required, array)
            + (object)
                + `federal_account_id` (required, number)
                + `federal_account_name` (required, string)
        + `sub_agencies` (required, array)
            + (object)
                + `subtier_agency_name` (required, string)
          
# Data Structures

## Agency (object)
+ `cgac_code` (required, string)
+ `name` (required, string)
+ `toptier_agency_id` (required, number)
