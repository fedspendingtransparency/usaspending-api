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
            + `cfo_agencies` (required, array[Agency], fixed-type)
            + `other_agencies` (required, array[Agency], fixed-type)
        + `federal_accounts` (required, array[FederalAgency], fixed-type)
        + `sub_agencies` (required, array[SubAgency], fixed-type)

# Data Structures

## Agency (object)
+ `cgac_code` (required, string)
+ `name` (required, string)
+ `toptier_agency_id` (required, number)

## FederalAgency (object)
+ `federal_account_id` (required, number)
+ `federal_account_name` (required, string)

## SubAgency (object)
+ `subtier_agency_name` (required, string)
