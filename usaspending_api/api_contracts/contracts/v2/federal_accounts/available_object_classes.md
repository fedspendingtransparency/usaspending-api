FORMAT: 1A
HOST: https://api.usaspending.gov

# Available Object Classes [v2/federal_accounts/{federal_account_id}/available_object_classes]

## GET

This route returns object classes that the specified federal account has allotted money to.

+ Parameters
    + `federal_account_id`: 5 (required, number)
        Database id for a federal account.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[MajorObjectClass], fixed-type)

# Data Structures

## MajorObjectClass (object)
+ `id`: `10` (required, number)
+ `name`: `Major class` (required, string)
+ `minor_object_class` (required, array[MinorObjectClass], fixed-type)

## MinorObjectClass (object)
+ `id`: `123` (required, number)
+ `name`: `class` (required, string)
