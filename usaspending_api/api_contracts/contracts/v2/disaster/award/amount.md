FORMAT: 1A
HOST: https://api.usaspending.gov

# Aggregated Award Spending From Disaster/Emergency Funding [/api/v2/disaster/award/amount/]

This endpoint provides account data obligation and outlay spending aggregations of all (File D) Awards which received disaster/emergency funding per the requested filters.

## POST

This endpoint provides the Account obligation and outlay aggregations of Awards

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)
    + Body


            {
                "filter": {
                    "def_codes": ["L", "M", "N", "O", "P", "U"],
                    "award_type_codes": ["02", "03", "04", "05", "07", "08", "10", "06", "09", "11", "A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E", "-1"]
                },
                "pagination": {
                    "limit": 10,
                    "page": 1,
                    "sort": "award_count",
                    "order": "desc"
                },
                "spending_type": "total"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `award_count` (required, number)
        + `face_value_of_loan` (optional, number)
        + `obligation` (required, number)
        + `outlay` (required, number)

    + Body


            {
                "award_count": 42,
                "face_value_of_loan": 984563875,
                "obligation": 32984563875,
                "outlay": 15484564321
            }


# Data Structures

## Filter (object)
+ `def_codes` (required, array[DEFC], fixed-type)
+ `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)
    When Award Type Codes are provided, results are only returned if the awards are linked between Financial Account by Awards and Awards (File C to File D linkage).
    If this filter isn't provided then the results are File C (Financial Account by Awards) only
    This is mutually exclusive from `award_type`
+ `award_type` (optional, enum[string], fixed-type)
    When provided, it will return results limiting to the award type (Assistance or Procurment) based on Financial Account data.
    This is mutually exclusive from `award_type_codes`
    + Members
        + procurement
        + assistance

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
When filtering on `award_type_codes` this will filter on File D records that have at least one File C with the provided DEFC
and belong to CARES Act DEFC.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)

## AwardTypeCodes (enum[string])
List of procurement and assistance award type codes supported by USAspending.gov

### Members
+ `02`
+ `03`
+ `04`
+ `05`
+ `06`
+ `07`
+ `08`
+ `09`
+ `10`
+ `11`
+ `A`
+ `B`
+ `C`
+ `D`
+ `IDV_A`
+ `IDV_B_A`
+ `IDV_B_B`
+ `IDV_B_C`
+ `IDV_B`
+ `IDV_C`
+ `IDV_D`
+ `IDV_E`
+ `-1`
