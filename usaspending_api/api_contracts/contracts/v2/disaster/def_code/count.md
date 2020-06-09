FORMAT: 1A
HOST: https://api.usaspending.gov

# Count of DEF Codes for the disaster/emergency spending [/api/v2/disaster/def_code/count/]

This endpoint provides the count of Object Classes which received disaster/emergency spending per the requested filters.

## POST

This endpoint returns a count of DEF Codes

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `def_codes` (required, array[string], fixed-type)
            + Members
                + `A`
                + `B`
                + `C`
                + `D`
                + `E`
                + `F`
                + `G`
                + `H`
                + `I`
                + `J`
                + `K`
                + `L`
                + `M`
                + `N`
                + `O`
                + `P`
                + `Q`
                + `R`
                + `S`
                + `T`
                + `9`
        + `fiscal_year` (required, number)
        + `spending_type` (required, enum[string], fixed-type)
            + Members
                + `total`
                + `award`
        + `award_type_codes` (optional, array[string], fixed-type)
            + Members
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

+ Response 200 (application/json)
    + Attributes (object)
        + `count` (required, number, fixed-type)
    + Body

            {
                "count": 5
            }
