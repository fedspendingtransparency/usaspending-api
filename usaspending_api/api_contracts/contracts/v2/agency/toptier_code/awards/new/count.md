FORMAT: 1A
HOST: https://api.usaspending.gov

# Overview of new awards for Agency [/api/v2/agency/{toptier_code}/awards/new/count{?fiscal_year,agency_type,award_type_codes}]

Return the count of new Awards under the Agency

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `toptier_code`: 086 (required, number)
            The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY
        + `agency_type` (optional, enum[string])
            The agency type to pull the count for.
            + Default: `awarding`
            + Members
                + `awarding`
                + `funding`
        + `award_type_codes` (optional, AwardTypeCodes)
            Comma-separated award type codes to pull the count for (e.g. `A,B,C,D` for all Contracts).

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `agency_type` (required, string)
        + `award_type_codes` (required, array[AwardTypeCodes], fixed-type)
        + `new_award_count` (required, number)

    + Body

            {
                "toptier_code": "086",
                "fiscal_year": 2018,
                "agency_type": "awarding",
                "award_type_codes": ["A", "B", "C", "D"]
                "award_count": 110204
            }

# Data Structures

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
