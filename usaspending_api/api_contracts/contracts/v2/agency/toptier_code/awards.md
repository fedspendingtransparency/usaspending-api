FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Awards [/api/v2/agency/{toptier_code}/awards/{?fiscal_year,agency_type,award_type_codes}]

Returns the number of transactions and award obligations for a given agency and fiscal year.

## GET

+ Parameters
    + `toptier_code`: `020` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `agency_type` (optional, enum[string])
        This will determine if the data being returned is derived from the awarding agency or the funding agency. Defaults to awarding
        + Default: `awarding`
        + Members
            + `awarding`
            + `funding`
    + `award_type_codes` (optional, AwardTypes)
        Filters the results by the provided award types. Defaults to all award types

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `latest_action_date` (required, string, nullable)
        + `transaction_count` (required, number)
        + `obligations` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "020",
                "fiscal_year": 2021,
                "latest_action_date": "2021-09-14T00:00:00",
                "transaction_count": 2,
                "obligations": 90000.0,
                "messages": []
            }

# Data Structures

## AwardTypes (array)
List of filterable award types

### Sample
- `A`
- `B`
- `C`
- `D`

### Default
- `02`
- `03`
- `04`
- `05`
- `06`
- `07`
- `08`
- `09`
- `10`
- `11`
- `A`
- `B`
- `C`
- `D`
- `IDV_A`
- `IDV_B`
- `IDV_B_A`
- `IDV_B_B`
- `IDV_B_C`
- `IDV_C`
- `IDV_D`
- `IDV_E`
