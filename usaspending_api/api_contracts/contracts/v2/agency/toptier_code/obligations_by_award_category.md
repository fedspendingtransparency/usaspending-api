FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Obligations [/api/v2/agency/{toptier_code}/obligations_by_award_category/{?fiscal_year}]

This endpoint is used to power USAspending.gov's agency profile pages.

## GET

This endpoint returns a breakdown of obligations by award category (contracts, IDVs, grants, loans, direct payments, other) within the requested fiscal year (or current FY).

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year`: 2017 (optional, number)
        The fiscal year for which you are querying data. Defaults to the current fiscal year if not provided.
        
+ Response 200 (application/json)
    + Attributes (object)
        + `total_aggregated_amount` (required, number)
            The agency's total award obligations for the provided fiscal year
        + `results` (required, array[ObligationSubtotals], fixed-type)
            Sorted by aggregated_amount, descending. Categories with $0 of obligations within the FY are not included.

    + Body

            {
                "total_aggregated_amount": 1219.55,
                "results": [
                    {
                        "category": "contracts",
                        "aggregated_amount": 1000.0
                    },
                    {
                        "category": "direct_payments",
                        "aggregated_amount": 60.55
                    },
                    {
                        "category": "idvs",
                        "aggregated_amount": 55.0
                    },
                    {
                        "category": "grants",
                        "aggregated_amount": 100.0
                    },
                    {
                        "category": "loans",
                        "aggregated_amount": 0.0
                    },
                    {
                        "category": "other",
                        "aggregated_amount": 4.0
                    }
                ]
            }

# Data Structures

## ObligationSubtotals (object)
+ `category` (required, enum[string])
    + Members
        + `contracts`
        + `direct_payments`
        + `idvs`
        + `grants`
        + `loans`
        + `other`
+ `aggregated_amount` (required, number)
