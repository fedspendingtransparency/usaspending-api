FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Profile

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

### Get Major Object Classes [GET]

+ Response 200 (application/json)
    + Attributes
        + results (required, array[MajorObjectClass], fixed-type)

## Major Object Classes [/api/v2/financial_spending/object_class/{?fiscal_year,funding_agency_id,major_object_class_code}]

This endpoint returns the total amount that a specific agency has obligated to minor object classes within a specific major object class in a given fiscal year.

+ Parameters
    + fiscal_year: 2017 (required, number)
        The fiscal year that you are querying data for.
    + funding_agency_id: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.
    + major_object_class_code: 30 (required, number)
        The major object class code returned in `/api/v2/financial_spending/major_object_class/`.

# Data Structures

## MinorObjectClass (object)
+ object_class_code: 310 (required, string)
+ object_class_name: Equipment (required, string)
+ obligated_amount: 209728763.65 (required, string)
