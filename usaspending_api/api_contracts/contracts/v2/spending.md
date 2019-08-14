FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending Explorer [/api/v2/spending/]

These endpoints are used to power USAspending.gov's Spending Explorer. This data can be used to drill down into specific subsets of data by level of detail.

The Spending Explorer requires the top-level (or entry point) filter to be one of three types:

* Budget Function (`budget_function`)
* Agency (`agency`)
* Object Class (`object_class`)

## POST

**General Spending Explorer Data**

The general Spending Explorer response will contain only one filter (required), a fiscal year and quarter to limit the data to. The data will include _all_ quarters up to and including the specified quarter in the given fiscal year.

This data represents all government spending in the specified time period, grouped by the data type of your choice.

Note that data is not available prior to FY 2017 Q2.

Note that data for the latest complete quarter is not available until 45 days after the quarter's close.

**Specific Spending Explorer**

Using the response from the general Spending Explorer, you can drill down to more detailed grouping fields. However, you must limit the scope of your request to one of the top-level groups and, optionally, additional lower-level groups. Each of your groups will combine to become the scope of your request. For example, if you filter by "Department of Justice" and "Salaries and Expenses," you will only see spending breakdowns for "Salaries and Expenses" within "Department of Justice."

+ Request Specific Spending Explorer (application/json)
    + Attributes (object)
        + `type`: `program_activity` (required, enum[string])
            + Members
                + `federal_account`
                + `object_class`
                + `recipient`
                + `award`
                + `budget_function`
                + `budget_subfunction`
                + `agency`
                + `program_activity`
        + `filters` (required, DetailedFilter, fixed-type)

+ Response 200 (application/json)
    + Attributes (object)
        + `total`: 1410774412.52 (required, number, nullable)
            Total should only be null when there are no results.
        + `end_date`: `2017-09-30` (required, string)
            This is the "as-of" date for the data being returned.
        + `results` (required, array[SpendingExplorerDetailedResponse], fixed-type)

+ Request General Spending Explorer (application/json)
    + Attributes (object)
        + `type`: `agency` (required, enum[string])
            + Members
                + `budget_function`
                + `agency`
                + `object_class`
        + `filters` (required, GeneralFilter, fixed-type)

+ Response 200 (application/json)
    + Attributes (object)
        + `total`: 126073789264.49 (required, number, nullable)
            Total should only be null when there are no results.
        + `end_date`: `2017-09-30` (required, string)
            This is the "as-of" date for the data being returned.
        + `results` (required, array[SpendingExplorerGeneralResponse, SpendingExplorerGeneralUnreportedResponse], fixed-type)
 


# Data Structures

## GeneralFilter (object)
+ `fy`: `2017` (required, string)
+ `quarter`: `4` (required, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`

## DetailedFilter (object)
+ `fy`: `2017` (required, string)
+ `quarter`: `4` (required, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`
+ `agency`: 252 (optional, number)
    This value is the `id` returned in the general Spending Explorer response.
+ `federal_account`: 830 (optional, number)
    This value is the `id` returned in the previous specific Spending Explorer response.
+ `object_class`: 123 (optional, number)
+ `budget_function`: 123 (optional, number)
+ `budget_subfunction`: 123 (optional, number)
+ `recipient`: 123 (optional, number)
+ `program_activity`: 123 (optional, number)

## SpendingExplorerGeneralResponse (object)
+ `code`: `019` (required, string)
+ `id`: `315` (required, string)
+ `type`: `agency` (required, string)
    The `type` will always be equal to the `type` parameter you provided in the request.
+ `name`: `Department of State` (required, string)
+ `amount`: 63036894632.2 (required, number)

### SpendingExplorerDetailedResponse (object)
+ `code`: `0006` (required, string)
+ `id`: `11367` (required, string)
+ `type`: `program_activity` (required, string)
    The `type` will always be equal to the `type` parameter you provided in the request.
+ `name`: `Law Enforcement Operations` (required, string)
+ `amount`: 1116815570.99 (required, number)
+ `account_number`: `123-4567` (optional, string)
    The response includes `account_number` when the requested `type` was `federal_account`.

## SpendingExplorerGeneralUnreportedResponse (object)
+ `code` (optional, nullable)
+ `id` (optional, nullable)
+ `type`: `agency` (required, string)
    The `type` will always be equal to the `type` parameter you provided in the request.
+ `name`: `Unreported Data` (required, string)
+ `amount`: 63036894632.2 (required, number)