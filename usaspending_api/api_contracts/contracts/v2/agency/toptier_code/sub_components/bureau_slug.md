FORMAT: 1A
HOST: https://api.usaspending.gov

# List Federal Accounts by Sub-Component [/api/v2/agency/{toptier_code}/sub_components/{bureau_slug}/{?fiscal_year,agency_type,order,sort,page,limit}]

Returns a list of Federal Accounts in the Agency's appropriations for a single fiscal year, filtered on the given Sub-Component

## GET

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `bureau_slug`: `bureau_of_the_census` (required, string) The id of the Sub-Component to filter on
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `agency_type` (optional, enum[string])
        Indicated if the data should be pulled from the awarding agency or the funding agency
        + Default: `awarding`
        + Members
          + `awarding`
          + `funding`
    + `order` (optional, enum[string])
        Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
        + Default: `desc`
        + Members
            + `desc`
            + `asc`
    + `sort` (optional, enum[string])
        Optional parameter indicating what value results should be sorted by.
        + Default: `total_budgetary_resources`
        + Members
            + `name`
            + `id` Federal Account Number
            + `total_obligations`
            + `total_budgetary_resources`
    + `page` (optional, number)
        The page number that is currently returned.
        + Default: 1
    + `limit` (optional, number)
        How many results are returned.
        + Default: 10

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `bureau_slug` (required, string)
        + `fiscal_year` (required, number)
        + `totals` (required, TotalObject)
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
        + `results` (required, array[FederalAccount], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "073",
                "bureau_slug": "bureau_of_the_census",
                "fiscal_year": 2018,
                "totals": {
                    "total_budgetary_resources": 400000,
                    "total_obligations": 200000.72,
                    "total_outlays": 393012.0   
                }                                      
                "page_metadata": {
                    "page": 1,
                    "total": 1,
                    "limit": 2,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false,
                },
                "results": [
                    {
                        "name": "Salaries and Expenses",
                        "id": "123-4567",
                        "total_budgetary_resources": 400000,
                        "total_obligations": 200000.72,
                        "total_outlays": 393012.0                
                    }
                ],
                "messages": []
            }

# Data Structures

## PageMetadata (object)
+ `page` (required, number)
+ `total` (required, number)
+ `limit` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

## FederalAccount (object)
+ `name` (required, string)
+ `id` (required, string) Federal Account Number. Numeric 3-digit agency identifier followed by a dash and a numeric 4-digit main account code
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)

## TotalObject (object)
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)
+ `total_outlays` (required, number)
