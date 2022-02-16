FORMAT: 1A
HOST: https://api.usaspending.gov

# List Federal Accounts [/api/v2/agency/{toptier_code}/sub_agency/{?fiscal_year,agency_type,award_type_codes,order,sort,page,limit}]

Returns a list of Federal Accounts and Treasury Accounts in the Agency's appropriations for a single fiscal year

## GET

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `award_type_codes` (optional, array[AwardTypeCodes])
        This will filter the subagencies by the award types provided
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
        + Default: `total_obligations`
        + Members
            + `name`
            + `total_obligations`
            + `transaction_count`
            + `new_award_count`
    + `page` (optional, number)
        The page number that is currently returned.
        + Default: 1
    + `limit` (optional, number)
        How many results are returned.
        + Default: 10

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
        + `results` (required, array[SubAgency], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "073",
                "fiscal_year": 2018,
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
                        "name": "Small Business Administration",
                        "abbreviation": "SBA",
                        "total_obligations": 553748221.72,
                        "transaction_count": 14358,
                        "new_award_count": 13266,
                        "children": [
                            {
                                "name": "OFC OF CAPITAL ACCESS",
                                "code": "737010",
                                "total_obligations": 549195419.92,
                                "transaction_count": 13410,
                                "new_award_count": 12417
                            },
                            {
                                "name": "OFC OF DISASTER ASSISTANCE",
                                "code": "732990",
                                "total_obligations": 4577429.17,
                                "transaction_count": 943,
                                "new_award_count": 576
                            }                        
                        ]
                    }
                ],
                "messages": []
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

## PageMetadata (object)
+ `page` (required, number)
+ `total` (required, number)
+ `limit` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

## SubAgency (object)
+ `name` (required, string)
+ `abbreviation` (required, nullable, string)
+ `total_obligations` (required, number)
+ `transaction_count` (required, number)
+ `new_award_count` (required, number)
+ `children` (required, array[Office], fixed-type)

## Office (object)
+ `name` (required, string)
+ `code` (required, string)
+ `total_obligations` (required, number)
+ `transaction_count` (required, number)
+ `new_award_count` (required, number)
