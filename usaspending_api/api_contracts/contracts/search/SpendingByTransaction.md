FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/search/spending_by_transaction/]

Returns transaction records which match the keyword and award type code filters.

## POST

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, FilterObject)
            Need to provide `keywords` and `award_type_codes`
        + `fields` (required, FieldNameObject)
            The field names to include in the response
        + `limit`: 5 (optional, number)
            The number of results to include per page.
            + Default: 10
        + `page`: 1 (optional, number)
            The page of results to return based on `limit`.
            + Default: 1
        + `sort`: `Transaction Amount` (required, string)
            The field on which to order `results` in the response.
            + Default: `Transaction Amount`
        + `order` (optional, enum[string])
            The direction in which to order results. `asc` for ascending or `desc` for descending.
            + Default: `desc`
            + Members
                + `asc`
                + `desc`

+ Response 200 (application/json)
    + Attributes
        + results (required, array[TransactionResponse], fixed-type)
        + page_metadata (required, PageMetaDataObject, fixed-type)
        + limit: 10 (required, number)

    + Body

            {
                "limit": 35,
                "page_metadata": {
                    "page": 1,
                    "hasPrevious": false,
                    "hasNext": true,
                    "previous": null,
                    "next": 2
                },
                "results": [
                    {
                        "Transaction Amount": "40000000.00",
                        "Award Type": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
                        "Awarding Agency": "Department of Transportation",
                        "internal_id": "68856340",
                        "Action Date": "2018-05-21",
                        "Recipient Name": "LEIDOS INNOVATIONS CORPORATION",
                        "Mod": "P00206",
                        "Award ID": "DTFAWA05C00031R",
                        "Awarding Sub Agency": "Federal Aviation Administration"
                    },
                    {
                        "Transaction Amount": "30000000.00",
                        "Award Type": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
                        "Awarding Agency": "Department of Transportation",
                        "internal_id": "68856340",
                        "Action Date": "2018-09-26",
                        "Recipient Name": "LEIDOS INNOVATIONS CORPORATION",
                        "Mod": "P00216",
                        "Award ID": "DTFAWA05C00031R",
                        "Awarding Sub Agency": "Federal Aviation Administration"
                    },
                    {
                        "Transaction Amount": "9920443.99",
                        "Award Type": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
                        "Awarding Agency": "Department of Transportation",
                        "internal_id": "68856340",
                        "Action Date": "2018-09-19",
                        "Recipient Name": "LEIDOS INNOVATIONS CORPORATION",
                        "Mod": "P00215",
                        "Award ID": "DTFAWA05C00031R",
                        "Awarding Sub Agency": "Federal Aviation Administration"
                    },
                    {
                        "Transaction Amount": "6384553.00",
                        "Award Type": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
                        "Awarding Agency": "Department of Transportation",
                        "internal_id": "68856340",
                        "Action Date": "2018-05-01",
                        "Recipient Name": "LEIDOS INNOVATIONS CORPORATION",
                        "Mod": "P00205",
                        "Award ID": "DTFAWA05C00031R",
                        "Awarding Sub Agency": "Federal Aviation Administration"
                    },
                    {
                        "Transaction Amount": "5052922.00",
                        "Award Type": "INDEFINITE DELIVERY / INDEFINITE QUANTITY",
                        "Awarding Agency": "Department of Transportation",
                        "internal_id": "69009914",
                        "Action Date": "2018-04-24",
                        "Recipient Name": "LEIDOS INNOVATIONS CORPORATION",
                        "Mod": "P00120",
                        "Award ID": "DTFAWA03C00059R",
                        "Awarding Sub Agency": "Federal Aviation Administration"
                    }
                ]
            }

# Data Structures

## PageMetaDataObject (object)
+ `page`: 1 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)

## TransactionResponse (object)

### Sample
+ `Action Date`: `2018-05-21` (required, string, nullable)
+ `Award ID`: `DTFAWA05C00031R` (required, string, nullable)
+ `Award Type`: `INDEFINITE DELIVERY / INDEFINITE QUANTITY` (required, string, nullable)
+ `Awarding Agency`: `Department of Transportation` (required, string, nullable)
+ `Awarding Sub Agency`: `Federal Aviation Administration` (required, string, nullable)
+ `internal_id`: `68856340` (required, string, nullable)
+ `Mod`: `P00206` (required, string, nullable)
+ `Recipient Name`: `LEIDOS INNOVATIONS CORPORATION` (required, string, nullable)
+ `Transaction Amount`: `40000000.00` (required, string, nullable)

### Default
+ `Action Date`: `2018-05-21` (required, string, nullable)
+ `Award ID`: `DTFAWA05C00031R` (required, string, nullable)
+ `Award Type`: `INDEFINITE DELIVERY / INDEFINITE QUANTITY` (required, string, nullable)
+ `Awarding Agency`: `Department of Transportation` (required, string, nullable)
+ `Awarding Sub Agency`: `Federal Aviation Administration` (required, string, nullable)
+ `awarding_agency_id`: `239` (required, string, nullable)
+ `Funding Agency`: `Department of Agriculture` (required, string, nullable)
+ `Funding Sub Agency`: `Forest Service` (required, string, nullable)
+ `internal_id`: `68856340` (required, string, nullable)
+ `Issued Date`: `2018-05-21` (required, string, nullable)
+ `Last Date to Order`: `2022-05-21` (required, string, nullable)
+ `Loan Value`: `1400.00` (required, string, nullable)
+ `Mod`: `P00206` (required, string, nullable)
+ `Recipient Name`: `LEIDOS INNOVATIONS CORPORATION` (required, string, nullable)
+ `Subsidy Cost`: `91400.00` (required, string, nullable)
+ `Transaction Amount`: `40000000.00` (required, string, nullable)


## FilterObjectAwardTypes (array)
List of filterable award types

### Sample
- IDV_A
- IDV_B
- IDV_B_A
- IDV_B_B
- IDV_B_C
- IDV_C
- IDV_D
- IDV_E

### Default
- 02
- 03
- 04
- 05
- 06
- 07
- 08
- 09
- 10
- 11
- A
- B
- C
- D
- IDV_A
- IDV_B
- IDV_B_A
- IDV_B_B
- IDV_B_C
- IDV_C
- IDV_D
- IDV_E

## FieldNameObject (array)
List of column names to request

### Sample
- Action Date
- Award ID
- Award Type
- Awarding Agency
- Awarding Sub Agency
- Mod
- Recipient Name
- Transaction Amount

### Default
- Action Date
- Award ID
- Award Type
- Awarding Agency
- Awarding Sub Agency
- awarding_agency_id
- Funding Agency
- Funding Sub Agency
- internal_id
- Issued Date
- Last Date to Order
- Loan Value
- Mod
- Recipient Name
- Subsidy Cost
- Transaction Amount


## FilterObject (object)
+ `keywords`: `lockheed` (required, array[string], fixed-type)
+ `award_type_codes` (required, FilterObjectAwardTypes, fixed-type)
    See use at
    https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation#award-type