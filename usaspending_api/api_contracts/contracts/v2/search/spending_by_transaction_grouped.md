FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By Transaction Grouped [/api/v2/search/spending_by_transaction_grouped/]

## POST

Searches for transaction records based on a provided set of filters and groups them by their prime award.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
            These filters are applied at the Transaction level (`children`). They will not affect the Prime Awards (`results`) themselves. Must at least provide the following in the filters object, `keywords` and `award_type_codes`.
        + `fields` (required, FieldNameObject)
            The field names to include for `children` objects (Transactions) the response.
        + `limit`: 5 (optional, number)
            The number of `results` (Prime Awards) to include per page. The number of `children` (Transactions) objects will always be limited to 10
            + Default: 10
        + `page`: 1 (optional, number)
            The page of `results` (Prime Awards) to return based on `limit`.
            + Default: 1
        + `sort`: `Prime Award ID` (required, string)
            The field on which to order `results` objects (Prime Awards) in the response. The `children` (Transaction) object's sort will always be `Transaction Amount`
            + Default: `Prime Award ID`
        + `order` (optional, enum[string])
            The direction in which to order `results` (Prime Awards). `asc` for ascending or `desc` for descending. The `children` (Transaction) objects will always be ordered `asc`
            + Default: `desc`
            + Members
                + `asc`
                + `desc`
    + Body


            {
                "filters": {
                    "keywords": ["test"],
                    "award_type_codes": [
                        "A",
                        "B",
                        "C",
                        "D"
                    ]
                },
                "fields": [
                    "Award ID",
                    "Mod",
                    "Recipient Name",
                    "Action Date",
                    "Transaction Amount",
                    "Awarding Agency",
                    "Awarding Sub Agency",
                    "Award Type"
                ],
                "page": 1,
                "limit": 35,
                "sort": "Prime Award ID",
                "order": "desc"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[AwardGroupResponse], fixed-type)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
        + `limit`: 10 (required, number)

    + Body

            {
                "limit": 10,
                "page_metadata": {
                    "page": 1,
                    "hasPrevious": false,
                    "hasNext": true,
                    "previous": null,
                    "next": 2
                },
                "results": [
                    {
                        "Prime Award ID": "HSSCCG10J00265",
                        "Matching Transaction Count": 4,
                        "Matching Transaction Obligation": 49142818.18,
                        "children": [
                            {
                                "Awarding Sub Agency": "U.S. Citizenship and Immigration Services",
                                "Award ID": "HSSCCG10J00265",
                                "Awarding Agency": "Department of Homeland Security",
                                "Action Date": "2012-06-20",
                                "internal_id": "24261000",
                                "Mod": "P00011",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "15639242.36",
                                "generated_internal_id": "CONT_AWD_HSSCCG10J00265_7003_HSHQDC06D00026_7001",
                                "Award Type": "DELIVERY ORDER"
                            },
                            {
                                "Awarding Sub Agency": "U.S. Citizenship and Immigration Services",
                                "Award ID": "HSSCCG10J00265",
                                "Awarding Agency": "Department of Homeland Security",
                                "Action Date": "2011-06-21",
                                "internal_id": "24261000",
                                "Mod": "P00008",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "15226100.09",
                                "generated_internal_id": "CONT_AWD_HSSCCG10J00265_7003_HSHQDC06D00026_7001",
                                "Award Type": "DELIVERY ORDER"
                            },
                            {
                                "Awarding Sub Agency": "U.S. Citizenship and Immigration Services",
                                "Award ID": "HSSCCG10J00265",
                                "Awarding Agency": "Department of Homeland Security",
                                "Action Date": "2013-06-25",
                                "internal_id": "24261000",
                                "Mod": "P00019",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "10497321.20",
                                "generated_internal_id": "CONT_AWD_HSSCCG10J00265_7003_HSHQDC06D00026_7001",
                                "Award Type": "DELIVERY ORDER"
                            },
                            {
                                "Awarding Sub Agency": "U.S. Citizenship and Immigration Services",
                                "Award ID": "HSSCCG10J00265",
                                "Awarding Agency": "Department of Homeland Security",
                                "Action Date": "2010-09-20",
                                "internal_id": "24261000",
                                "Mod": "0",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "7780154.53",
                                "generated_internal_id": "CONT_AWD_HSSCCG10J00265_7003_HSHQDC06D00026_7001",
                                "Award Type": "DELIVERY ORDER"
                            }
                        ]
                    },
                    {
                        "Prime Award ID": "HHSM500201600013U",
                        "Matching Transaction Count": 4,
                        "Matching Transaction Obligation": 29207504.00,
                        "children": [
                            {
                                "Awarding Sub Agency": "Centers for Medicare and Medicaid Services",
                                "Award ID": "HHSM500201600013U",
                                "Awarding Agency": "Department of Health and Human Services",
                                "Action Date": "2019-08-26",
                                "internal_id": "23325500",
                                "Mod": "P00006",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "8303064.00",
                                "generated_internal_id": "CONT_AWD_HHSM500201600013U_7530_GS00Q09BGD0039_4735",
                                "Award Type": "DELIVERY ORDER"
                            },
                            {
                                "Awarding Sub Agency": "Centers for Medicare and Medicaid Services",
                                "Award ID": "HHSM500201600013U",
                                "Awarding Agency": "Department of Health and Human Services",
                                "Action Date": "2018-08-27",
                                "internal_id": "23325500",
                                "Mod": "P00003",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "7964806.00",
                                "generated_internal_id": "CONT_AWD_HHSM500201600013U_7530_GS00Q09BGD0039_4735",
                                "Award Type": "DELIVERY ORDER"
                            },
                            {
                                "Awarding Sub Agency": "Centers for Medicare and Medicaid Services",
                                "Award ID": "HHSM500201600013U",
                                "Awarding Agency": "Department of Health and Human Services",
                                "Action Date": "2018-01-11",
                                "internal_id": "23325500",
                                "Mod": "P00002",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "6600114.00",
                                "generated_internal_id": "CONT_AWD_HHSM500201600013U_7530_GS00Q09BGD0039_4735",
                                "Award Type": "DELIVERY ORDER"
                            },
                            {
                                "Awarding Sub Agency": "Centers for Medicare and Medicaid Services",
                                "Award ID": "HHSM500201600013U",
                                "Awarding Agency": "Department of Health and Human Services",
                                "Action Date": "2017-07-06",
                                "internal_id": "23325500",
                                "Mod": "5",
                                "Recipient Name": "LEIDOS, INC.",
                                "Transaction Amount": "6339520.00",
                                "generated_internal_id": "CONT_AWD_HHSM500201600013U_7530_GS00Q09BGD0039_4735",
                                "Award Type": "DELIVERY ORDER"
                            }
                        ]
                    }
                ]
            }

# Data Structures

## Request Objects

### AdvancedFilterObject (object)
The filters available are defined in [AdvancedFilterObject](./spending_by_transaction.md#advanced-filter-object). The one difference is that `keywords` is not required.

### FieldNameObject (array)
List of column names to request

- `Action Date`
- `Award ID`
- `Award Type`
- `Awarding Agency`
- `Awarding Sub Agency`
- `awarding_agency_id`
- `Funding Agency`
- `Funding Sub Agency`
- `internal_id`
- `generated_internal_id`
- `Issued Date`
- `Last Date to Order`
- `Loan Value`
- `Mod`
- `Recipient Name`
- `Subsidy Cost`
- `Transaction Amount`
- `def_codes`

## Response Objects

### AwardGroupResponse (object)
+ `Prime Award ID` (required, string)
+ `Matching Transaction Count` (required, number)
+ `Matching Transaction Obligation` (required, number)
+ `children` (required, array[TransactionResponse], fixed-type)
    Regardless of the number of matching Transactions associated with a Prime Award, only 10 at most will be included in this `children` section.
    To retrieve more Transactions with the provided filters associated with the Award, use the [api/v2/search/spending_by_transaction/](./spending_by_transaction.md) endpoint with an additional filter for Award ID.
+ `page_metadata` (required, PageMetaDataObject, fixed-type)
+ `limit`: 10 (required, number)

### TransactionResponse (object)

#### Sample
+ `Action Date`: `2018-05-21` (required, string, nullable)
+ `Award ID`: `DTFAWA05C00031R` (required, string, nullable)
+ `Award Type`: `INDEFINITE DELIVERY / INDEFINITE QUANTITY` (required, string, nullable)
+ `Awarding Agency`: `Department of Transportation` (required, string, nullable)
+ `Awarding Sub Agency`: `Federal Aviation Administration` (required, string, nullable)
+ `internal_id`: `68856340` (required, string, nullable)
+ `generated_internal_id`: `CONT_AWD_00013U_7090_KJ88_4735` (required, string, nullable)
+ `Mod`: `P00206` (required, string, nullable)
+ `Recipient Name`: `LEIDOS INNOVATIONS CORPORATION` (required, string, nullable)
+ `Transaction Amount`: `40000000.00` (required, string, nullable)

#### Default
+ `Action Date` (required, string, nullable)
+ `Award ID` (required, string, nullable)
+ `Award Type` (required, string, nullable)
+ `Awarding Agency` (required, string, nullable)
+ `Awarding Sub Agency` (required, string, nullable)
+ `awarding_agency_id` (required, string, nullable)
+ `Funding Agency` (required, string, nullable)
+ `Funding Sub Agency` (required, string, nullable)
+ `internal_id` (required, string, nullable)
+ `generated_internal_id` (required, string, nullable)
+ `Issued Date` (required, string, nullable)
+ `Last Date to Order` (required, string, nullable)
+ `Loan Value` (required, string, nullable)
+ `Mod` (required, string, nullable)
+ `Recipient Name` (required, string, nullable)
+ `Subsidy Cost` (required, string, nullable)
+ `Transaction Amount` (required, string, nullable)
+ `Transaction Description` (required, string, nullable)
+ `Action Type` (required, string, nullable)
+ `Recipient UEI` (required, string, nullable)
+ `Recipient Location` (required, string, nullable)
+ `Place of Performance` (required, string, nullable)
+ `Assistance Listing` (required, string, nullable)

### PageMetaDataObject (object)
+ `page`: 1 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)