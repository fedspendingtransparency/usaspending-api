FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/search/spending_by_transaction/]

## POST

Returns transaction records which match the keyword and award type code filters.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
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
                "sort": "Transaction Amount",
                "order": "desc"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[TransactionResponse], fixed-type)
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
                        "Awarding Sub Agency": "Federal Acquisition Service",
                        "Award ID": "GSTO405BF0033",
                        "Awarding Agency": "General Services Administration",
                        "Action Date": "2008-07-17",
                        "internal_id": "22509500",
                        "Mod": "0",
                        "Recipient Name": "LEIDOS, INC.",
                        "Transaction Amount": "12669957.55",
                        "generated_internal_id": "CONT_AWD_GSTO405BF0033_4735_GS09K99BHD0010_4735",
                        "Award Type": "DO"
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
                        "Awarding Sub Agency": "Defense Finance and Accounting Service",
                        "Award ID": "0148",
                        "Awarding Agency": "Department of Defense",
                        "Action Date": "2008-12-16",
                        "internal_id": "2748500",
                        "Mod": "9",
                        "Recipient Name": "LEIDOS GOVERNMENT SERVICES, INC.",
                        "Transaction Amount": "7796098.43",
                        "generated_internal_id": "CONT_AWD_0148_9700_MDA22001D0002_9700",
                        "Award Type": "DO"
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
+ `generated_internal_id`: `CONT_AWD_00013U_7090_KJ88_4735` (required, string, nullable)
+ `Mod`: `P00206` (required, string, nullable)
+ `Recipient Name`: `LEIDOS INNOVATIONS CORPORATION` (required, string, nullable)
+ `Transaction Amount`: `40000000.00` (required, string, nullable)

### Default
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


## FilterObjectAwardTypes (array)
List of filterable award types

### Sample
- `IDV_A`
- `IDV_B`
- `IDV_B_A`
- `IDV_B_B`
- `IDV_B_C`
- `IDV_C`
- `IDV_D`
- `IDV_E`

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

## FieldNameObject (array)
List of column names to request

### Sample
- `Action Date`
- `Award ID`
- `Award Type`
- `Awarding Agency`
- `Awarding Sub Agency`
- `Mod`
- `Recipient Name`
- `Transaction Amount`

### Default
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


## AdvancedFilterObject (object)
+ `keywords`: `lockheed` (required, array[string], fixed-type)
+ `award_type_codes` (required, FilterObjectAwardTypes, fixed-type)
