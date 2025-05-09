FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By Transaction [/api/v2/search/spending_by_transaction/]

## POST

Returns transaction records which match the provided filters.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
            Need to provide `award_type_codes`
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

## Request Objects

### PageMetaDataObject (object)
+ `page`: 1 (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)

### AdvancedFilterObject (object)
+ `keywords`: `lockheed` (optional, array[string], fixed-type)
+ `description` (optional, string)
+ `time_period` (optional, array[TimePeriodObject], fixed-type)
+ `award_type_codes` (required, FilterObjectAwardTypes, fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `place_of_performance_locations` (optional, array[LocationObject], fixed-type)
+ `agencies` (optional, array[AgencyObject], fixed-type)
+ `recipient_search_text`: [`Hampton`, `Roads`] (optional, array[string])
    + Text searched across a recipient’s name, UEI, and DUNS
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[LocationObject], fixed-type)
+ `recipient_type_names`: [`category_business`, `sole_proprietorship`] (optional, array[string])
+ `award_ids`: [`SPE30018FLGFZ`, `SPE30018FLJFN`] (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_amounts` (optional, array[AwardAmounts], fixed-type)
+ `program_numbers`: [`10.331`] (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `contract_pricing_type_codes`: [`J`] (optional, array[string])
+ `set_aside_type_codes`: [`NONE`] (optional, array[string])
+ `extent_competed_type_codes`: [`A`] (optional, array[string])
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)
+ `program_activity` (optional, array[number])
+ `program_activities` (optional, array[ProgramActivityObject])
    A filter option that supports filtering by a program activity name or code. Please note that if this filter is used at least one of the members of the object, ProgramActivityObject, need to be provided.
+ `def_codes` (optional, array[DEFC], fixed-type)
    If the `def_codes` provided are in the COVID-19 or IIJA group, the query will only return transactions that meet two requirements:
    1. The transaction's associated prime award has at least one File C record with one of the supplied DEFCs.
    2. The matching DEFC's associated public law has an enactment date prior to the transaction's action_date.
+ `award_unique_id` (optional, string)

### LocationObject (object)
These fields are defined in the [StandardLocationObject](../../../search_filters.md#standard-location-object)

### AgencyObject (object)
+ `type` (required, enum[string])
    + Members
        + `awarding`
        + `funding`
+ `tier` (required, enum[string])
    + Members
        + `toptier`
        + `subtier`
+ `name`: `Office of Inspector General` (required, string)
+ `toptier_name`: `Department of the Treasury` (optional, string)
    Only applicable when `tier` is `subtier`.  Ignored when `tier` is `toptier`.  Provides a means by which to scope subtiers with common names to a
    specific toptier.  For example, several agencies have an "Office of Inspector General".  If not provided, subtiers may span more than one toptier.

### ProgramActivityObject (object)
At least one of the following fields are required when using the ProgramActivityObject.
+ `name`: (optional, string)
+ `code`: (optional, number)

### AwardAmounts (object)
+ `lower_bound` (optional, number)
+ `upper_bound`: 1000000 (optional, number)

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

### PSCCodeObject (object)
+ `require`: [[`Service`, `B`, `B5`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`Service`, `B`, `B5`, `B502`]] (optional, array[array[string]], fixed-type)

### TASCodeObject (object)
+ `require`: [[`091`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`091`, `091-0800`]] (optional, array[array[string]], fixed-type)

### TreasuryAccountComponentsObject (object)
+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier - three characters
+ `aid` (required, string)
    Agency Identifier - three characters
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability - four digits
+ `epoa` (optional, string, nullable)
    Ending Period of Availability - four digits
+ `a` (optional, string, nullable)
    Availability Type Code - X or null
+ `main` (required, string)
    Main Account Code - four digits
+ `sub` (optional, string, nullable)
    Sub-Account Code - three digits

###  TimePeriodObject

**Description:**
Search based on one or more date range using the transaction's `action_date` field. Dates should be in the following format: YYYY-MM-DD

**Example**
```
{
    "time_period": [
        {
            "start_date": "2016-10-01",
            "end_date": "2017-09-30"
        }
    ]
}
```

Request parameter description:
+ `start_date`: (required)
    See [Time Period](#time-period)
+ `end_date`: (required)
    See [Time Period](#time-period)

### FilterObjectAwardTypes (array)
List of filterable award types

#### Sample
- `IDV_A`
- `IDV_B`
- `IDV_B_A`
- `IDV_B_B`
- `IDV_B_C`
- `IDV_C`
- `IDV_D`
- `IDV_E`

#### Default
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

### DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)

### FieldNameObject (array)
List of column names to request
- `Action Date`
- `Action Type`
- `Assistance Listing`
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
- `NAICS`
- `PSC`
- `Primary Place of Performance`
- `Recipient Location`
- `Recipient Name`
- `Recipient UEI`
- `Subsidy Cost`
- `Transaction Amount`
- `Transaction Description`
- `def_codes`

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