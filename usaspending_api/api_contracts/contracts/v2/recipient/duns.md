FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Duns [/api/v2/recipient/duns/]

*Deprecated: Please see the following API contract instead: [usaspending_api/api_contracts/contracts/v2/recipient.md](../recipient.md)*

These endpoints are used to power USAspending.gov's recipient profile pages. This data can be used to visualize the government spending that pertains to a specific recipient.

## POST

This endpoint returns a list of recipients, their level, DUNS, and amount.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `order` (optional, enum[string])
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: `desc`
            + Members
                + `asc`
                + `desc`
        + `sort` (optional, enum[string])
            The field results are sorted by.
            + Default: `amount`
            + Members
                + `name`
                + `duns`
                + `amount`
        + `limit` (optional, number)
            The number of results to include per page. Maximum: 1000
            + Default: 50
        + `page` (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + `keyword` (optional, string)
            The keyword results are filtered by. Searches on name, UEI, or DUNS.
        + `award_type` (optional, enum[string])
            The award type results are filtered by.
            + Default: `all`
            + Members
                + `all`
                + `contracts`
                + `grants`
                + `loans`
                + `direct_payments`
                + `other_financial_assistance`

    + Body
            
            
            {
                "order": "desc",
                "sort": "amount",
                "page": 1,
                "limit": 50,
                "award_type": "all"
            }


+ Response 200 (application/json)
    + Attributes (object)
        + `page_metadata` (PageMetaDataObject)
        + `results` (array[RecipientListing], fixed-type)

    + Body


            {
                "page_metadata": {
                    "page": 1,
                    "total": 6426446,
                    "limit": 50,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false
                },
                "results": [
                    {
                        "id": "1c3edaaa-611b-840c-bf2b-fd34df49f21f-P",
                        "duns": "071549000",
                        "uei": "NUDGYLBB4S99",                
                        "name": "CALIFORNIA, STATE OF",
                        "recipient_level": "P",
                        "amount": 125645635660.93
                    },
                    {
                        "id": "9f972e57-c05f-b374-bf11-0408f30da215-R",
                        "duns": null,
                        "uei": null,                
                        "name": "OPTUM BANK",
                        "recipient_level": "R",
                        "amount": 99578072991.9
                    },
                    {
                        "id": "b0e81504-fe4b-96fb-ae3c-27b1a7fb4cda-P",
                        "duns": "041002973",
                        "uei": "DKBAJQ45GQS8",                
                        "name": "NEW YORK, STATE OF",
                        "recipient_level": "P",
                        "amount": 81273339054.44
                    },
                    {
                        "id": "bf782d74-80d4-581e-ae9f-3b5b57092d7d-C",
                        "duns": "796528263",
                        "uei": "JE73CDQUAPA7",                
                        "name": "HEALTH CARE SERVICES, CALIFORNIA DEPARTMENT OF",
                        "recipient_level": "C",
                        "amount": 66803275488.0
                    }
                ]
            }




# Data Structures

## RecipientListing (object)
+ `name` (required, string, nullable)
    Name of the recipient. `null` when the name is not provided.
+ `duns` (required, string, nullable)
    Recipient's DUNS (Data Universal Numbering System) number. `null` when no DUNS is provided.
+ `uei` (required, string, nullable)
    Recipient's UEI (Unique Entity Identifier). `null` when no UEI is provided.
+ `id` (required, string)
    A unique identifier for the recipient at this `recipient_level`.
+ `amount` (required, number)
    The aggregate monetary value of all transactions associated with this recipient for the trailing 12 months.
+ `recipient_level` (required, enum[string])
    A letter representing the recipient level. `R` for neither parent nor child, `P` for Parent Recipient, or `C` for child recipient.
    + Members
        + `R`
        + `P`
        + `C`

## PageMetaDataObject (object)
+ `page` (required, number)
    The page number.
+ `limit` (required, number)
    The number of results per page.
+ `total` (required, number)
    The total number of results (all pages).
