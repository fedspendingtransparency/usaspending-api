FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Duns [/api/v2/recipient/duns/]

These endpoints are used to power USAspending.gov's recipient profile pages. This data can be used to visualize the government spending that pertains to a specific recipient.

## POST

This endpoint returns a list of recipients, their level, DUNS, and amount.

+ Request (application/json)
    + Attributes (object)
        + `order`: `desc` (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: `desc`
        + `sort`: `amount` (optional, enum[string])
            The field results are sorted by.
            + Default: `amount`
            + Members
                + `name`
                + `duns`
                + `amount`
        + `limit`: 50 (optional, number)
            The number of results to include per page.
            + Default: 50
        + `page`: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + `keyword` (optional, string)
            The keyword results are filtered by. Searches on name and DUNS.
        + `award_type`: `all` (optional, enum[string])
            The award type results are filtered by.
            + Default: `all`
            + Members
                + `all`
                + `contracts`
                + `grants`
                + `loans`
                + `direct_payments`
                + `other_financial_assistance`

+ Response 200 (application/json)
    + Attributes (RecipientsListResponse)

# Data Structures

## RecipientsListResponse (object)
+ `page_metadata` (PageMetaDataObject)
+ `results` (array[RecipientListing], fixed-type)

## RecipientListing (object)
+ `name`: `The ABC Corporation` (required, string, nullable)
    Name of the recipient. `null` when the name is not provided.
+ `duns`: `001006360` (required, string, nullable)
    Recipient's DUNS (Data Universal Numbering System) number. `null` when no DUNS is provided.
+ `id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-C` (required, string)
    A unique identifier for the recipient at this `recipient_level`.
+ `amount`: 30020000000 (required, number)
    The aggregate monetary value of all transactions associated with this recipient for the trailing 12 months.
+ `recipient_level`: `C` (required, enum[string])
    A letter representing the recipient level. `R` for neither parent nor child, `P` for Parent Recipient, or `C` for child recipient.
    + Members
        + `R`
        + `P`
        + `C`

## PageMetaDataObject (object)
+ `page`: 1 (required, number)
    The page number.
+ `limit`: 50 (required, number)
    The number of results per page.
+ `total`: 101 (required, number)
    The total number of results (all pages).
