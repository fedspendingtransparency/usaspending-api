FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Duns [/api/v2/recipient/duns/]

These endpoints are used to power USAspending.gov's recipient profile pages. This data can be used to visualize the government spending that pertains to a specific recipient.

## List Recipients [POST /api/v2/recipient/duns/]

This endpoint returns a list of recipients, their level, DUNS, and amount.

+ Request (application/json)
    + Attributes (object)
        + order: desc (optional, string)
            The direction results are sorted by. `asc` for ascending, `desc` for descending.
            + Default: desc
        + sort: amount (optional, enum[string])
            The field results are sorted by.
            + Default: amount
            + Members
                + name
                + duns
                + amount
        + limit: 50 (optional, number)
            The number of results to include per page.
            + Default: 50
        + page: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + keyword (optional, string)
            The keyword results are filtered by. Searches on name and DUNS.
        + award_type: all (optional, enum[string])
            The award type results are filtered by.
            + Default: all
            + Members
                + all
                + contracts
                + grants
                + loans
                + direct_payments
                + other_financial_assistance

+ Response 200 (application/json)
    + Attributes (RecipientsListResponse)

## Get Recipient Overview [GET /api/v2/recipient/duns/{recipient_id}/{?year}]

This endpoint returns a high-level overview of a specific recipient, given its id.

+ Parameters

    + `recipient_id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-C` (required, string)
        A unique identifier for the recipient at a specific level (parent, child, or neither).
    + year: `2017` (optional, string)
        The fiscal year you would like data for. Use `all` to view all time or `latest` to view the latest 12 months.

+ Response 200 (application/json)

    + Attributes (RecipientOverview)

# Data Structures

## RecipientsListResponse (object)
+ page_metadata (PageMetaDataObject)
+ results (array[RecipientListing], fixed-type)

## RecipientListing (object)
+ name: The ABC Corporation (required, string, nullable)
    Name of the recipient. `null` when the name is not provided.
+ duns: `001006360` (required, string, nullable)
    Recipient's DUNS (Data Universal Numbering System) number. `null` when no DUNS is provided.
+ id: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-C` (required, string)
    A unique identifier for the recipient at this `recipient_level`.
+ amount: 30020000000 (required, number)
    The aggregate monetary value of all transactions associated with this recipient for the trailing 12 months.
+ `recipient_level`: C (required, enum[string])
    A letter representing the recipient level. `R` for neither parent nor child, `P` for Parent Recipient, or `C` for child recipient.
    + Members
        + R
        + P
        + C

## PageMetaDataObject (object)
+ page: 1 (required, number)
    The page number.
+ limit: 50 (required, number)
    The number of results per page.
+ total: 101 (required, number)
    The total number of results (all pages).

## RecipientOverview (object)
+ name: The ABC Corporation (required, string, nullable)
    Name of the recipient. `null` when the name is not provided.
+ duns: `001006360` (required, string, nullable)
    Recipient's DUNS (Data Universal Numbering System) number. `null` when no DUNS is provided.
+ `recipient_id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-C` (required, string)
    A unique identifier for the recipient.
+  parents (required, array[ParentRecipient], fixed-type)
+ `parent_name`: The XYZ Corporation (required, string, nullable)
    Parent recipient's name. `null` if the recipient does not have a parent recipient.
+ `parent_duns`: `001006361` (required, string, nullable)
    Parent recipient's DUNS number. `null` if the recipient does not have a parent recipient, or the parent recipient's DUNS is not provided.
+ `parent_id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-P` (required, string, nullable)
    A unique identifier for the parent recipient. `null` if the recipient does not have a parent recipient.
+ location (required, RecipientLocation, fixed-type)
+ `business_types`: `minority_owned_business`, `for_profit_organization` (required, array[string], fixed-type)
    An array of business type field names used to categorize recipients.
+ `total_transaction_amount`: 30020000000 (required, number)
    The aggregate monetary value of all transactions associated with this recipient for the given time period.
+ `total_transactions`: 327721 (required, number)
    The number of transactions associated with this recipient for the given time period.
+ `recipient_level`: C (required, enum[string])
    A letter representing the recipient level. `R` for neither parent nor child, `P` for Parent Recipient, or `C` for child recipient.
    + Members
        + R
        + P
        + C

## RecipientLocation (object)
+ `address_line1`: 123 Sesame St (required, string, nullable)
    The first line of the recipient's street address.
+ `address_line2` (required, string, nullable)
    Second line of the recipient's street address.
+ `address_line3` (required, string, nullable)
    Third line of the recipient's street address.
+ `foreign_province` (required, string, nullable)
    Name of the province in which the recipient is located, if it is outside the United States.
+ `city_name`: McLean (required, string, nullable)
    Name of the city in which the recipient is located.
+ `county_name` (required, string, nullable)
    Name of the county in which the recipient is located.
+ `state_code`: VA (required, string, nullable)
    Code for the state in which the recipient is located.
+ zip: `22102` (required, string, nullable)
    Recipient's zip code (5 digits)
+ zip4 (required, string, nullable)
    Recipient's zip code (4 digits)
+ `foreign_postal_code` (required, string, nullable)
    Recipient's postal code, if it is outside the United States.
+ `country_name` (required, string, nullable)
     Name of the country in which the recipient is located.
+ `country_code`: USA (required, string, nullable)
     Code for the country in which the recipient is located.
+ `congressional_code`: `05` (required, string, nullable)
    Number for the recipient's congressional district.

## ParentRecipient (object)
+ `parent_name`: The XYZ Corporation (required, string, nullable)
+ `parent_duns`: `001006361` (required, string, nullable)
    DUNS number
+ `parent_id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-P` (required, string, nullable)
    A unique identifier for the parent recipient.
 