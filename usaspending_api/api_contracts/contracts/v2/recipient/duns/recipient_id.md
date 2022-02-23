FORMAT: 1A
HOST: https://api.usaspending.gov

# Specific Recipient Duns [/api/v2/recipient/duns/{recipient_id}/{?year}]

Deprecated: Please see the following API contract instead: usaspending_api/api_contracts/contracts/v2/recipient/recipient.md

These endpoints are used to power USAspending.gov's recipient profile pages. This data can be used to visualize the government spending that pertains to a specific recipient.

## GET

This endpoint returns a high-level overview of a specific recipient, given its id.
+ Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

+ Parameters

    + `recipient_id`: `45700e3c-05bc-6426-ddc6-5e0ce1664716-C` (required, string)
        A unique identifier for the recipient at a specific level (parent, child, or neither).
    + `year` (optional, string)
        The fiscal year you would like data for. Use `all` to view all time or `latest` to view the latest 12 months.



+ Response 200 (application/json)

    + Attributes (RecipientOverview)

    + Body

            {
                "name": "ART LINE WHOLESALERS, INC.",
                "alternate_names": [
                    "ARTLINE WHOLESALERS INC"
                ],
                "duns": "058675323",
                "uei": "NUDGYLBB4S99",        
                "recipient_id": "1c0c11fa-ee74-ced4-75c3-0b5a0d4826db-P",
                "recipient_level": "P",
                "parent_id": "1c0c11fa-ee74-ced4-75c3-0b5a0d4826db-P",
                "parent_name": "ART LINE WHOLESALERS, INC.",
                "parent_duns": "058675323",
                "parent_uei": "D1RMTDTMKLE8",        
                "parents": [
                    {
                        "parent_id": "1c0c11fa-ee74-ced4-75c3-0b5a0d4826db-P",
                        "parent_duns": "058675323",
                        "parent_uei": "D1RMTDTMKLE8",                
                        "parent_name": "ART LINE WHOLESALERS, INC."
                    }
                ],
                "business_types": [
                    "category_business",
                    "corporate_entity_not_tax_exempt",
                    "minority_owned_business",
                    "self_certified_small_disadvanted_business",
                    "small_business",
                    "special_designations",
                    "subcontinent_asian_indian_american_owned_business"
                ],
                "location": {
                    "address_line1": "1 MIDLAND AVE",
                    "address_line2": null,
                    "address_line3": null,
                    "foreign_province": null,
                    "city_name": "HICKSVILLE",
                    "county_name": null,
                    "state_code": "NY",
                    "zip": "11801",
                    "zip4": "4320",
                    "foreign_postal_code": null,
                    "country_name": "UNITED STATES",
                    "country_code": "USA",
                    "congressional_code": "03"
                },
                "total_transaction_amount": 0,
                "total_transactions": 0,
                "total_face_value_loan_amount": 0,
                "total_face_value_loan_transactions": 0
            }

# Data Structures

## RecipientOverview (object)
+ `name` (required, string, nullable)
    Name of the recipient. `null` when the name is not provided.
+ `alternate_names` (required, array[string], fixed-type)
    Additional names that the recipient has been / is known by.
+ `duns` (required, string, nullable)
    Recipient's DUNS (Data Universal Numbering System) number. `null` when no DUNS is provided.
+ `uei` (required, string, nullable)
    Recipient's UEI (Unique Entity Identifier). `null` when no UEI is provided.
+ `recipient_id` (required, string)
    A unique identifier for the recipient.
+ `parents` (required, array[ParentRecipient], fixed-type)
+ `parent_name` (required, string, nullable)
    Parent recipient's name. `null` if the recipient does not have a parent recipient.
+ `parent_duns` (required, string, nullable)
    Parent recipient's DUNS number. `null` if the recipient does not have a parent recipient, or the parent recipient's DUNS is not provided.
+ `parent_id` (required, string, nullable)
    A unique identifier for the parent recipient. `null` if the recipient does not have a parent recipient.
+ `parent_uei` (required, string, nullable)
    Parent recipient's UEI (Unique Entity Identifier). `null` when no UEI is provided.
+ `location` (required, RecipientLocation, fixed-type)
+ `business_types` (required, array[string], fixed-type)
    An array of business type field names used to categorize recipients.
+ `total_transaction_amount` (required, number)
    The aggregate monetary value of all transactions associated with this recipient for the given time period.
+ `total_transactions` (required, number)
    The number of transactions associated with this recipient for the given time period.
+ `total_face_value_loan_amount` (required, number)
    The aggregate face value loan guarantee value of all transactions associated with this recipient for the given time period.
+ `total_face_value_loan_transactions` (required, number)
    The number of transactions associated with this recipient for the given time period and face value loan guarantee.
+ `recipient_level` (required, enum[string])
    A letter representing the recipient level. `R` for neither parent nor child, `P` for Parent Recipient, or `C` for child recipient.
    + Members
        + `R`
        + `P`
        + `C`

## RecipientLocation (object)
+ `address_line1` (required, string, nullable)
    The first line of the recipient's street address.
+ `address_line2` (required, string, nullable)
    Second line of the recipient's street address.
+ `address_line3` (required, string, nullable)
    Third line of the recipient's street address.
+ `foreign_province` (required, string, nullable)
    Name of the province in which the recipient is located, if it is outside the United States.
+ `city_name` (required, string, nullable)
    Name of the city in which the recipient is located.
+ `county_name` (required, string, nullable)
    Name of the county in which the recipient is located.
+ `state_code` (required, string, nullable)
    Code for the state in which the recipient is located.
+ `zip` (required, string, nullable)
    Recipient's zip code (5 digits)
+ `zip4` (required, string, nullable)
    Recipient's zip code (4 digits)
+ `foreign_postal_code` (required, string, nullable)
    Recipient's postal code, if it is outside the United States.
+ `country_name` (required, string, nullable)
     Name of the country in which the recipient is located.
+ `country_code` (required, string, nullable)
     Code for the country in which the recipient is located.
+ `congressional_code` (required, string, nullable)
    Number for the recipient's congressional district.

## ParentRecipient (object)
+ `parent_name` (required, string, nullable)
+ `parent_duns` (required, string, nullable)
    DUNS number
+ `parent_id` (required, string, nullable)
    A unique identifier for the parent recipient.
+ `parent_uei` (required, string, nullable)
    Parent recipient's UEI (Unique Entity Identifier). `null` when no UEI is provided.

