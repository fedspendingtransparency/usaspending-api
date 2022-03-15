FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Children [/api/v2/recipient/children/{duns_or_uei}/{?year}]

This endpoint is used to power USAspending.gov's recipient profile pages. This data can be used to visualize the government spending that pertains to a specific recipient.

## GET

This endpoint returns a list of child recipients belonging to the given parent recipient DUNS or UEI.

+ Parameters

    + `duns_or_uei`: `001006360` (required, string)
        Parent recipient's DUNS or UEI.
    + `year`: `2017` (optional, string)
        The fiscal year you would like data for. Use `all` to view all time or `latest` to view the latest 12 months.

+ Response 200 (application/json)

    + Attributes (array[ChildRecipient], fixed-type)

# Data Structures

## ChildRecipient (object)
+ `name`: `Child of ABC Corporation` (required, string, nullable)
    Name of the child recipient. `null` if the child recipient's name is not provided.
+ `duns`: `001006360` (required, string, nullable)
    Child recipient's DUNS. `null` if the child recipient's DUNS is not provided.
+ `uei` (required, string, nullable)
    Recipient's UEI (Unique Entity Identifier). `null` when no UEI is provided.
+ `recipient_id`: `0036a0cb-0d88-2db3-59e0-0f9af8ffef57-C` (required, string)
    A unique identifier for the child recipient.
+ `state_province`: New Jersey (required, string, nullable)
    The state or province in which the child recipient is located.
+ `amount`: 300200000 (required, number)
    The aggregate monetary value of transactions associated with this child recipient for the selected time period.
