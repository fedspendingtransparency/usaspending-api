# List Recipients

**Route:** /api/v2/recipient/duns/

**Method:** POST

This route returns a list of all recipients in USAspending DB. A "Recipient" is defined by the unique combination of DUNS + Recipient Name (with minor "normalization" performed on Recipient Name using either SAM data or the data from the latest transaction for each DUNS)
### Request

keyword (**OPTIONAL**): String value. Parameter will search both DUNS and Recipient Name with value

award_type (**OPTIONAL**): String value. Parameter indicating which award type to filter results and displays totals of only this type. Below are the following values available. Default is `all`
* `all`
* `contracts`
* `direct_payments`
* `grants`
* `loans`
* `other_financial_assistance`

sort (**OPTIONAL**): String value. Parameter indicating which column to sort results. Below are the following values available. Default is `amount`
* `name`
* `duns`
* `amount`

order (**OPTIONAL**): String value. Specifies result ordering. Default is `desc`
* `asc`
* `desc`

limit (**OPTIONAL**): Integer value. How many results are returned. If no limit is specified, the limit is set to 50.

page (**OPTIONAL**): Integer value. The page number that is currently returned. Default is 1.


## Response Example

```
{
    "page_metadata": {
        "page": 1,
        "total": 960110,
        "limit": 99,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    },
    "results": [
        {
            "id": "0b2839c0-b343-8703-a845-9d78e2c2ad3b-P",
            "duns": "004027553",
            "name": "ALABAMA, STATE OF",
            "recipient_level": "P",
            "amount": -157454658.83
        },
        {
            "id": "f52ced68-1458-6a50-991b-7575e6a9aa87-P",
            "duns": "195194779",
            "name": "NATIONAL SECURITY TECHNOLOGIES, LLC",
            "recipient_level": "P",
            "amount": -141437775.3
        },
        {
            "id": "a5f218a4-7b8b-1a1d-cb0a-ae0a78ee77b8-C",
            "duns": "079098386",
            "name": "TRIBUTE CONTRACTING, LLC",
            "recipient_level": "C",
            "amount": -85982000
        },
        {
            "id": "f1ee7802-31e6-487c-90c2-6e16ef6e0674-P",
            "duns": "167266134",
            "name": "SENTINEL FIELD SERVICES, INC.",
            "recipient_level": "P",
            "amount": -69259642.44
        }
    ]
}
```

* `name`: The recipient name
* `duns`: The unique identifier given to the recipient and provided to USASpending in the transaction
* `recipient_level`: Designates if that record is of a parent, child, or generic(Recipient) type.
    * Parent: Another recipient lists this recipient's DUNS as it's parent in the transaction
    * Child: Includes a parent DUNS (can reference itself)
    * Recipient: Recipients which are neither a parent nor a child type
* `id`: is an interally-produced ID which is a combination of Recipient Name, DUNS, and the "Recipient Level"
* `amount`: The overall obligation for that recipient (in the appropriate type) over the past 12 months.
    * It is possible a group of transactions will be recorded for all recipient levels for a single recipient