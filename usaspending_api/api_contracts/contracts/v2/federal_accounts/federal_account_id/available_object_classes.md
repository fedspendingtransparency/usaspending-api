FORMAT: 1A
HOST: https://api.usaspending.gov

# Available Object Classes [v2/federal_accounts/{federal_account_id}/available_object_classes]

## GET

This route returns object classes that the specified federal account has allotted money to.

+ Parameters
    + `federal_account_id`: 5 (required, number)
        Database id for a federal account.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[MajorObjectClass], fixed-type)
    + Body


        {
            "results": [
                {
                    "id": "10",
                    "name": "Personnel compensation and benefits",
                    "minor_object_class": [
                        {
                            "id": "113",
                            "name": "Other than full-time permanent"
                        },
                        {
                            "id": "115",
                            "name": "Other personnel compensation"
                        },
                        {
                            "id": "111",
                            "name": "Full-time permanent"
                        },
                        {
                            "id": "130",
                            "name": "Benefits for former personnel"
                        },
                        {
                            "id": "121",
                            "name": "Civilian personnel benefits"
                        }
                    ]
                },
                {
                    "id": "40",
                    "name": "Grants and fixed charges",
                    "minor_object_class": [
                        {
                            "id": "420",
                            "name": "Insurance claims and indemnities"
                        },
                        {
                            "id": "410",
                            "name": "Grants, subsidies, and contributions"
                        }
                    ]
                }
            ]
        }

# Data Structures

## MajorObjectClass (object)
+ `id` (required, number)
+ `name` (required, string)
+ `minor_object_class` (required, array[MinorObjectClass], fixed-type)

## MinorObjectClass (object)
+ `id` (required, number)
+ `name` (required, string)
