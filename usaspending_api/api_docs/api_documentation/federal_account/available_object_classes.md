## Available Object Classes
**Route:** `v2/federal_accounts/[federal_account_id]/available_object_classes`

**Method:** `GET`

This route returns object classes that the specified federal account has allotted money to.

**Query Parameters Description:**

* `federal_account_id` - **REQUIRED** - Database id for a federal account.

### Response (JSON) 

```
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
        },....
    ]
}

```

### Errors
Possible HTTP Status Codes:
* 500 : All other errors
      ```
      {
        "detail": "Sample error message"
      }
      ```
