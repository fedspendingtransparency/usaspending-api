## [Federal Accounts Object Class](#usaspending-api-documentation)
**Route**:```/api/v2/federal_accounts/:federal_account_id/available_object_classes```

**Method**: ```GET```

This route sends a request to the backend to retrieve minor object classes rolled up under major classes filtered by federal account.

### Required Parameters
* ```federal_account_id```: **required** - an integer representing the federal account id to filter on

### Response (JSON)
HTTP Status Code: 200

```
{
    "results": [
        {
            "id": "20",
            "name": "Contractual services and supplies",
            "minor_object_class": [
                {
                    "id": "220",
                    "name": "Transportation of things"
                },
            ...]
        },
    ...]
}
```

### Response Attributes Description
* ```id```: Major object class id (Major object class code) Reference for filtering
* ```name```: Major object class name/label
* ```minor_object_class:id```: Minor object class id (Minor object class code). Reference for filtering
* ```minor_object_class:name```: Minor object class name/label

### Errors
Possible HTTP Status Codes:
* 400: Missing Parameter
* 500: All other errors
