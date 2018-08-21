## Transactions
**Route:** `/api/v2/transactions/`

**Method** `POST`

This route sends a request to the backend to retrieve transactions related to a specific parent 
award.

## Request Parameters

- award_id: (required) id of parent award to retrieve related transactions
- sort : (optional) parameter to determine order of sorted results.

## Response (JSON)

```
{
    "page_metadata": {
        "page": 1,
        "next": null,
        "previous": null,
        "hasNext": false,
        "hasPrevious": false
    },
    "results": [
        {
            "id": "CONT_TX_9700_9700_0001_82_DACA8797D0066_0",
            "type": "C",
            "type_description": "DELIVERY ORDER",
            "action_date": "2017-03-16",
            "action_type": "C",
            "action_type_description": "FUNDING ONLY ACTION",
            "modification_number": "82",
            "description": "T4C IGF:;OT::IGF",
            "federal_action_obligation": -97620,
            "face_value_loan_guarantee": null,
            "original_loan_subsidy_cost": null
        }
    ]
}
```


### Errors
Possible HTTP Status Codes:

* 400 : Bad Request

```
{
  "detail": "Sample error message"
}
```

* 422 : Unprocessable Entity

```
{
  "detail": "Sample error message"
}
```

* 500: All other errors