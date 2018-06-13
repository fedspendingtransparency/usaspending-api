## Subawards
**Route:** `/api/v2/subawards`

**Method:** `POST`

This route sends a request to the backend to retrieve subawards either related, optionally, to a specific parent 
award, or for all parent awards if desired.

### Response (JSON)

```{
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    },
    "results": [
        {
            "id": 1364,
            "subaward_number": "WO#1-EEA",
            "description": "General consulting technical services work order",
            "action_date": "2013-10-29",
            "amount": 37588,
            "recipient_name": "CHEMONICS INTERNATIONAL, INC"
        },
        {
            "id": 4329,
            "subaward_number": "UWSC8202",
            "description": "Collaborative Research: Global Ocean Repeat Hydropraphy, Carbon, and Tracer Measurements, 2015-2020.",
            "action_date": "2015-04-02",
            "amount": 354302,
            "recipient_name": "UNIVERSITY OF WASHINGTON"
        },
        {
            "id": 4470,
            "subaward_number": "UWSC8200",
            "description": "\"Collaborative Research: Global Ocean Repeat Hydrography, Carbon and Tracer Measurements, 2015-2020\".",
            "action_date": "2017-03-15",
            "amount": 125839,
            "recipient_name": "UNIVERSITY OF WASHINGTON"
        },
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
