## [Recipient Autocomplete](#usaspending-api-documentation)
**Route:** `/api/v2/autocomplete/recipient`

**Method:** `POST`

This route sends a request to the backend to retrieve Parent and Recipient DUNS matching the search text in order of similarity.

### Request Example
```
{
    "search_text": "booz",
    "limit": 10
}
```
### Query Parameters Description

**search_text** - `required` - a string that contains the search term(s) or DUNS number on which to search.

**limit** - `optional` - an integer representing the number of desired entries per budget title type. It can be an integer or a string representation of an integer. Defaults to 10.

### Request example
**`Searching on a string of text`**
```
{
    "search_text": "booz"
}
```
### Response (JSON)

HTTP Status Code: 200
```
{
    "results": {
        "parent_recipient": [
            {
                "legal_entity_id": 12325,
                "recipient_name": "BOONE, TOWN OF",
                "parent_recipient_unique_id": "021395942"
            },
            {
                "legal_entity_id": 10208,
                "recipient_name": "BOONE, COUNTY OF",
                "parent_recipient_unique_id": "021688650"
            },
            {
                "legal_entity_id": 8051,
                "recipient_name": "BOONE, COUNTY OF",
                "parent_recipient_unique_id": "077392728"
            },
            {
                "legal_entity_id": 365364,
                "recipient_name": "HENRY BOOTH HOUSE",
                "parent_recipient_unique_id": "113654735"
            },
            {
                "legal_entity_id": 7855,
                "recipient_name": "BONITZ, INC.",
                "parent_recipient_unique_id": "157497124"
            },
            {
                "legal_entity_id": 106024,
                "recipient_name": "BOOZ ALLEN HAMILTON HOLDING CORPORATION",
                "parent_recipient_unique_id": "964725688"
            },
            {
                "legal_entity_id": 12061,
                "recipient_name": "BOWIE, CITY OF",
                "parent_recipient_unique_id": "077792802"
            },
            {
                "legal_entity_id": 11870,
                "recipient_name": "BOVINA, CITY OF",
                "parent_recipient_unique_id": "044480080"
            },
            {
                "legal_entity_id": 9886,
                "recipient_name": "TOWN OF BOURNE",
                "parent_recipient_unique_id": "076614395"
            },
            {
                "legal_entity_id": 11611,
                "recipient_name": "BONHAM, CITY OF",
                "parent_recipient_unique_id": "078389319"
            }
        ],
        "recipient": [
            {
                "legal_entity_id": 91215,
                "recipient_name": "BOOM, ROBERT",
                "recipient_unique_id": "809391944"
            },
            {
                "legal_entity_id": 359728,
                "recipient_name": "BOOM!HEALTH",
                "recipient_unique_id": "932807902"
            },
            {
                "legal_entity_id": 504712,
                "recipient_name": "BOOKPAL, LLC",
                "recipient_unique_id": "608477035"
            },
            {
                "legal_entity_id": 875,
                "recipient_name": "BOOZ ALLEN HAMILTON INC.",
                "recipient_unique_id": "006928857"
            },
            {
                "legal_entity_id": 64703,
                "recipient_name": "BOOZ ALLEN HAMILTON INC.",
                "recipient_unique_id": "077368509"
            },
            {
                "legal_entity_id": 91060,
                "recipient_name": "BOOZ ALLEN HAMILTON INC.",
                "recipient_unique_id": "831527965"
            },
            {
                "legal_entity_id": 303117,
                "recipient_name": "BOOZ ALLEN HAMILTON INC.",
                "recipient_unique_id": "085631448"
            },
            {
                "legal_entity_id": 367781,
                "recipient_name": "BOOZ ALLEN HAMILTON INC.",
                "recipient_unique_id": "004679015"
            },
            {
                "legal_entity_id": 344579,
                "recipient_name": "Ox Bow",
                "recipient_unique_id": "620799648"
            },
            {
                "legal_entity_id": 346002,
                "recipient_name": "BOON EDAM INC.",
                "recipient_unique_id": "017625625"
            }
        ]
    }
}
```
### Request example
**`Searching on a specific DUNS number`**
```
{
  "search_text": "004679015"
}
```
### Response (JSON)

HTTP Status Code: 200
```
{
    "results": {
        "parent_recipient": [
            {
                "legal_entity_id": 503052,
                "recipient_name": "AUGUSTA SCHOOL DISTRICT 10",
                "parent_recipient_unique_id": "004939013"
            },
            {
                "legal_entity_id": 503330,
                "recipient_name": "SWEETWATER, CITY OF",
                "parent_recipient_unique_id": "079015137"
            },
            {
                "legal_entity_id": 8582,
                "recipient_name": "CITY OF GLOBE",
                "parent_recipient_unique_id": "070259015"
            },
            {
                "legal_entity_id": 14661,
                "recipient_name": "CENTRAL FLORIDA COMMUNITY ACTION AGENCY, INC.",
                "parent_recipient_unique_id": "046753901"
            },
            {
                "legal_entity_id": 502875,
                "recipient_name": "MONTICELLO SCHOOL DISTRICT 18",
                "parent_recipient_unique_id": "004932679"
            },
            {
                "legal_entity_id": 499304,
                "recipient_name": "BETHLEHEM CENTRAL SCHOOL DISTRICT",
                "parent_recipient_unique_id": "080467046"
            },
            {
                "legal_entity_id": 501128,
                "recipient_name": "LANCASTER COUNTY SCHOOL DISTRICT",
                "parent_recipient_unique_id": "084713015"
            },
            {
                "legal_entity_id": 502930,
                "recipient_name": "HOXIE SCHOOL DISTRICT 46",
                "parent_recipient_unique_id": "004935367"
            },
            {
                "legal_entity_id": 7889,
                "recipient_name": "PRAIRIE ENERGY COOPERATIVE",
                "parent_recipient_unique_id": "004795621"
            },
            {
                "legal_entity_id": 503026,
                "recipient_name": "MOUNTAIN VIEW SCHOOL DISTRICT 30",
                "parent_recipient_unique_id": "004937850"
            }
        ],
        "recipient": [
            {
                "legal_entity_id": 367781,
                "recipient_name": "BOOZ ALLEN HAMILTON INC.",
                "recipient_unique_id": "004679015"
            }
        ]
    }
}
```
### Request example
**`Searching on a specific Parent DUNS number`**
```
{
  "search_text": "964725688"
}
```
### Response (JSON)

HTTP Status Code: 200
```
{
    "results": {
        "parent_recipient": [
            {
                "legal_entity_id": 106024,
                "recipient_name": "BOOZ ALLEN HAMILTON HOLDING CORPORATION",
                "parent_recipient_unique_id": "964725688"
            }
        ],
        "recipient": [
            {
                "legal_entity_id": 59980,
                "recipient_name": "CASA de Maryland, Inc.",
                "recipient_unique_id": "968872564"
            },
            {
                "legal_entity_id": 345245,
                "recipient_name": "Tapology Inc.",
                "recipient_unique_id": "964726744"
            },
            {
                "legal_entity_id": 348001,
                "recipient_name": "LSS HOUSING WILLOW WOOD INC",
                "recipient_unique_id": "964747278"
            },
            {
                "legal_entity_id": 349167,
                "recipient_name": "LAWRENCE AREA NON-PROFIT HOUSING CORPORATION",
                "recipient_unique_id": "964757988"
            },
            {
                "legal_entity_id": 506307,
                "recipient_name": "GRANO REFORESTATION, INC.",
                "recipient_unique_id": "969256882"
            },
            {
                "legal_entity_id": 5906,
                "recipient_name": "ADINO, INC.",
                "recipient_unique_id": "964776228"
            },
            {
                "legal_entity_id": 7010,
                "recipient_name": "NEW VISION ENTERPRISE INCORPORATED",
                "recipient_unique_id": "964751143"
            },
            {
                "legal_entity_id": 8141,
                "recipient_name": "Grand River Township",
                "recipient_unique_id": "964754712"
            },
            {
                "legal_entity_id": 12775,
                "recipient_name": "CHHE",
                "recipient_unique_id": "968872580"
            },
            {
                "legal_entity_id": 63811,
                "recipient_name": "BIT DIRECT INC",
                "recipient_unique_id": "968839642"
            }
        ]
    }
}
```
### Errors
Possible HTTP Status Codes:

- 400 : Missing parameters

- 500 : All other errors

### Response (JSON)
**Sample Error**
```
{
    "details": "Missing one or more required request parameters: search_text"
}
```
