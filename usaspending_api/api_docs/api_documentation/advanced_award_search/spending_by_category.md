## [Spending by Category](#spending-by-category) Depreciated
**Route:** `/api/v2/search/spending_by_category/`

**Method:** `POST`

This route takes award filters, and returns spending by the defined category/scope.  The category is defined by the `category` keyword, and the scope is defined by is denoted by the `scope` keyword.

### Requests Overview
`category`: Major pivot for gathering the total obligation dollars, captured by `aggregated_amount`.

All Category Options:
* `awarding_agency`
* `funding_agency`
* `recipient`
* `cfda_programs`
* `industry_codes`


`scope`: Scope adds another level of aggregation to category.  The possible scopes are dependent on the category keyword.

`filter`: how the awards are filtered.  The filter object is defined here: [Filter Object](https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation)

`limit` (**OPTIONAL**):  how many results are returned. Defaults to `10`

`page` (**OPTIONAL**):  what page of results are returned. Defaults to `1`

### Awarding Agency
category: `awarding_agency`
scope: [`agency`, `subagency`, `office` (TBD)]

#### Example Request: "agency"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "awarding_agency",
    "scope": "agency",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "limit": 5,
    "page": 1
}
```

#### Example Response: "agency"
```
{
    "category": "awarding_agency",
    "scope": "agency",
    "limit": 5,
    "results": [
        {
            "agency_name": "Department of Health and Human Services",
            "agency_abbreviation": "HHS",
            "aggregated_amount": 74410050836.86
        },
        {
            "agency_name": "Department of Energy",
            "agency_abbreviation": "DOE",
            "aggregated_amount": 3808583107.02
        },
        {
            "agency_name": "Department of Veterans Affairs",
            "agency_abbreviation": "VA",
            "aggregated_amount": 2626471945.91
        },
        {
            "agency_name": "National Aeronautics and Space Administration",
            "agency_abbreviation": "NASA",
            "aggregated_amount": 1634947240
        },
        {
            "agency_name": "Department of Homeland Security",
            "agency_abbreviation": "DHS",
            "aggregated_amount": 1388392891.24
        }
    ],
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    }
}
```
#### Example Request: "subagency"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "awarding_agency",
    "scope": "subagency",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "limit": 5,
    "page": 1
}
```

#### Example Response: "subagency"
```
{
    "category": "awarding_agency",
    "scope": "subagency",
    "limit": 5,
    "results": [
        {
            "agency_name": "Centers for Medicare and Medicaid Services",
            "agency_abbreviation": "CMS",
            "aggregated_amount": 68301207828.29
        },
        {
            "agency_name": "Administration for Children and Families",
            "agency_abbreviation": "ACF",
            "aggregated_amount": 4597262702
        },
        {
            "agency_name": "Department of Energy",
            "agency_abbreviation": "DOE",
            "aggregated_amount": 3808234805.64
        },
        {
            "agency_name": "Department of Veterans Affairs",
            "agency_abbreviation": "VA",
            "aggregated_amount": 2626471945.91
        },
        {
            "agency_name": "National Aeronautics and Space Administration",
            "agency_abbreviation": "NASA",
            "aggregated_amount": 1634947240
        }
    ],
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    }
}
```
### Funding Agency
category: `awarding_agency`
scope: [`agency`, `subagency`, `office` (TBD)]

#### Example Request: "agency"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "funding_agency",
    "scope": "agency",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "limit": 5,
    "page": 1
}
```

#### Example Response: "agency"
```
{
    "category": "funding_agency",
    "scope": "agency",
    "limit": 5,
    "results": [
        {
            "agency_name": "Department of Energy",
            "agency_abbreviation": "DOE",
            "aggregated_amount": 3787132039.67
        },
        {
            "agency_name": "Department of Veterans Affairs",
            "agency_abbreviation": "VA",
            "aggregated_amount": 2629536687.35
        },
        {
            "agency_name": "National Aeronautics and Space Administration",
            "agency_abbreviation": "NASA",
            "aggregated_amount": 1637479316.15
        },
        {
            "agency_name": "Department of Homeland Security",
            "agency_abbreviation": "DHS",
            "aggregated_amount": 1398128620.74
        },
        {
            "agency_name": "Department of Health and Human Services",
            "agency_abbreviation": "HHS",
            "aggregated_amount": 1262258799.83
        }
    ],
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    }
}
```

#### Example Request: "subagency"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "funding_agency",
    "scope": "subagency",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "limit": 5,
    "page": 1
}
```

#### Example Response: "subagency"
```
{
    "category": "funding_agency",
    "scope": "subagency",
    "limit": 5,
    "results": [
        {
            "agency_name": "Department of Energy",
            "agency_abbreviation": "DOE",
            "aggregated_amount": 3786783738.29
        },
        {
            "agency_name": "Department of Veterans Affairs",
            "agency_abbreviation": "VA",
            "aggregated_amount": 2629536687.35
        },
        {
            "agency_name": "National Aeronautics and Space Administration",
            "agency_abbreviation": "NASA",
            "aggregated_amount": 1637479316.15
        },
        {
            "agency_name": "Centers for Disease Control and Prevention",
            "agency_abbreviation": "CDC",
            "aggregated_amount": 1089104979.51
        },
        {
            "agency_name": "Federal Emergency Management Agency",
            "agency_abbreviation": "FEMA",
            "aggregated_amount": 716689871.09
        }
    ],
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    }
}
```

### Recipient
category: `awarding_agency`
scope: [`duns`, `parent_duns`, `recipient_type` (TBD)]

#### Example Request: "duns"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "recipient",
    "scope": "duns",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "limit": 5,
    "page": 2
}
```

#### Example Response: "duns"
```
{
    "category": "recipient",
    "scope": "duns",
    "limit": 5,
    "results": [
        {
            "legal_entity_id": 169645,
            "aggregated_amount": 2400453500,
            "recipient_name": "Arizona Health Care Cost"
        },
        {
            "legal_entity_id": 169543,
            "aggregated_amount": 1185747000,
            "recipient_name": "Connecticut"
        },
        {
            "legal_entity_id": 172296,
            "aggregated_amount": 742602000,
            "recipient_name": "Oklahoma Health Care Authority"
        },
        {
            "legal_entity_id": 169631,
            "aggregated_amount": 728145000,
            "recipient_name": "Nevada"
        },
        {
            "legal_entity_id": 170551,
            "aggregated_amount": 465507000,
            "recipient_name": "Maine"
        }
    ],
    "page_metadata": {
        "page": 2,
        "next": 3,
        "previous": 1,
        "hasNext": true,
        "hasPrevious": true
    }
}
```

#### Example Request: "parent_duns"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "recipient",
    "scope": "parent_duns",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "limit": 5,
    "page": 2
}
```

#### Example Response: "parent_duns"
```
{
    "category": "recipient",
    "scope": "parent_duns",
    "limit": 5,
    "results": [
        {
            "aggregated_amount": 268820600.56,
            "recipient_name": "LAWRENCE LIVERMORE NATIONAL SECURITY LIMITED LIABILITY COMPANY",
            "parent_recipient_unique_id": "785627931"
        },
        {
            "aggregated_amount": 266898722.37,
            "recipient_name": "GLAXOSMITHKLINE LLC",
            "parent_recipient_unique_id": "238980408"
        },
        {
            "aggregated_amount": 232472684.8,
            "recipient_name": "BECHTEL MARINE PROPULSION CORPORATION",
            "parent_recipient_unique_id": "094878980"
        },
        {
            "aggregated_amount": 171467429.68,
            "recipient_name": "SAVANNAH RIVER NUCLEAR SOLUTIONS, LLC",
            "parent_recipient_unique_id": "798861048"
        },
        {
            "aggregated_amount": 140934433,
            "recipient_name": "UT BATTELLE LIMITED LIABILITY COMPANY",
            "parent_recipient_unique_id": "099114287"
        }
    ],
    "page_metadata": {
        "page": 2,
        "next": 3,
        "previous": 1,
        "hasNext": true,
        "hasPrevious": true
    }
}
```

### CFDA
category: `cfda_programs`

#### Example Request
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "limit": 5,
    "page": 5,
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }]
    },
    "category": "cfda_programs"
}
```

#### Example Response
```
{
    "category": "cfda_programs",
    "limit": 5,
    "results": [
        {
            "cfda_program_number": "17.002",
            "aggregated_amount": 13150240.44,
            "program_title": "Labor Force Statistics",
            "popular_name": ""
        },
        {
            "cfda_program_number": "93.560",
            "aggregated_amount": 9000000,
            "program_title": "Family Support Payments to States Assistance Payments",
            "popular_name": null
        },
        {
            "cfda_program_number": "19.510",
            "aggregated_amount": 8816288,
            "program_title": "U.S. Refugee Admissions Program",
            "popular_name": "U.S. Refugee Admissions Program"
        },
        {
            "cfda_program_number": "19.018",
            "aggregated_amount": 7159318,
            "program_title": "Resettlement Support Centers (RSCs) for U.S. Refugee Resettlement",
            "popular_name": "PRM"
        },
        {
            "cfda_program_number": "12.401",
            "aggregated_amount": 6325490,
            "program_title": "National Guard Military Operations and Maintenance (O&M) Projects",
            "popular_name": ""
        }
    ],
    "page_metadata": {
        "page": 5,
        "next": 6,
        "previous": 4,
        "hasNext": true,
        "hasPrevious": true
    }
}
```

### Industry Codes
category: `industry_codes`
scope: [`psc`, `naics`]

#### Example Request: "psc"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "industry_codes",
    "scope": "psc",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }],
        "award_type_codes": ["A", "B", "C", "D"]
    },
    "limit": 5,
    "page": 1
}
```

#### Example Response: "psc"
```
{
    "category": "industry_codes",
    "scope": "psc",
    "limit": 5,
    "results": [
        {
            "psc_code": "M181",
            "aggregated_amount": 1466355584.93
        },
        {
            "psc_code": "6505",
            "aggregated_amount": 1177461817.17
        },
        {
            "psc_code": "M1JZ",
            "aggregated_amount": 883763023.2
        },
        {
            "psc_code": "AR22",
            "aggregated_amount": 527791969.82
        },
        {
            "psc_code": "S206",
            "aggregated_amount": 520770880.43
        }
    ],
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    }
}
```

#### Example Request: "naics"
```
POST <usapending>/api/v2/search/spending_by_category/
BODY
{
    "category": "industry_codes",
    "scope": "naics",
    "filters": {
        "time_period": [{
            "start_date": "2017-10-01",
            "end_date": "2018-09-30"
        }],
        "award_type_codes": ["A", "B", "C", "D"]
    },
    "limit": 5,
    "page": 1
}
```

#### Example Response: "naics"
```
{
    "category": "industry_codes",
    "scope": "naics",
    "limit": 5,
    "results": [
        {
            "naics_code": "561210",
            "aggregated_amount": 2349696344.73,
            "naics_description": "FACILITIES SUPPORT SERVICES"
        },
        {
            "naics_code": "541710",
            "aggregated_amount": 1029315785.97,
            "naics_description": "RESEARCH AND DEVELOPMENT IN THE PHYSICAL, ENGINEERING, AND LIFE SCIENCES"
        },
        {
            "naics_code": "541712",
            "aggregated_amount": 761652715.54,
            "naics_description": "RESEARCH AND DEVELOPMENT IN THE PHYSICAL, ENGINEERING, AND LIFE SCIENCES (EXCEPT BIOTECHNOLOGY)"
        },
        {
            "naics_code": "561612",
            "aggregated_amount": 480090803.04,
            "naics_description": "SECURITY GUARDS AND PATROL SERVICES"
        },
        {
            "naics_code": "541519",
            "aggregated_amount": 447390731.76,
            "naics_description": "OTHER COMPUTER RELATED SERVICES"
        }
    ],
    "page_metadata": {
        "page": 1,
        "next": 2,
        "previous": null,
        "hasNext": true,
        "hasPrevious": false
    }
}
```

### Errors
Possible HTTP Status Codes:
* 400 : Missing parameters or limit is not a valid, positive integer
* 500 : All other errors

```
{
  "detail": "Sample error message"
}
```

### Other Search Filters
[Link](https://github.com/fedspendingtransparency/usaspending-website/wiki/Award-Search-Visualizations)
