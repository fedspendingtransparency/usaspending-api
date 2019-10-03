# State Data

**Route:** /api/v2/award_spending/recipient/

**Method:** GET

This route returns basic information about the specified state.

`:awarding_agency_id` fips code representing the state
`:fiscal_year` year to filter on 
`:limit` page length of results (*optional*)
`:page` page to return (*optional*)

## Response Example

```
{
    "page_metadata": {
        "count": 36608,
        "page": 1,
        "has_next_page": true,
        "has_previous_page": false,
        "next": "https://api.usaspending.gov/api/v2/award_spending/recipient/?awarding_agency_id=183&fiscal_year=2016&limit=10&page=2",
        "current": "https://api.usaspending.gov/api/v2/award_spending/recipient/?awarding_agency_id=183&fiscal_year=2016&limit=10&page=1",
        "previous": null
    },
    "results": [
        {
            "award_category": "contract",
            "obligated_amount": "82250000.00",
            "recipient": {
                "recipient_id": 14078692,
                "recipient_name": "T-REX CONSULTING CORPORATION"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "54735335.00",
            "recipient": {
                "recipient_id": 45997393,
                "recipient_name": "HENSEL PHELPS CONSTRUCTION CO"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "50424860.00",
            "recipient": {
                "recipient_id": 40209051,
                "recipient_name": "HARRIS CORPORATION"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "28348968.93",
            "recipient": {
                "recipient_id": 49361840,
                "recipient_name": "VION CORPORATION"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "27236680.00",
            "recipient": {
                "recipient_id": 38117168,
                "recipient_name": "GREAT LAKES DREDGE & DOCK COMPANY, LLC"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "26318528.00",
            "recipient": {
                "recipient_id": 111838220,
                "recipient_name": "REED TECHNOLOGY AND INFORMATION SERVICES INC."
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "25000000.00",
            "recipient": {
                "recipient_id": 22929108,
                "recipient_name": "HARRIS CORPORATION"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "24938014.00",
            "recipient": {
                "recipient_id": 32209507,
                "recipient_name": "INTERNATIONAL BUSINESS MACHINES CORPORATION"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "24000000.00",
            "recipient": {
                "recipient_id": 11599750,
                "recipient_name": "HARRIS CORPORATION"
            }
        },
        {
            "award_category": "contract",
            "obligated_amount": "24000000.00",
            "recipient": {
                "recipient_id": 14982761,
                "recipient_name": "HARRIS CORPORATION"
            }
        }
    ]
}
```
