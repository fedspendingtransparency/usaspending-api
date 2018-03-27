# Search Filters v2 Documentation

**NOTE:** Filter examples can be combined into a single object in order to AND their results. Some search filters can be OR'd internally and will be specified as such. For example:

```
{
	"keyword": "example search text",
	"time_period": [
		{
			"start_date": "2001-01-01",
			"end_date": "2001-01-31"
		}
	]
}
```

---
Available filters are in the order they appear on the Search & Download Page.

| Available Filters |
| ----------------- |
| [Standard Location Object - Applies to multiple filters](#standard-location-object) |
| [Keyword Search](#keyword-search) |
| [Time Period](#time-period) |
| [Place of Performance Scope](#place-of-performance-scope) |
| [Place of Performance Location](#place-of-performance-location) |
| [Agencies](#agencies-awardingfunding-agency) |
| [Recipient Name/DUNS](#recipient-nameduns) |
| [Recipient Scope](#recipient-scope) |
| [Recipient Location](#recipient-location) |
| [Recipient/Business Type](#recipientbusiness-type) |
| [Award Type](#award-type) |
| [Award ID](#award-id) |
| [Award Amount](#award-amount) |
| [CFDA](#cfda) |
| [NAICS](#naics) |
| [PSC](#psc) |
| [Type of Contract Pricing](#type-of-contract-pricing) |
| [Set-Aside Type](#set-aside-type) |
| [Extent Completed](#extent-completed) |

---
## Standard Location Object


A country:
```
{
    "country": "DEU"
}
```

A US state:
```
{
    "country": "USA",
    "state": "VA"
}
```

A US county:
```
{
    "country": "USA",
    "state": "VA",
    "county": "059"
}
```

A US congressional district:
```
{
    "country": "USA",
    "state": "VA",
    "district": "11"
}
```


Keys in a location object include:
* **country (required)** - a 3 character code indicating the country to search within.
    * If the country code is *not* `USA`, all further parameters can be ignored.
    * A special country code - `FOREIGN` - represents all non-US countries.
* **state** - a 2 character string abbreviation for the state or territory
* **county** - a 3 digit FIPS code indicating the county
    * If `county` is provided, a `state` must always be provided as well.
    * If `county` is provided, a `district` value *must never* be provided.
* **district** - a 2 character code indicating the congressional district
    * If `district` is provided, a `state` must always be provided as well.
    * If `district` is provided, a `county` *must never* be provided.


## Keyword Search

**Description:** Search is based on a single string input.

**TODO:**
1. Determine what backend fields are being searched against.

**Example Request:**
```
{
	"keyword": "example search text"
}
```

Request parameter description:
* `keyword` (String) : String containing the search text. Also the top level key name for the filter.

## Time Period

**Description:** Search based on one or more fiscal year selections OR date range. Dates should be in the following format: YYYY-MM-DD

**Example Request for Fiscal Year (Fiscal Year is converted to a date range by frontend):**
```
{
	"time_period": [
		{
			"start_date": "2016-10-01",
			"end_date": "2017-09-30"
		}
	]
}
```

**Example Request for Date Range:**
```
{
	"time_period": [
		{
			"start_date": "2001-01-01",
			"end_date": "2001-01-31"
		}
	]
}
```

Request parameter description:
* `time_period` (List) : Top level key name for this filter. Contains a list of date ranges to filter on.
* `start_date` (String) : Start date value for date range filtering.
* `end_date` (String) : End date value for date range filtering.

## Place of Performance Scope

**Description:** Filtering based on radio button selection OR autocomplete selection. Different filtering scenarios:
1. If no autocomplete values are selected, then “Show only” affects results.
2. If All and no location specified, filter not used (won’t be sent to backend).
3. Otherwise, only selected autocomplete values dictate results.

**Example Request for "Show Only" Selection:**
```
{
	"place_of_performance_scope": "domestic"
}
```

Request parameter description:
* `place_of_performance_scope` (String) : Top level key name for filter. Holds the type of location data to display. Can be one of the following: domestic, foreign


## Place of Performance Location

**Description:** Filtering is based on Standard Location Objects.

**Example Request**
```
{
	"place_of_performance_locations": [
        {
            "country": "USA",
            "state": "VA",
            "county": "059"
        }
    ]
}
```

Request parameter description:
* `place_of_performance_locations` (array of Standard Location Objects): Top level key name for filter. Holds an array of Standard Location Objects.

## Agencies (Awarding/Funding Agency)

**Description:** Filters for searching based on the awarding agency or the toptier agency.

**NOTE:** The values provided here will be OR'd against each other, but only within the scope of this particular filter. This filter set, as a whole, will still be AND'd with the other filters.

**Example Request:**
```
{
	"agencies": [
    	{
        	"type": "funding",
            	"tier": "toptier",
            	"name": "Office of Pizza"
        },
	{
        	"type": "awarding",
            	"tier": "subtier",
            	"name": "Personal Pizza"
        }
    ]
}
```

Request parameter description:
* `agencies` (List) : Top level key name for filter. Contains a list of JSON Objects.
* `type` (String) : Type of agency. Can be one of the following: funding, awarding
* `tier` (String) : Tier of agency. Can be one of the following: toptier, subtier
* `name` (String) : Name of agency.

## Recipient Name/DUNS

**Description:** Filtering based on the autocomplete selections for Recipient Name/DUNS.

**Example Request:**
```
{
	"recipient_search_text": ["D12345678"]
}
```

Request parameter description:
* `recipient_search_text` (List) : Top level key name for filter. Contains list of search inputs the user has provided to be used to filter results in all visualizations. (**must** not exceed a length of 1 item)

## Recipient Scope

**Description:** Filtering based on radio button selection OR autocomplete selection. Different filtering scenarios:
1. If no autocomplete values are selected, then “Show only” affects results.
2. If All and no location specified, filter not used (won’t be sent to backend).
3. Otherwise, only selected autocomplete values dictate results.

**Example Request for "Show Only" Selection:**
```
{
	"recipient_scope": "domestic"
}
```

Request parameter description:
* `recipient_scope` (String) : Top level key name for filter. Type of location data to display. Can be one of the following: domestic, foreign

## Recipient Location

**Description:** Filtering is based on Standard Location Objects.

**Example Request**
```
{
	"recipient_locations": [
        {
            "country": "USA",
            "state": "VA",
            "county": "059"
        }
    ]
}
```

Request parameter description:
* `recipient_locations` (array of Standard Location Objects): Top level key name for filter. Holds an array of Standard Location Objects.

## Recipient/Business Type

**Description:** Filtering based on one or more checkbox selections for recipient/business type.

**NOTE:** The values provided here will be OR'd against each other, but only within the scope of this particular filter. This filter set, as a whole, will still be AND'd with the other filters.

**Example Request:**
```
{
	"recipient_type_names": [
		"Small Business",
		"Alaskan Native Owned Business"
    	]
}
```

Request Parameter Description:
* `recipient_type_names` (List) : Top level key name for filter. Contains list of strings corresponding to drop down selections. Top or lower level selections are passed here. See [Recipient/Business Types](https://github.com/fedspendingtransparency/usaspending-api/wiki/Recipient-Business-Types).

## Award Type

**Description:** Filtering based on one or more checkbox selections for award type.

**NOTE:** The values provided here will be OR'd against each other, but only within the scope of this particular filter. This filter set, as a whole, will still be AND'd with the other filters.

**Example Request:**
```
{
	"award_type_codes": ["A", "B", "03"]
}
```

Request Parameter Description:
* `award_type_codes` (List) : Top level key name for filter. Contains list of strings corresponding to drop down selections. Lowest level selections are passed here. For example, if "Contracts" is selected at the top level, then the list would contains all selections within "Contracts": ["A", "B", "C", "D"].

## Award ID

**Description:** Filtering based on autocomplete selections for award id.

**Example Request:**
```
{
	"award_ids": ["1605SS17F00018", "P063P151708", "AID-OFDA-G-14-00121-01"]
}
```

Request parameter description:
* `award_ids` (List) : Top level key name for filter. Contains list of fains/piids/uris corresponding to Awards.

## Award Amount

**Description:** Filtering based on one or more checkbox selections for award amount.

**NOTE:** The values provided here will be OR'd against each other, but only within the scope of this particular filter. This filter set, as a whole, will still be AND'd with the other filters.

**TODO:**
1. Determine if upper and lower bounds are inclusive or exclusive when both are provided.

**Example Request:**
```
{
	"award_amounts": [
	      {
		"lower_bound": 1000000.00,
		"upper_bound": 25000000.00
	      },
	      {
		"upper_bound": 1000000.00
	      },
	      {
		"lower_bound": 500000000.00
	      }
    	]
}
```

Request Parameter Description:
* `award_amounts` (List) : Top level key name for filter. Contains a list of JSON Objects with additional keys, setting the upper and lower bounds for each selection.
* `lower_bound` (Float) : Float corresponding to the lower bound to check within. If not provided, everything <= to the upper bound should be returned.
* `upper_bound` (Float) : Float corresponding to the upper buond to check within. If not provided, everything >= to the lower bound should be returned.

## CFDA

**Description:** Filtering based on autocomplete selections for CFDA Programs.

**Example Request:**
```
{
	"program_numbers": ["10.553"]
}
```

Request parameter description:
* `program_numbers` (List) : Top level key name for filter. Contains list of Strings corresponding to CFDA Program Numbers.

## NAICS

**Description:** Filtering based on autocomplete selections for NAICS Code.

**Example Request:**
```
{
	"naics_codes": ["336411"]
}
```

Request parameter description:
* `naics_codes` (List) : Top level key name for filter. Contains list of Strings corresponding to NAICS Codes.


## PSC

**Description:** Filtering based on autocomplete selections for PSC Code.

**Example Request:**
```
{
	"psc_codes": ["1510"]
}
```

Request parameter description:
* `psc_codes` (List) : Top level key name for filter. Contains list of Strings corresponding to PSC Codes.

## Type of Contract Pricing

**Description:** Filtering based on one or more checkbox selections for type of contract pricing.

**Example Request:**
```
{
	"contract_pricing_type_codes": ["SAMPLECODE123"]
}
```

Request parameter description:
* `contract_pricing_type_codes` (List) : Top level key name for filter. Contains list of Strings corresponding to Type of Contract Pricing codes.

## Set-Aside Type

**Description:** Filtering based on one or more checkbox selections for set-aside type.

**Example Request:**
```
{
	"set_aside_type_codes": ["SAMPLECODE123"]
}
```

Request parameter description:
* `set_aside_type_codes` (List) : Top level key name for filter. Contains list of Strings corresponding to Set-Aside Type codes.

## Extent Competed

**Description:** Filtering based on one or more checkbox selections for extent competed.

**Example Request:**
```
{
	"extent_competed_type_codes": ["SAMPLECODE123"]
}
```

Request parameter description:
* `extent_competed_type_codes` (List) : Top level key name for filter. Contains list of Strings corresponding to Extent Competed type codes.