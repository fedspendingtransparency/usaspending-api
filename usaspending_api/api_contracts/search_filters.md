# Search Filters v2 Documentation

**NOTE:** Filter examples can be combined into a single object in order to AND their results. Some search filters can be OR'd internally and will be specified as such. For example:

```
{
    "keywords": ["example search text"],
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

All US cities with a given name:
```
{
    "country": "USA",
    "city": "Portland"
}
```

A city in a state:
```
{
    "country": "USA",
    "state": "OR",
    "city": "Portland"
}
```


A US congressional district:
```
{
    "country": "USA",
    "state": "VA",
    "district_original": "11"
}
```

A ZIP code:
```
{
    "country": "USA",
    "zip": "60661"
}
```


Keys in a location object include:
* **country (required)** - a 3 character code indicating the country to search within.
    * If the country code is *not* `USA`, all further parameters can be ignored.
    * A special country code - `FOREIGN` - represents all non-US countries.
* **state** - a 2 character string abbreviation for the state or territory
* **county** - a 3 digit FIPS code indicating the county
    * If `county` is provided, a `state` must always be provided as well.
    * If `county` is provided, a `district_original` value *must never* be provided.
    * If `county` is provided, a  `district_current` value *must never* be provided.
* **city** - string city name
    * If no `state` is provided, this will return results for all cities in any state with the provided name
* **district_original** - a 2 character code indicating the congressional district
    * When provided, a `state` must always be provided as well.
    * When provided, a `county` *must never* be provided.
    * When provided, `country` must always be "USA".
    * When provided, `district_current` *must never* be provided.
* **district_current** - a 2 character code indicating the current congressional district
    * When provided, a `state` must always be provided as well.
    * When provided, a `county` *must never* be provided.
    * When provided, `country` must always be "USA".
    * When provided, `district_original` *must never* be provided.
* **zip** - a 5 digit string indicating the postal area to search within.


## Keyword Search

**Description:** Search is based on a list of string inputs.

**TODO:**
1. Determine what backend fields are being searched against.

**Example Request:**
```
{
    "keywords": ["example search text", "more search text"]
}
```


Request parameter description:
* `keywords` (List) : List containing one or more strings to search for. Also the top level key name for the filter.

**NOTE: `keyword` (singluar), which accepts a string rather than a list, is being deprecated, but will continue to function until the API is moved to v3**

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
* `start_date`: `2017-10-01` (String) : Start date value for date range filtering.
    Start date value for date range filtering. Currently limited to an earliest date of `2007-10-01` (FY2008). For data going back to `2000-10-01` (FY2001), use either the Custom Award Download feature on the website or one of our `download` or `bulk_download` API endpoints.
* `end_date`:  `2018-09-30` (String) : End date value for date range filtering.
    Currently limited to an earliest date of `2007-10-01` (FY2008).  For data going back to `2000-10-01` (FY2001), use either the Custom Award Download
    feature on the website or one of our `download` or `bulk_download` API endpoints.
* `date_type`:  (enum[string])
    Check specific time period objects for date_type's members, defaults, and descriptions.

## Award Search Time Period Object

**Description:**
See [Time Period](#time-period)

**Examples**
See [Time Period](#time-period)

Request parameter description:
+ `start_date`: (required)
    See [Time Period](#time-period)
+ `end_date`: (required)
    See [Time Period](#time-period)
+ `date_type`: (optional)
    If `date_type` is set to one of the following members then that member is used to compare to both the `start_date` and `end_date` input.
    + Members
        + `action_date`
            This date type value is the default type compared to the `start_date` when a value isn't set for `date_type` in the request. Typically, `action_date` represents the date of the latest transaction associated with the award.
        + `date_signed`
            This date type value is the default type compared to the `end_date` when a value isn't set for `date_type` in the request. Typically, `date_signed` represents the date of the base transaction associated with the award.
        + `last_modified_date`
        + `new_awards_only`
            Indicates when the results should reflect new awards only. You should expect only
            awards whose base transaction date falls within the time period bounds specified to be returned.

## Transaction Search Time Period Object

**Description:**
See [Time Period](#time-period)

**Examples**
See [Time Period](#time-period)

Request parameter description:
+ `start_date`: (required)
    See [Time Period](#time-period)
+ `end_date`: (required)
    See [Time Period](#time-period)
+ `date_type`: (optional)
    + Members
        + `action_date`
            This date type value is the default.
        + `date_signed`
            This date type value is equivalent to `award_date_signed` for transactions. Behind the scenes, if you provide this input we map it to `award_date_signed`.
        + `last_modified_date`
        + `new_awards_only`
            Indicates when the results should reflect new awards only. You should expect only
            transactions that have a date within the time period bounds and are associated with a new award to be returned.

## Subaward Search Time Period Object

**Description:**
See [Time Period](#time-period)

**Examples**
See [Time Period](#time-period)

Request parameter description:
+ `start_date`: (required)
    See [Time Period](#time-period)
+ `end_date`: (required)
    See [Time Period](#time-period)
+ `date_type`: (optional)
    + Members
        + `action_date`
            This date type value is the default.
        + `last_modified_date`

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
    "recipient_search_text": ["D12345678", "Department of Defense"]
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
        "Alaskan Native Corporation Owned Firm"
        ]
}
```

Request Parameter Description:
* `recipient_type_names` (List) : Top level key name for filter. Contains list of strings corresponding to drop down selections. Top or lower level selections are passed here.

## Award Type

**Description:** Filtering based on one or more checkbox selections for award type.

**Acceptable Values:** `A`, `B`, `C`, `D`, `02`, `03`, `04`, `05`, `06`, `07`, `08`, `09`, `10`, `11`, `IDV_A`, `IDV_B`, `IDV_B_A`, `IDV_B_B`, `IDV_B_C`, `IDV_C`, `IDV_D`, `IDV_E`

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
* `award_ids` (List) : Top level key name for filter. Contains list of fains/piids/uris corresponding to Awards. Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.


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
    "naics_codes": {
        "require": ["33"],
        "exclude": ["336411"]
    }
}
```

`naics_codes` (NAICSFilterObject) : Two nullable lists of strings: `require` and `exclude`.
* When `require` is provided, search will only return results that have a NAICS code that starts with one element from the require list.
* When `exclude` is provided,  search will only return results that do NOT have a NAICS code that starts with any element from the exclude list.
* If an element matches both lists, the more specific rule (longer prefix) supercedes.

## TAS

**Description:** There are two filters for TAS: tas_codes and treasury_account_components. Unlike other filters, these two work on OR logic with each other (but AND logic with all other filters)

**Example Request:**
```
{
    "tas_codes": {
        "require": [["091"]],
        "exclude": [["091","091-0800"]]
    },
    "treasury_account_components": [{"aid":"005","bpoa":"2015","epoa":"2015","main":"0107","sub":"000"}]
}
```

`tas_codes` (TASFilterObject) : Two nullable arrays of arrays of strings: `require` and `exclude`.
* When `require` is provided, search will only return results that have a TAS code that is a descendant of one of the paths from the require list.
* When `exclude` is provided,  search will only return results that do NOT have a TAS code that is a descendant of one of the paths from the exclude list.
* If an element matches both lists, the more specific rule (longer array) supercedes.

`treasury_account_components` (TreasuryAccountComponentsObject): List of objects. Each object can have any of the following keys, and will filter results down to those that match the value for each key provided:
* `ata` Allocation Transfer Agency
* `aid` Agency Identifier
* `bpoa` Beginning Period of Availability
* `epoa` Ending Period of Availability
* `a` Availability Code
* `main` Main Account Code
* `sub` Sub Account Code

## PSC

**Description:** Filtering based on autocomplete or tree filter selections for PSC Code.

**Example Request:**
```
{
    "psc_codes": ["1510"]
}
```
*OR*
```
{
    "psc_codes": {
        "require": [["091"]]
        "exclude": [["091", "091-0800"]]
    }
}
```

Request parameter description:
* `psc_codes` (List or Object) : Top level key name for filter.  Contains list of Strings corresponding to PSC Codes or an object that exposes `require` and `exclude` filters where:
    * When `require` is provided, search will only return results that have a PSC code that is a descendant of one of the paths from the require list.
    * When `exclude` is provided, search will only return results that do NOT have a PSC code that is a descendant of one of the paths from the exclude list.
    * If an element matches both lists, the more specific rule (longer array) supercedes.

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
