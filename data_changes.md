# Changes to Imported Data

We load data from two primary sources:

1. The current production [USAspending website](https://www.usaspending.gov) (aka, _legacy USAspending_). This provides us with historic data about contract and financial assistance spending.  
2. The [DATA Broker](https://broker.usaspending.gov), which provides us with agency DATA Act submissions.

Generally, data will be imported as-is. Below are the exceptions to that rule.

## Geographic Information

Making sure our location-related fields are as robust and accurate as possible powers the geographic filters used by our website and API.

### State Code and Foreign Province

**Data Source:** DATA Broker  
**Code:** `submissions/management/commands/load_submission.py`

When loading information from the DATA Act broker, if the data has a state code but does not have a country code of `USA`, we load the state code as a foreign province.


### State, County, and City

**Data Source:** USAspending history and DATA Broker  
**Code:** `references/models.py`

We attempt to match incoming records that have partial location data to already-stored unique combinations of _state code_, _state name_, _city code_, _county code_, and _county name_.

For example, if a record has a state code but no state name, we'll pull in state name from our master list of geographical data and add it to the record during the load.

### Country Codes and Names

**Data Source:** USAspending history  
**Code:** `etl/helpers.py`

If a legacy USAspending record doesn't have a country code, we attempt to find one by matching the country name (if provided) against our existing country list.

## Single Character Code Extracts

**Data Source:** USAspending history  
**Code:** `etl/helpers.py`

Codes and descriptions in legacy Usaspending data are often stored in the same field. For example, a funding agency column looks like `7300: SMALL BUSINESS ADMINISTRATION`.

In these cases, we extract the code to use in our data load. We then use that code to look up the description against a canonical data source. Using a single, central source for code descriptions ensures data consistency.

## Award Type

**Data Source:** USAspending history  
**Code:** `etl/management/commands/load_usaspending_contracts.py`

If a legacy USAspending contract record has a string description of the award type instead of a single character, we parse the string to find the appropriate single-character award type.

## Awards

**Data Source:** USAspending history and DATA Broker  
**Code:** `awards/models.py`

Currently, award-level data coming from both legacy USAspending records and data broker submissions represents transactional information. In other words, we receive information about individual spending events against an award, but no records that represent the award itself.

To make it easier for users to see the lifespan of an award, we create award records when loading the contract and financial assistance transactions. These award "holders" allow us group all related transactions together, so people can see the entire spending history.
