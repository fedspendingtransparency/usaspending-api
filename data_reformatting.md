# Reformatting Imported Data

We load data from two primary sources:

1. The current production [USAspending website](https://www.usaspending.gov) (aka, _legacy USAspending_). This provides us with historic data about contract and financial assistance spending.  
2. The [DATA Broker](https://broker.usaspending.gov), which provides us with agency DATA Act submissions.

Generally, data will be imported as-is. In some cases, however, we reformat the data to ensure that it's consistent and usable.

## Geographic Information

Making sure our location-related fields are as robust and accurate as possible powers the geographic filters used by our website and API.

### State Code and Foreign Province

**Data Source:** USAspending history and DATA Broker  
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

### Canonicalizing location text fields

**Data Source:** USAspending history and DATA Broker  
**Code:** `references/helpers.py`

Text fields in location records are stored in a standard format to avoid
false duplicates owing only to quirks of spacing or capitalization.
Values are stored in UPPERCASE, with leading and trailing whitespace
removed, and with any internal whitespace converted to a single space
between words.

The following fields are canonicalized this way:

- _city_name_, _county_name_, _state_name_
- _foreign_city_name_, _foreign_province_
- Address (line 1, 2, and 3)


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

## Federal Accounts

**Data Source:** DATA Broker  
**Code:** `accounts/models.py`

Agencies submit financial information to the DATA Broker by Treasury Account Symbol (TAS). In addition to displaying financial data for each individual TAS, USAspending groups financial data by federal account. Each federal account contains multiple individual TAS.

To create federal account records, we use a unique combination of TAS agency identifier (AID) and main account code. The title that we assign to each federal account is the title of its child TAS with the most recent ending period of availability.

## Annual and Quarterly Balances

**Data Source:** DATA Broker  
**Code:** `accounts/models.py` and `financial_activities/models.py`

Agencies submit financial data to the DATA Broker on a quarterly basis. These numbers (total outlays, for example) are cumulative for the fiscal year of the submission. In other words, quarter three numbers represent all financial activity for quarters one, two, and three.

All annual financial data displayed on USAspending represents the latest set of numbers reported for the fiscal year. Tables and charts that show quarterly data represent activity during that period only. We derive quarterly numbers during the submission load process by taking the submission's numbers and subtracting the totals from the most recent previous submission in the same fiscal year.

For example, if the total outlay reported for a Treasury Account Symbol (TAS) in quarter two is $1000 and the total quarter one outlay for that TAS in quarter one is $300, the quarterly data will show $300 for quarter one and $700 for quarter two.
