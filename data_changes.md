## Changes to Imported Data ##

Generally, data will be imported as-is from historical data or newly given data
with a few exceptions:

- _IMPORT TYPE_: Broker, USASpending
- _CHANGE_: **Creation of parent awards**
- _DESCRIPTION_: Parent awards that do not currently exist in the database will be
created with empty data, to provide references for transactions being loaded.
- _FILE_: `submissions/management/commands/load_submission.py`, et. al.


- _IMPORT TYPE_: Broker
- _CHANGE_: **State code converted to foreign province**
- _DESCRIPTION_: Cases where a location must be created, but the data specified has
a state code, but is not also specified with country code of 'USA' will be converted
to a foreign province
- _FILE_: `submissions/management/commands/load_submission.py`


- _IMPORT TYPE_: All (Location Data)
- _CHANGE_: *Location Data is updated if a valid match is found*
- _DESCRIPTION_: Cases where some location, but not all, data is specified, if a
single unique reference (STATE CODE, CITY CODE, COUNTY CODE, STATE NAME, COUNTY NAME)
is found, the location object is updated with that matching data
- _FILE_: `references/models.py`


- _IMPORT TYPE_: USA Spending, Contract; USA Spending, Financial
- _CHANGE_: *Extraction of Single Character Codes*
- _DESCRIPTION_: Data loaded from legacy USA Spending data typically comes in the format of
"<CODE>: <DESCRIPTION>" where code is a single character. In these cases, the single code is
extracted and stored in the database, with descriptions being provided via enumerated dictionaries.
- _FILE_:
 `etl/management/commands/load_usaspending_contracts.py` et al.


- _IMPORT TYPE_: USA Spending, Contract; USA Spending, Financial
- _CHANGE_: **Extraction of Single Character Codes**
- _DESCRIPTION_: Data loaded from legacy USA Spending data typically comes in the format of
"<CODE>: <DESCRIPTION>" where code is a single character. In these cases, the single code is
extracted and stored in the database, with descriptions being provided via enumerated dictionaries.
- _FILE_: `etl/management/commands/load_usaspending_contracts.py` et al.


- _IMPORT TYPE_: USA Spending, Contract
- _CHANGE_: **Award type derived from string**
- _DESCRIPTION_: In some instances, a character representation of the award type is not
provided, but rather a string description. In this case, the string is parsed to store
an appropriate character.
- _FILE_: `etl/management/commands/load_usaspending_contracts.py`
