# A Little Background

Whenever an award or subaward is made, there are several federal agencies tied to the award
including, but not limited to:

* Awarding Agency - The agency that interacted with the recipient to make the award.
* Funding Agency - The agency that provided the bulk of the funding for the award.
* Reporting Agency - The agency that reported the award to the system of record (FSRS, DABS, etc).

These agencies may overlap, be missing entirely or, in the case of some legacy data, be invalid
or inaccurate.

Agencies fall into two tiers.

## Toptier Agency

Toptier Agencies are likely what most people think of when they think of government agencies.
Examples include Department of Defense (DoD), Department of the Treasury (TREAS), Department
of Health and Human Services (HHS), etc.  There are about 195 of these.

Toptier Agencies are identified by their `cgac_code` which is, unfortunately, a bit of
a misnomer.  While MOST agencies are identified by their three digit Common Government-Wide
Accounting Classification (CGAC), there are several identified by their Financial Reporting
Entity Code (FREC).  This is a point of confusion for many new developers and is on a very
long TODO list of things to clean up when we have time.

Deciding whether to use CGAC or FREC to identify a toptier agency is controlled by the `is_frec`
flag in the source agency CSV file.

## Subtier Agency

Subtier Agencies are entities that fall within the purview of a Toptier Agency, for example Office
of Inspector General within the Department of the Treasury (TREAS) or National Institutes of Health
(NIH) within the Department of Health and Human Services (HHS).  There are about 1,500 of these.

Subtier agencies are identified by a four digit Subtier code.  The Subtier code list is maintained
by the product owners of USAspending.

An agency can have many subtiers.  Often, but not always, one of those subtiers
represents the agency as a whole.  For example, 020 is Department of the Treasury.  As of
this writing, Treasury has 45 subtiers.  One of those subtiers, 2000, represents the entirety
of Treasury.  It is roughly equivalent to 020 but at the subtier level.  2004, on the other
hand, represents the Office of Inspector General within Treasury and, as such, is not equivalent
to Treasury as a whole.  `toptier_flag` is used to identify those subtiers that are essentially
equivalent to their toptier counterpart.  In this example, `toptier_flag` would be True for 2000
but False for 2004.

## Armed Forces

There are currently three toptier agencies that get rolled up into 097 Department of Defense for
reporting purposes:

* 017 Department of the Navy
* 021 Department of the Army
* 057 Department of the Air Force

This rollup largely occurs in code rather than in the database, however, these agencies are the
only toptier agencies that may be represented in the agency table without subtiers.

## FEMA

058 Federal Emergency Management Agency (FEMA) is also a special case.  FEMA was originally a
toptier agency, however, the Homeland Security Act of 2002 saw the creation of 070 Department
of Homeland Security (DHS) and the absorption of FEMA into DHS as a subtier agency (7022).
Because we have some historical data attached to FEMA as a toptier agency we keep it around.  It
occasionally receives special handling in some endpoints, but receives no special attention in
the database where it is recorded as both a toptier agency and a subtier agency.

## CFO Agencies

The Chief Financial Officers (CFO) Act of 1990 established CFOs at several of the larger toptier
agencies.  These agencies are not yet identified in the database but can be found defined in the
application global constants CFO_CGACS_MAPPING and CFO_CGACS.  It is on the TODO list to
incorporate this information into the database.
