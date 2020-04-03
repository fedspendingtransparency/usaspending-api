# Agencies

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

Toptier Agencies are identified by their `toptier_code` which can be either the agency's three
digit Common Government-Wide Accounting Classification (CGAC) or four digit Financial Reporting
Entity Code (FREC).  Deciding whether to use CGAC or FREC to identify a toptier agency is
controlled by the `is_frec` flag in the source agency CSV file.

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

This rollup largely occurs in code rather than in the database (see DOD_SUBSUMED_CGAC).

Additionally, there are two FREC agencies that get rolled into Department of Defense.  These agencies
fall under 011 Executive Office of the President but are entirely funded by Department of Defense:

* 1137 (only as part of CGAC 011)
* DE00 (only as part of CGAC 011)

These FREC agencies do not show up in our toptier table and therefore require special handling when
querying Federal or Treasury Account data (see DOD_ARMED_FORCES_TAS_CGAC_FREC).

Finally, there is a special case Federal Account that is primarily (but not entirely) funded by
Department of Defense but falls under Executive Office of the President.

* 011-1082

This account occasionally requires special handling when querying aggregated account level data since
we want to to display as a Department of Defense account (see DOD_FEDERAL_ACCOUNTS).

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

## User Selectable Flag

The proper usage of the `user_selectable` flag has been a little hazy since its introduction.
Currently, it is being used to control which agencies are displayed in dropdowns on Download pages
and is used to restrict the agencies for which Delta files are generated.

It is **NOT** currently being used to limit agency typeaheads in advanced search.  In the advanced
search typeahead, any agency with a subtier is being listed unless it is contained in the
EXCLUDE_CGAC global.

# Population Data

Since census population data doesn't change very often (at most annually) there isn't much need to make the process streamlined.

1. Create temp tables which match the source CSV
1. Load CSV data into new temp table
1. Truncate reference data population table
1. Insert temp table data into reference data population table
1. Drop temp table


## County (and state) Population Data

Step 1: Create temporary table

    CREATE TABLE co_est2019_alldata (SUMLEV TEXT,REGION TEXT,DIVISION TEXT,STATE TEXT,COUNTY TEXT,STNAME TEXT,CTYNAME TEXT,CENSUS2010POP TEXT,ESTIMATESBASE2010 TEXT,POPESTIMATE2010 TEXT,POPESTIMATE2011 TEXT,POPESTIMATE2012 TEXT,POPESTIMATE2013 TEXT,POPESTIMATE2014 TEXT,POPESTIMATE2015 TEXT,POPESTIMATE2016 TEXT,POPESTIMATE2017 TEXT,POPESTIMATE2018 TEXT,POPESTIMATE2019 TEXT,NPOPCHG_2010 TEXT,NPOPCHG_2011 TEXT,NPOPCHG_2012 TEXT,NPOPCHG_2013 TEXT,NPOPCHG_2014 TEXT,NPOPCHG_2015 TEXT,NPOPCHG_2016 TEXT,NPOPCHG_2017 TEXT,NPOPCHG_2018 TEXT,NPOPCHG_2019 TEXT,BIRTHS2010 TEXT,BIRTHS2011 TEXT,BIRTHS2012 TEXT,BIRTHS2013 TEXT,BIRTHS2014 TEXT,BIRTHS2015 TEXT,BIRTHS2016 TEXT,BIRTHS2017 TEXT,BIRTHS2018 TEXT,BIRTHS2019 TEXT,DEATHS2010 TEXT,DEATHS2011 TEXT,DEATHS2012 TEXT,DEATHS2013 TEXT,DEATHS2014 TEXT,DEATHS2015 TEXT,DEATHS2016 TEXT,DEATHS2017 TEXT,DEATHS2018 TEXT,DEATHS2019 TEXT,NATURALINC2010 TEXT,NATURALINC2011 TEXT,NATURALINC2012 TEXT,NATURALINC2013 TEXT,NATURALINC2014 TEXT,NATURALINC2015 TEXT,NATURALINC2016 TEXT,NATURALINC2017 TEXT,NATURALINC2018 TEXT,NATURALINC2019 TEXT,INTERNATIONALMIG2010 TEXT,INTERNATIONALMIG2011 TEXT,INTERNATIONALMIG2012 TEXT,INTERNATIONALMIG2013 TEXT,INTERNATIONALMIG2014 TEXT,INTERNATIONALMIG2015 TEXT,INTERNATIONALMIG2016 TEXT,INTERNATIONALMIG2017 TEXT,INTERNATIONALMIG2018 TEXT,INTERNATIONALMIG2019 TEXT,DOMESTICMIG2010 TEXT,DOMESTICMIG2011 TEXT,DOMESTICMIG2012 TEXT,DOMESTICMIG2013 TEXT,DOMESTICMIG2014 TEXT,DOMESTICMIG2015 TEXT,DOMESTICMIG2016 TEXT,DOMESTICMIG2017 TEXT,DOMESTICMIG2018 TEXT,DOMESTICMIG2019 TEXT,NETMIG2010 TEXT,NETMIG2011 TEXT,NETMIG2012 TEXT,NETMIG2013 TEXT,NETMIG2014 TEXT,NETMIG2015 TEXT,NETMIG2016 TEXT,NETMIG2017 TEXT,NETMIG2018 TEXT,NETMIG2019 TEXT,RESIDUAL2010 TEXT,RESIDUAL2011 TEXT,RESIDUAL2012 TEXT,RESIDUAL2013 TEXT,RESIDUAL2014 TEXT,RESIDUAL2015 TEXT,RESIDUAL2016 TEXT,RESIDUAL2017 TEXT,RESIDUAL2018 TEXT,RESIDUAL2019 TEXT,GQESTIMATESBASE2010 TEXT,GQESTIMATES2010 TEXT,GQESTIMATES2011 TEXT,GQESTIMATES2012 TEXT,GQESTIMATES2013 TEXT,GQESTIMATES2014 TEXT,GQESTIMATES2015 TEXT,GQESTIMATES2016 TEXT,GQESTIMATES2017 TEXT,GQESTIMATES2018 TEXT,GQESTIMATES2019 TEXT,RBIRTH2011 TEXT,RBIRTH2012 TEXT,RBIRTH2013 TEXT,RBIRTH2014 TEXT,RBIRTH2015 TEXT,RBIRTH2016 TEXT,RBIRTH2017 TEXT,RBIRTH2018 TEXT,RBIRTH2019 TEXT,RDEATH2011 TEXT,RDEATH2012 TEXT,RDEATH2013 TEXT,RDEATH2014 TEXT,RDEATH2015 TEXT,RDEATH2016 TEXT,RDEATH2017 TEXT,RDEATH2018 TEXT,RDEATH2019 TEXT,RNATURALINC2011 TEXT,RNATURALINC2012 TEXT,RNATURALINC2013 TEXT,RNATURALINC2014 TEXT,RNATURALINC2015 TEXT,RNATURALINC2016 TEXT,RNATURALINC2017 TEXT,RNATURALINC2018 TEXT,RNATURALINC2019 TEXT,RINTERNATIONALMIG2011 TEXT,RINTERNATIONALMIG2012 TEXT,RINTERNATIONALMIG2013 TEXT,RINTERNATIONALMIG2014 TEXT,RINTERNATIONALMIG2015 TEXT,RINTERNATIONALMIG2016 TEXT,RINTERNATIONALMIG2017 TEXT,RINTERNATIONALMIG2018 TEXT,RINTERNATIONALMIG2019 TEXT,RDOMESTICMIG2011 TEXT,RDOMESTICMIG2012 TEXT,RDOMESTICMIG2013 TEXT,RDOMESTICMIG2014 TEXT,RDOMESTICMIG2015 TEXT,RDOMESTICMIG2016 TEXT,RDOMESTICMIG2017 TEXT,RDOMESTICMIG2018 TEXT,RDOMESTICMIG2019 TEXT,RNETMIG2011 TEXT,RNETMIG2012 TEXT,RNETMIG2013 TEXT,RNETMIG2014 TEXT,RNETMIG2015 TEXT,RNETMIG2016 TEXT,RNETMIG2017 TEXT,RNETMIG2018 TEXT,RNETMIG2019 TEXT);

Step 2: Insert CSV data into temporary table

    psql $DATABASE_URL -v ON_ERROR_STOP=1 -c "COPY co_est2019_alldata FROM STDIN WITH CSV HEADER" < /Users/tonysappe/Downloads/census_2019_county_clean.csv

Step 3: Restock the reference data table

    BEGIN;
    TRUNCATE TABLE ref_population_county RESTART IDENTITY;

    INSERT INTO ref_population_county (state_code, state_name, county_number, county_name, latest_population)
    SELECT state, stname, county, ctyname, popestimate2019::bigint
    FROM co_est2019_alldata;

    DROP TABLE co_est2019_alldata;
    COMMIT;

## Congressional District Population Data

Step 1: Create temporary table

    CREATE TABLE cong_est2019_alldata(population TEXT, state_code TEXT,usps_code TEXT,state_name TEXT,congressional_district TEXT);

Step 2: Insert CSV data into temporary table

    psql $DATABASE_URL -v ON_ERROR_STOP=1 -c "COPY cong_est2019_alldata FROM STDIN WITH CSV HEADER" < /Users/tonysappe/Downloads/2018_PopulationCongressionalDistrict.csv

Step 3: Restock the reference data table

    BEGIN;
    TRUNCATE TABLE ref_population_cong_district RESTART IDENTITY;

    INSERT INTO ref_population_cong_district (state_code, state_name, state_abbreviation, congressional_district, latest_population)
    SELECT lpad(state_code, 2, '0'), state_name, usps_code, lpad(congressional_district, 2, '0'), population::bigint
    FROM  cong_est2019_alldata;

    DROP TABLE cong_est2019_alldata;
    COMMIT;
