The files in this directory are all downloaded from  https://www.census.gov/eos/www/naics/downloadables/downloadables.html
They are parsed by the management command load_naics.py (manage.py load_naics) to populate the naics table in the usaspending database.
These file are pre-formatted in the following ways:
* The parser determines the year for the naics file by searching the title for 20xx. This can appear anywhere in the file name
* The parser expects two columns, the first with the naics code, and the second with naics description. Delete all other rows/columns.