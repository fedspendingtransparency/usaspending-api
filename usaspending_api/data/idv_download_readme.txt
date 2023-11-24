# This file is included in the IDV download zip file.
#
# Lines starting with # are ignored.
========ABOUT THESE FILES========

Data Element Definitions: A searchable Data Dictionary that defines every data element in the included files can be found here: https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx. We have also included a copy in this download for convenience.

Empty Files: When no data is available for a given file, its contents will only contain column headers (no records will be included).

Split Files: The # in all filenames defaults to 1; if the number of rows in a given file is large enough to warrant breaking it into multiple files, then additional files will be present and appended with 2, 3, etc. instead.

PIID Characters: In edge cases where PIID contain characters that are file system unfriendly (e.g., '?' or '='), they are converted to '_' characters for purposes of file names (no underlying data within the files is altered).

-----------------

File: IDV_[Award PIID]_FederalAccountFunding_#.csv

This file contains award-level financial data representing funding committed to award orders made under this IDV (including child and grandchild award orders), as reported on a quarterly basis from agency financial systems. This data is also available from the Custom Account Download section of the site. This file does not contain funding committed to the IDV itself, if any exists, or funding committed to any child IDV orders (if any exists). Financial data is provided at the TAS level for increased granularity, but may easily be rolled up by Federal Account using the federal_account_symbol column.

File: IDV_[Award PIID]_Orders_#.csv

This file contains award-level data (data that sums obligated amounts and ceilings across each of an award's modifications) for all child IDV orders (if any), child award orders, and grandchild award orders (if any) made under this IDV.

File: IDV_[Award PIID]_TransactionHistory_#.csv

This file contains transaction-level data representing all of the modifications made to this IDV. This data is also available from the Advanced Search or Custom Award Download sections of the site.

File: Data_Dictionary_Crosswalk.xlsx

This file contains the data dictionary covering all elements available for download from USAspending.gov. You can find an online and up-to-date version of the data dictionary here: https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx
