========ABOUT THESE FILES========

This ZIP file was generated from a specific Assistance Award Summary Page on USAspending.gov, located at https://www.usaspending.gov/award/[AWARD_ID]

Data Element Definitions: A searchable Data Dictionary that defines every data element in the included files can be found here: https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx. We have also included a copy in this download for convenience. Note that the dictionary is updated periodically.

Empty Files: When no data is available for a given file, its contents will only contain column headers (no records will be included).

Split Files: The # in all filenames defaults to 1; if the number of rows in a given file is large enough to warrant breaking it into multiple files, then additional files will be present and appended with 2, 3, etc. instead.

Award ID Characters: In edge cases where the Award ID (FAIN or URI) contain characters that are file system unfriendly (e.g., '?' or '='), they are converted to '_' characters for purposes of file names (no underlying data within the files is altered).

-----------------

File: Assistance_[Award ID]_FederalAccountFunding_#.csv

This file contains Account Breakdown By Award data, which is reported on a quarterly basis from audited agency financial systems as required by the DATA Act of 2014. It is a breakdown of funding for this award by Treasury Account, Budget Function, Object Class, and sometimes Program Activity--effectively linking the appropriation, budget, financial, and award spheres. Financial data is provided at the TAS level for increased granularity, but may easily be rolled up by Federal Account using the federal_account_symbol column. This data is also available from the Custom Account Download section of the site.

Note that the DATA Act of 2014 went into effect FY17Q2; as such, Account Breakdown by Award data is only available from January 2017 onward, and will not be present for award transactions that occurred prior to that point. Note also that a subset of agency-submitted Account Breakdown by Award data is not definitively linkable to a single Federal Award; unlinked data is available via Custom Account Download only.

File: Assistance_[Award ID]_Sub-Awards_#.csv

This file contains all Sub-Grant data associated with this prime award. Sub-Grant data is also available from the Advanced Search or Custom Award Download sections of the site.

File: Assistance_[Award ID]_TransactionHistory_#.csv

This file contains transaction-level data for all of the modifications made to this assistance award, including the base award. This data is also available from the Advanced Search, Award Data Archive, and Custom Award Download sections of the site.

File: Data_Dictionary_Crosswalk.xlsx

This file contains the data dictionary covering all elements available for download from USAspending.gov. You can find an online and up-to-date version of the data dictionary here: https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx