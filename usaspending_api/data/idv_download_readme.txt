# This file is included in the IDV download zip file.  File names are substituted based on the tags:
#
#    {idv_federal_account_funding}
#    {idv_orders}
#    {idv_transaction_history}
#
# Lines starting with # are ignored.
ABOUT THESE FILES

File: {idv_federal_account_funding}

This file contains award-level financial data representing funding committed to orders made under this IDV (or for Federal Supply Schedules, orders placed under its child IDVs), as reported on a quarterly basis from agency financial systems. This data is also available from the Custom Account Download section of the site. This file does not contain funding committed to the IDV itself, or any Child IDV Orders, if any exists. Financial data is provided at the TAS level for increased granularity, but may easily be rolled up by Federal Account using the federal_account_symbol column.

File: {idv_orders}

This file contains award-level data (data that sums obligated amounts and ceilings from an award's modifications) for all of the orders made under this IDV, including child award orders, child IDV orders made under this IDV, and grand child award orders, if any.

File: {idv_transaction_history}

This file contains transaction-level data representing all of the modifications made to this IDV. This data is also available from the Advanced Search or Custom Award Download sections of the site.
