========ABOUT THESE FILES========

This ZIP file was generated from the COVID-19 Profile page on USAspending.gov, located at https://www.usaspending.gov/disaster/covid-19.

Data Element Definitions: A searchable Data Dictionary that defines every data element in the included files can be found here: https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx. We have also included a copy in this download for convenience. The dictionary is updated periodically as the data model is improved or download headers change.

Split Files: The # in all filenames defaults to 1; if the number of rows in a given file is large enough to warrant breaking it into multiple files, then additional files will be present and appended with 2, 3, etc. instead.

-----------------

FY20P07-Present_All_TAS_AccountBalances_2019-11-20_H23M06S44_1.csv

This file captures the total resources made available under COVID-19 supplemental spending legislation and the current status of those resources: how much was made available (Total Budgetary Resources), how much spending has been promised (Total Obligations), how much has been paid out (Total Outlays), and how much remains available to spend (Total Unobligated Balance).

Each row of this file contains a Treasury Account Symbol (TAS) followed by COVID-19 Disaster Emergency Fund Code (DEFC)(one of 5 codes used to track COVID-19 supplemental appropriations from the 4 bills that authorized it), followed by the dollar balances associated with that TAS + DEFC combination. Non-COVID supplemental appropriations are NOT captured in this file, but this data is available from our Custom Account Download page.

The data in this file is reported to the Government wide Treasury Account Symbol Adjusted Trial Balance System (GTAS) on a monthly basis (other than fiscal year period 01) from audited agency financial systems. The data is provided at the TAS + DEFC level for increased granularity, but may rolled up by TAS, Federal Account, or DEFC as desired, using the appropriate columns.

Only the latest relevant data is included in this file. For the status of these dollar amounts at earlier points in time, please visit Custom Account download and select the particular period of interest.


FY20P07-Present_All_TAS_AccountBreakdownByPA-OC_2019-11-20_H23M06S44_1.csv

The Account Balances file captured the total resources made available by COVID-19 supplemental spending legislation and the current status of those resources. This file takes the spending status components (obligations, outlays, and deobligations) and breaks them down further by Program Activity, Budget Object Class, and Direct/Reimbursable flag.

Each row of this file contains a Treasury Account Symbol (TAS) followed by the COVID-19 DEFC, Program Activity, and Object Class that apply to the row's dollar breakdown, followed by the spending dollar amounts associated with that TAS + DEFC + Object Class + Program Activity combination. Spending that was not appropriated in the four COVID-19 supplemental appropriations bills is NOT captured in this file, but this data is available from our Custom Account Download page.

The data in this file is reported to Data Broker on a monthly basis (other than fiscal year period 01) from audited agency financial systems, as required by the DATA Act of 2014 and OMB M-20-21. The data is provided at the TAS + DEFC + Object Class + Program Activity level for increased granularity, but may rolled up by TAS, Federal Account, Object Class, Program Activity, or DEFC as desired, using the appropriate columns.

Only the latest relevant data is included in this file. For the status of these dollar amounts at earlier points in time, please visit Custom Account download and select the particular period of interest


Contracts_PrimeAwardSummaries_2019-11-20_H18M06S44_1.csv

This file contains award summary-level data for each contract award that was funded (in part or whole) by COVID-19 supplemental appropriation dollars. The specific amounts of COVID-19 supplemental appropriation dollars that have been obligated and outlayed under the award are provided, as well as the overall obligated amount from all sources. Each award is captured in a single data row.

The data in this file is primarily sourced from that reported by contracting officers to the Federal Procurement Data System (FPDS) on a regular basis. It is supplemented by data reported to Data Broker on a monthly basis from audited agency financial systems per the DATA Act of 2014 and OMB M-20-21.

This file is at a summary level. If you are interested in seeing the full set of award transactions under these awards, visit the Custom Award Download. If you are interested in the full set of financial system data tied to these awards, visit the Custom Award Download.


Assistance_PrimeAwardSummaries_2019-11-20_H18M06S44_1.csv

This file contains award summary-level data for each financial assistance (grants, insurance, loans, etc.) award that was funded (in part or whole) by COVID-19 supplemental appropriation dollars. The specific amounts of COVID-19 supplemental appropriation dollars that have been obligated and outlayed under the award are provided, as well as the overall obligated amount from all sources. Each award is captured in a single data row.

The data in this file is primarily sourced from that reported by assistance officers to the Financial Assistance Broker Submission (FABS) component of Data Broker on a regular basis. It is supplemented by data reported to Data Broker on a monthly basis from audited agency financial systems per the DATA Act of 2014 and OMB M-20-21.

This file is at a summary level. If you are interested in seeing the full set of award transactions under these awards, visit the Custom Award Download. If you are interested in the full set of financial system data tied to these awards, visit the Custom Award Download.

Contracts_Subawards_2019-11-20_H18M06S44_1.csv

This file contains transactional subcontract data for all subcontracts that are associated with prime contracts funded (in whole or part) by COVID-19 supplemental appropriation dollars and that have subaward_action_date >= 4/1/2020. Note that COVID-19 supplemental spending dollars are not tracked to the subaward level; so while reasonable conclusions can sometimes be drawn as to how much of a particular row's subaward_amount dollars were funded by COVID-19 supplementals (particularly when the associated prime award was itself funded entirely by them), it remains an inference. If a prime award is funded by a combination of COVID-19 supplemental appropriations and general appropriations, there is no way to be certain from this data which source funded a given subaward.

The data in this file is primarily sourced from that reported by prime contract recipients to the FFATA Subaward Reporting System (FSRS) on a regular basis. It is supplemented by data reported to Data Broker on a monthly basis from audited agency financial systems per the DATA Act of 2014 and OMB M-20-21.


Assistance_Subawards_2019-11-20_H18M06S44_1.csv

This file contains transactional subgrant data for all subgrants that are associated with prime grants funded (in whole or part) by COVID-19 supplemental appropriation dollars and that have subaward_action_date >= 4/1/2020. Note that COVID-19 supplemental spending dollars are not tracked to the subaward level; so while reasonable conclusions can sometimes be drawn as to how much of a particular row's subaward_amount dollars were funded by COVID-19 supplementals (particularly when the associated prime award was itself funded entirely by them), it remains an inference. If a prime award is funded by a combination of COVID-19 supplemental appropriations and general appropriations, there is no way to be certain from this data which source funded a given subaward.

The data in this file is primarily sourced from that reported by prime grant recipients to the FFATA Subaward Reporting System (FSRS) on a regular basis. It is supplemented by data reported to Data Broker on a monthly basis from audited agency financial systems per the DATA Act of 2014 and OMB M-20-21.


File: Data_Dictionary_Crosswalk.xlsx

This file contains the data dictionary covering all elements available for download from USAspending.gov. You can find an online and up-to-date version of the data dictionary here: https://files.usaspending.gov/docs/Data_Dictionary_Crosswalk.xlsx
