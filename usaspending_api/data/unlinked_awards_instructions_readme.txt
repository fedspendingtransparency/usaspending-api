Pursuant to a recommendation in GAO-22-104702 that Treasury ‘design and implement a process to periodically inform agencies
about unlinked data to help agencies reconcile and resolve data linkage differences between transaction and submission data
on USAspending.gov.’ The USAspending.gov team created the Agency Submissions Statistics page (ASSP) in response to user
feedback requesting greater transparency into the completeness of agency data available on the site.

On the ASSP, the Number of Unlinked Awards section has columns that show how many awards pertaining to the selected submission period are 'unlinked.'
Unlinked awards lack a shared award ID that allows two systems to match their records.

When an award is linked, USAspending uses the data to add context both on-screen and in downloads. Examples:

- On the Award Summary pages we leverage linked account data from File C and D1/D2 to display the federal accounts,
object classes, and program activities that funded an award.
- The Account Breakdown by Award data that can be found in Custom Account Downloads page includes information
about the award recipient sourced from the Contract Awards and Assistance Awards data.

In contrast, an unlinked award will not have an Award Summary page or recipient information in the download.
For more information about unlinked awards,
see https://www.usaspending.gov/submission-statistics/data-sources?section=unlinked_awards

The files included in this ZIP file will help agencies identify unlinked awards. The ZIP file includes the files:
- Unlinked Contracts: [agency name]_UnlinkedContracts_[date/time generated]
- Unlinked Financial Assistance Awards: [agency name]_UnlinkedFinancialAssistance_[date/time generated], generic file names for contacts, Financial
- Unlinked awards listed in File C: [agency name]_UnlinkedAccountsByAward_[date/time generated]
- A summary of the data on the ASSP: [agency name]_UnlinkedAwardsSummary_[date/time generated]
- These instructions: Unlinked Awards Instructions Readme

***To determine the unlinked awards in contracts (File D1):

1. Open the Unlinked Contracts file in Excel and filter for the fiscal_year and fical_period to match the time frame selected on the ASSP.
2. Extract the selected data to a new workbook.
3. Select all the data in the new workbook. Go to the 'Data' tab and select 'Remove Duplicates'.
4. Click the 'Unselect All' button and then select only 'contract_award_unique_key'.

This will result in one row per contract.
Note: This may not match ASSP exactly as ASSP is dynamic as will update as information is added to the database.

***To determine the unlinked contracts in File C:

1. Open the Unlinked Accounts by Award file in Excel and filter for the fiscal_year and fical_period to match the time frame selected on the ASSP.
2. Filter to remove blanks in the 'piid' column.
3. Extract the selected data to a new workbook.
4. Select all the data in the new workbook. Go to the 'Data' tab and select 'Remove Duplicates'.
5. Click the 'Unselect All' button and then select only 'piid'.

This will result in one row per contract.
Note: This may not match ASSP exactly as ASSP is dynamic as will update as information is added to the database.

***To determine the unlinked financial assistance awards (File D2):

1. Open the Unlinked Financial Assistance file in Excel and filter for the fiscal_year and fical_period to match the time frame selected on the ASSP.
2. Extract the selected data to a new workbook.
3. Select all the data in the new workbook. Go to the 'Data' tab and select 'Remove Duplicates'.
4. Click the 'Unselect All' button and then select only 'assistance_award_unique_key'.

This will result in one row per financial assistance award.
Note: This may not match ASSP exactly as ASSP is dynamic as will update as information is added to the database.

***To determine the unlinked assistance awards in File C:

1. Open the Unlinked Accounts by Award file in Excel and filter for the fiscal_year and fical_period to match the time frame selected on the ASSP.
2. Filter to remove blanks in the 'fain' column.
3. Extract the selected data to a new workbook.
4. Select all the data in the new workbook. Go to the 'Data' tab and select 'Remove Duplicates'.
5. Click the 'Unselect All' button and then select only 'fain'.

This will result in one row per financial assistance award.
Note: This may not match ASSP exactly as ASSP is dynamic as will update as information is added to the database.
