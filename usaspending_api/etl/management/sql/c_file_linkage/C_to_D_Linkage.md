# C to D Linkage

### Scope = Website

## Background

File C (Award Financial) is submitted by agencies on a quartlery basis. Each record in File C represents an Award and its details, details which include an associated TAS.

FPDS and FABS are pulled in nightly in the form of transactions. Transactions are grouped into awards based on the grouping of the following fields:
* Contracts
    * agency_id
    * referenced_idv_agency_iden
    * piid
    * parent_award_id
* Assistance
    * awarding_sub_tier_agency_c
    * fain
    * uri


File C records should be a subset of the Awards data ingested from FPDS/FABS. The linkage logic is defined below.

## Linkage Logic

1. If PIID is not null, assume it's a contract and look for an award with either a matching PIID, or a matching PIID and Parent PIID if and only if a Parent PIID is present in the File C record.
    * if 0 awards found, no link is made
    * if > 1 awards found, no link is made
    * if exactly 1 award is found, the row is linked to the award
2. If PIID is null, assume it's financial assistance and look for an award with matching FAIN or URI
    1. if FAIN is not null and URI is null, look for award only based on fain
        * if 0 awards found, no link is made
        * if > 1 awards found, no link is made
        * if exactly 1 award is found, the row is linked to the award
    2. if URI is not null and FAIN is null, look for award only based on URI
        * if 0 awards found, no link is made
        * if > 1 awards found, no link is made
        * if exactly 1 award is found, the row is linked to the award
    3. if FAIN and URI are both not null, look for a distinct award id in the transactions based on the FAIN. If that does not yield an exact match, use the URI.
        * if 0 unique awards found, no link is made
        * if > 1 unique awards found, no link is made
        * if exactly 1 award is found, the row is linked to the award

## Execution

* SQL files used to facilitate execution:
    * usaspending_api/etl/management/sql/check_assistance_file_c_linkages.sql
    * usaspending_api/etl/management/sql/check_contract_file_c_linkages.sql
    * usaspending_api/etl/management/sql/update_file_c_linkages_fain.sql
    * usaspending_api/etl/management/sql/update_file_c_linkages_fain_and_uri.sql
    * usaspending_api/etl/management/sql/update_file_c_linkages_piid.sql
    * usaspending_api/etl/management/sql/update_file_c_linkages_uri.sql

* Helper function: `update_c_to_d_linkages`
    * Location: usaspending_api/common/helpers/etl_helpers.py
    * Uses:
        * usaspending_api/broker/management/commands/fabs_nightly_loader.py
        * usaspending_api/broker/management/commands/fpds_nightly_loader.py
        * usaspending_api/etl/management/commands/update_file_c_linkages.py

* Management Command: `python manage.py update_file_c_linkages`
    * Average run time = ~1 hour

* Notes:
    * If relinking _all_ File C records, the `award_id` for all records must be set to `NULL` first:

        ```
        UPDATE financial_accounts_by_awards
        SET award_id=NULL;
        ```