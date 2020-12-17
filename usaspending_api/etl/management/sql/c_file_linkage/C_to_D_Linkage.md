# C to D Linkage

### Scope = Website

## Background

File C (Award Financial) data is submitted by agencies as a part of quarterly or monthly DABS. Each record in File C represents an Award and its details, details which include an associated TAS.

FPDS and FABS are pulled in nightly in the form of transactions. Transactions are grouped into awards based on the following fields:
* Contracts
    * agency_id
    * referenced_idv_agency_iden
    * piid
    * parent_award_id
* Assistance
    * awarding_sub_tier_agency_c
    * fain
    * uri

During the File C to D linkage process, File C records are attempted to be matched to an award ingested from FPDS/FABS (File D). The linkage logic is defined below.

## Two Methods for Linkages

There are two phases in the nightly pipeline that create File C to D linkages. The linkage logic below focuses on only the second phase.

1. When submissions are loaded from broker into usaspending in the nightly pipeline, File C records from these submissions are attempted to be linked to File D awards.
2. After new FABS/FPDS records are loaded from broker, SQL scripts (described in the logic below) are used to attempt to link File C records from already loaded submissions.

## Linkage Logic

In all the below scenarios, only unlinked File C records are considered for updates. Additionally, links are only made when a File C record matches **EXACTLY ONE** award. If zero or more than one award is matched, no link is made.

1. If PIID is not null, assume it's a **contract** (FPDS) and look for an award with either a matching PIID, or a matching PIID and Parent PIID
    1. if PIID is not null and Parent PIID is not null, look for an award based on both
    2. if PIID is not null and Parent PIID is null, look for an award based on PIID
2. If PIID is null, assume it's **financial assistance** (FABS) and look for an award with matching FAIN or URI
    1. if FAIN is not null and URI is null, look for award only based on FAIN
    2. if URI is not null and FAIN is null, look for award only based on URI
    3. if FAIN and URI are both not null, look for a distinct award id in the transactions based on the FAIN. If that does not yield an exact match, use the URI.

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