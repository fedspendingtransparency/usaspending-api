# USAspending C to D Linkage

## Background

File C (Award Financial) data is submitted by agencies as a part of quarterly or monthly DABS. Each record in File C represents award activity, details which include an associated TAS. USAspending stores File C data in `financial_accounts_by_awards`.

FPDS and FABS transactions are pulled in nightly. Transactions are grouped into awards based on the following fields and various transaction details are used to create the award record and stored in `awards`:
* Procurement
    * `piid`
    * `agency_id`
    * `parent_award_id` (conditional: Awards-only, not IDV. `atom_type` == 'award')
    * `referenced_idv_agency_iden` (conditional: Awards-only, not IDV. `atom_type` == 'award')
* Assistance
    * `fain` (conditional: non-aggregated. `record_type` != 1)
    * `uri` (conditional: aggregated. `record_type` == 1)
    * `awarding_sub_tier_agency_c`

During the File C to D linkage process, File C records are attempted to be matched to an award ingested from FPDS/FABS (Files D1 & D2 respectively). The linkage logic is defined below.

## Two Linkage Phases

There are two phases in the nightly pipeline that create File C to D linkages.

1. When submissions are loaded from Broker into USAspending
2. After new FABS/FPDS records are loaded from Broker

## Linkage Logic

In all the below scenarios, *only unlinked File C records are considered for updates*. Additionally, links are only made when a File C record matches **EXACTLY ONE** award. If zero or more than one award is matched, no link is made. To help with downstream ETL scripts, if a File C record is linked to a File D award, the File D record has it's `update_date` touched so other scripts can re-calculate award details using the new File C-D linkage.

Note: the predicates described below are only applied to File C awards (stored in `financial_accounts_by_awards`). For example, when linking procurement awards, if `parent_award_id` is null, that is not verified to be null in `awards`. This was re-confirmed at the desired approach December 2020.

1. **Procurement** (FPDS) look for an award with either a matching PIID, or a matching PIID and Parent PIID
    1. If File C `piid` is not null and File C `parent_award_id` is not null, look for an award based on both
        * Upper-case `piid` and `parent_award_id` values in both `financial_accounts_by_awards` and `awards`
        * Create link if `upper(financial_accounts_by_awards.piid)` == `upper(awards.piid)` and `upper(financial_accounts_by_awards.parent_award_id)` == `upper(awards.parent_award_piid)`
    2. If File C `piid` is not null and File C `parent_award_id` is null, look for a File D award based on `piid`
        * Upper-case `piid` values in both `financial_accounts_by_awards` and `awards`
        * Create link if `upper(financial_accounts_by_awards.piid)` == `upper(awards.piid)`
            * This ignores the value in File D Award's `parent_award_piid`
2. **Financial Assistance** (FABS) look for an award with matching FAIN or URI
    1. If File C `fain` is not null and File C `uri` is null, look for award only based on `fain`
        * Upper-case `fain` values in both `financial_accounts_by_awards` and `awards`
        * Create link if `upper(financial_accounts_by_awards.fain)` == `upper(awards.fain)`
            * This ignores the value in File D Award's `uri`
    2. If File C `fain` is null and File C `uri` is not null, look for award only based on `uri`
        * Upper-case `uri` values in both `financial_accounts_by_awards` and `awards`
        * Create link if `upper(financial_accounts_by_awards.uri)` == `upper(awards.uri)`
            * This ignores the value in File D Award's `fain`
    3. If File C `fain` is not null and File C `uri` is not null, look for a distinct award id based on the `fain`. If that does not yield an exact match, use the `uri`.
        * Each check is separate and logic is identical to #1 and #2 above with the exception that the predicate include only non-null `fain` and `uri` records in File C

## ETL scripts

* Management Command: `update_file_c_linkages`
    * Location: [usaspending_api/etl/management/commands/update_file_c_linkages.py](../../commands/update_file_c_linkages.py)
    * Usage: `python3 manage.py update_file_c_linkages`

* Helper function: `update_c_to_d_linkages`
    * Location: [usaspending_api/common/helpers/etl_helpers.py](../../../../common/helpers/etl_helpers.py)
    * Uses outside of the management command:
        * [usaspending_api/broker/management/commands/fabs_nightly_loader.py](../../../../broker/management/commands/fabs_nightly_loader.py)
        * [usaspending_api/broker/management/commands/load_fpds_transactions.py](../../../../broker/management/commands/load_fpds_transactions.py)
        * [usaspending_api/etl/submission_loader_helpers/file_c.py](../../submission_loader_helpers/file_c.py)

* SQL files used to facilitate ETL
    * [usaspending_api/etl/management/sql/c_file_linkage/check_assistance_file_c_linkages.sql](check_assistance_file_c_linkages.sql)
    * [usaspending_api/etl/management/sql/c_file_linkage/check_contract_file_c_linkages.sql](check_contract_file_c_linkages.sql)
    * [usaspending_api/etl/management/sql/c_file_linkage/update_file_c_linkages_fain.sql](update_file_c_linkages_fain.sql)
    * [usaspending_api/etl/management/sql/c_file_linkage/update_file_c_linkages_fain_and_uri.sql](update_file_c_linkages_fain_and_uri.sql)
    * [usaspending_api/etl/management/sql/c_file_linkage/update_file_c_linkages_piid.sql](update_file_c_linkages_piid.sql)
    * [usaspending_api/etl/management/sql/c_file_linkage/update_file_c_linkages_uri.sql](update_file_c_linkages_uri.sql)

## Notes

* The SQL above will not "Unlink" any File C records from File D.
* Unlinks will only occur if File D awards are deleted, and the unlinking is only applied to the specific File D awards which were removed.
* Since the SQL only updates File C records with `null` stored in `award_id`, if it is desired to relink _all_ File C records, the `award_id` for all records must be set to `null` first.
    * Similarly, specific `financial_accounts_by_awards` could also be reset to `null` if only a subset needs to be recalculated.