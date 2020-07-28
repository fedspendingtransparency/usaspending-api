import logging
import re

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.submission_loader_helpers.skipped_tas import update_skipped_tas
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.etl.submission_loader_helpers.bulk_create_manager import BulkCreateManager
from usaspending_api.etl.submission_loader_helpers.disaster_emergency_fund_codes import get_disaster_emergency_fund
from usaspending_api.etl.submission_loader_helpers.object_class import get_object_class
from usaspending_api.etl.submission_loader_helpers.program_activities import get_program_activity
from usaspending_api.etl.submission_loader_helpers.treasury_appropriation_account import (
    bulk_treasury_appropriation_account_tas_lookup,
    get_treasury_appropriation_account_tas_lookup,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


logger = logging.getLogger("script")


def get_file_b(submission_attributes, db_cursor):
    """
    Get broker File B data for a specific submission.
    This function was added as a workaround for the fact that a few agencies (two, as of April, 2017: DOI and ACHP)
    submit multiple File B records for the same object class. These "dupes", come in as the same 4 digit object
    class code but with one of the direct reimbursable flags set to NULL.

    From our perspective, this is a duplicate, because we get our D/R info from the 1st digit of the object class when
    it's four digits.

    Thus, this function examines the File B data for a given submission. If it has the issue of "duplicate" object
    classes, it will squash the offending records together so that all financial totals are reporting as a single object
    class/program activity/TAS record as expected.

    If the broker validations change to prohibit this pattern in the data, this intervening function will no longer be
    necessary, we can go back to selecting * from the broker's File B data.

    Args:
        submission_attributes: submission object currently being loaded
        db_cursor: db connection info
    """
    submission_id = submission_attributes.submission_id

    # does this file B have the dupe object class edge case?
    check_dupe_oc = f"""
        select      count(*)
        from        certified_object_class_program_activity
        where       submission_id = %s and length(object_class) = 4
        group by    tas_id, program_activity_code, object_class, disaster_emergency_fund_code
        having      count(*) > 1
    """
    db_cursor.execute(check_dupe_oc, [submission_id])
    dupe_oc_count = len(dictfetchall(db_cursor))

    if dupe_oc_count == 0:
        # there are no object class duplicates, so proceed as usual
        db_cursor.execute(
            "select * from certified_object_class_program_activity where submission_id = %s", [submission_id]
        )
    else:
        # file b contains at least one case of duplicate 4 digit object classes for the same program activity/tas,
        # so combine the records in question
        combine_dupe_oc = f"""
            select
                submission_id,
                job_id,
                agency_identifier,
                allocation_transfer_agency,
                availability_type_code,
                beginning_period_of_availa,
                ending_period_of_availabil,
                main_account_code,
                right(object_class, 3) as object_class,
                case
                    when length(object_class) = 4 and left(object_class, 1) = '1' then 'D'
                    when length(object_class) = 4 and left(object_class, 1) = '2' then 'R'
                    else by_direct_reimbursable_fun
                end as by_direct_reimbursable_fun,
                tas,
                tas_id,
                program_activity_code,
                program_activity_name,
                sub_account_code,
                sum(deobligations_recov_by_pro_cpe) as deobligations_recov_by_pro_cpe,
                sum(gross_outlay_amount_by_pro_cpe) as gross_outlay_amount_by_pro_cpe,
                sum(gross_outlay_amount_by_pro_fyb) as gross_outlay_amount_by_pro_fyb,
                sum(gross_outlays_delivered_or_cpe) as gross_outlays_delivered_or_cpe,
                sum(gross_outlays_delivered_or_fyb) as gross_outlays_delivered_or_fyb,
                sum(gross_outlays_undelivered_cpe) as gross_outlays_undelivered_cpe,
                sum(gross_outlays_undelivered_fyb) as gross_outlays_undelivered_fyb,
                sum(obligations_delivered_orde_cpe) as obligations_delivered_orde_cpe,
                sum(obligations_delivered_orde_fyb) as obligations_delivered_orde_fyb,
                sum(obligations_incurred_by_pr_cpe) as obligations_incurred_by_pr_cpe,
                sum(obligations_undelivered_or_cpe) as obligations_undelivered_or_cpe,
                sum(obligations_undelivered_or_fyb) as obligations_undelivered_or_fyb,
                sum(ussgl480100_undelivered_or_cpe) as ussgl480100_undelivered_or_cpe,
                sum(ussgl480100_undelivered_or_fyb) as ussgl480100_undelivered_or_fyb,
                sum(ussgl480200_undelivered_or_cpe) as ussgl480200_undelivered_or_cpe,
                sum(ussgl480200_undelivered_or_fyb) as ussgl480200_undelivered_or_fyb,
                sum(ussgl483100_undelivered_or_cpe) as ussgl483100_undelivered_or_cpe,
                sum(ussgl483200_undelivered_or_cpe) as ussgl483200_undelivered_or_cpe,
                sum(ussgl487100_downward_adjus_cpe) as ussgl487100_downward_adjus_cpe,
                sum(ussgl487200_downward_adjus_cpe) as ussgl487200_downward_adjus_cpe,
                sum(ussgl488100_upward_adjustm_cpe) as ussgl488100_upward_adjustm_cpe,
                sum(ussgl488200_upward_adjustm_cpe) as ussgl488200_upward_adjustm_cpe,
                sum(ussgl490100_delivered_orde_cpe) as ussgl490100_delivered_orde_cpe,
                sum(ussgl490100_delivered_orde_fyb) as ussgl490100_delivered_orde_fyb,
                sum(ussgl490200_delivered_orde_cpe) as ussgl490200_delivered_orde_cpe,
                sum(ussgl490800_authority_outl_cpe) as ussgl490800_authority_outl_cpe,
                sum(ussgl490800_authority_outl_fyb) as ussgl490800_authority_outl_fyb,
                sum(ussgl493100_delivered_orde_cpe) as ussgl493100_delivered_orde_cpe,
                sum(ussgl497100_downward_adjus_cpe) as ussgl497100_downward_adjus_cpe,
                sum(ussgl497200_downward_adjus_cpe) as ussgl497200_downward_adjus_cpe,
                sum(ussgl498100_upward_adjustm_cpe) as ussgl498100_upward_adjustm_cpe,
                sum(ussgl498200_upward_adjustm_cpe) as ussgl498200_upward_adjustm_cpe,
                disaster_emergency_fund_code
            from
                certified_object_class_program_activity
            where
                submission_id = %s
            group by
                submission_id,
                job_id,
                agency_identifier,
                allocation_transfer_agency,
                availability_type_code,
                beginning_period_of_availa,
                ending_period_of_availabil,
                main_account_code,
                right(object_class, 3),
                case
                    when length(object_class) = 4 and left(object_class, 1) = '1' then 'D'
                    when length(object_class) = 4 and left(object_class, 1) = '2' then 'R'
                    else by_direct_reimbursable_fun
                end,
                program_activity_code,
                program_activity_name,
                sub_account_code,
                tas,
                tas_id,
                disaster_emergency_fund_code
        """
        logger.info(
            f"Found {dupe_oc_count:,} duplicated File B 4 digit object codes in submission {submission_id}. "
            f"Aggregating financial values."
        )
        # we have at least one instance of duplicated 4 digit object classes so aggregate the financial values together
        db_cursor.execute(combine_dupe_oc, [submission_id])

    data = dictfetchall(db_cursor)
    return data


def load_file_b(submission_attributes, prg_act_obj_cls_data, db_cursor):
    """ Process and load file B broker data (aka TAS balances by program activity and object class). """
    reverse = re.compile(r"(_(cpe|fyb)$)|^transaction_obligated_amount$")

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}

    bulk_treasury_appropriation_account_tas_lookup(prg_act_obj_cls_data, db_cursor)

    save_manager = BulkCreateManager(FinancialAccountsByProgramActivityObjectClass)
    for row in prg_act_obj_cls_data:
        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(row.get("tas_id"))
        if treasury_account is None:
            update_skipped_tas(row, tas_rendering_label, skipped_tas)
            continue

        # get the corresponding account balances row (aka "File A" record)
        account_balances = AppropriationAccountBalances.objects.get(
            treasury_account_identifier=treasury_account, submission_id=submission_attributes.submission_id
        )

        financial_by_prg_act_obj_cls = FinancialAccountsByProgramActivityObjectClass()

        value_map = {
            "submission": submission_attributes,
            "reporting_period_start": submission_attributes.reporting_period_start,
            "reporting_period_end": submission_attributes.reporting_period_end,
            "treasury_account": treasury_account,
            "appropriation_account_balances": account_balances,
            "object_class": get_object_class(row["object_class"], row["by_direct_reimbursable_fun"]),
            "program_activity": get_program_activity(row, submission_attributes),
            "disaster_emergency_fund": get_disaster_emergency_fund(row),
        }

        save_manager.append(
            load_data_into_model(financial_by_prg_act_obj_cls, row, value_map=value_map, save=False, reverse=reverse)
        )

    save_manager.save_stragglers()

    for key in skipped_tas:
        logger.info(f"Skipped {skipped_tas[key]['count']:,} rows due to missing TAS: {key}")

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info(f"Skipped a total of {total_tas_skipped:,} TAS rows for File B")
