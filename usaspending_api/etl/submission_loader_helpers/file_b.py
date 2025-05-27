import logging
import re

from collections import defaultdict

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.etl.broker_etl_helpers import dictfetchall
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
        from        published_object_class_program_activity
        where       submission_id = %s and length(object_class) = 4
        group by    account_num, program_activity_code, object_class, upper(disaster_emergency_fund_code),
                    upper(prior_year_adjustment), upper(program_activity_reporting_key)
        having      count(*) > 1
    """
    db_cursor.execute(check_dupe_oc, [submission_id])
    dupe_oc_count = len(dictfetchall(db_cursor))

    columns = {
        "raw": [
            "submission_id",
            "job_id",
            "agency_identifier",
            "allocation_transfer_agency",
            "availability_type_code",
            "beginning_period_of_availa",
            "ending_period_of_availabil",
            "main_account_code",
            "by_direct_reimbursable_fun",
            "program_activity_code",
            "program_activity_name",
            "sub_account_code",
            "tas",
            "account_num",
        ],
        "left": ["object_class"],
        "upper": ["prior_year_adjustment", "program_activity_reporting_key", "disaster_emergency_fund_code"],
        "numeric": [
            "deobligations_recov_by_pro_cpe",
            "gross_outlay_amount_by_pro_cpe",
            "gross_outlay_amount_by_pro_fyb",
            "gross_outlays_delivered_or_cpe",
            "gross_outlays_delivered_or_fyb",
            "gross_outlays_undelivered_cpe",
            "gross_outlays_undelivered_fyb",
            "obligations_delivered_orde_cpe",
            "obligations_delivered_orde_fyb",
            "obligations_incurred_by_pr_cpe",
            "obligations_undelivered_or_cpe",
            "obligations_undelivered_or_fyb",
            "ussgl480100_undelivered_or_cpe",
            "ussgl480100_undelivered_or_fyb",
            "ussgl480110_rein_undel_ord_cpe",
            "ussgl480200_undelivered_or_cpe",
            "ussgl480200_undelivered_or_fyb",
            "ussgl483100_undelivered_or_cpe",
            "ussgl483200_undelivered_or_cpe",
            "ussgl487100_downward_adjus_cpe",
            "ussgl487200_downward_adjus_cpe",
            "ussgl488100_upward_adjustm_cpe",
            "ussgl488200_upward_adjustm_cpe",
            "ussgl490100_delivered_orde_cpe",
            "ussgl490100_delivered_orde_fyb",
            "ussgl490110_rein_deliv_ord_cpe",
            "ussgl490200_delivered_orde_cpe",
            "ussgl490800_authority_outl_cpe",
            "ussgl490800_authority_outl_fyb",
            "ussgl493100_delivered_orde_cpe",
            "ussgl497100_downward_adjus_cpe",
            "ussgl497200_downward_adjus_cpe",
            "ussgl498100_upward_adjustm_cpe",
            "ussgl498200_upward_adjustm_cpe",
        ],
    }

    raw_cols = ",".join([col for col in columns["raw"]])
    left_cols = ",".join([f"left({col}, 3)" for col in columns["left"]])
    upper_cols = ",".join([f"upper({col})" for col in columns["upper"]])
    numeric_cols = ",".join([col for col in columns["numeric"]])

    left_cols_with_alias = ",".join([f"left({col}, 3) as {col}" for col in columns["left"]])
    upper_cols_with_alias = ",".join([f"upper({col}) as {col}" for col in columns["upper"]])
    numeric_sum_cols_with_alias = ",".join(f"sum({col}) as {col}" for col in columns["numeric"])

    if dupe_oc_count == 0:
        # there are no object class duplicates, so proceed as usual
        db_cursor.execute(
            f"""
            select
                {raw_cols},
                {left_cols_with_alias},
                {upper_cols_with_alias},
                {numeric_cols}
            from
               published_object_class_program_activity
            where
               submission_id = %s
        """,
            [submission_id],
        )
    else:
        # file b contains at least one case of duplicate 4 digit object classes for the same program activity/tas,
        # so combine the records in question
        combine_dupe_oc = f"""
            select
                {raw_cols},
                {left_cols_with_alias},
                {upper_cols_with_alias},
                {numeric_sum_cols_with_alias}
            from
                published_object_class_program_activity
            where
                submission_id = %s
            group by
                {raw_cols},
                {left_cols},
                {upper_cols}
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
    """Process and load file B broker data (aka TAS balances by program activity and object class)."""
    reverse = re.compile(r"(_(cpe|fyb)$)|^transaction_obligated_amount$")
    skipped_tas = defaultdict(int)  # tracks count of rows skipped due to "missing" TAS
    bulk_treasury_appropriation_account_tas_lookup(prg_act_obj_cls_data, db_cursor)

    save_manager = BulkCreateManager(FinancialAccountsByProgramActivityObjectClass)
    for row in prg_act_obj_cls_data:
        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(row.get("account_num"))
        if treasury_account is None:
            skipped_tas[tas_rendering_label] += 1
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

    for tas, count in skipped_tas.items():
        logger.info(f"Skipped {count:,} rows due to {tas}")

    total_tas_skipped = sum([count for count in skipped_tas.values()])

    if total_tas_skipped > 0:
        logger.info(f"SKIPPED {total_tas_skipped:,} ROWS of File B (missing TAS)")
    else:
        logger.info("All File B records in Broker loaded into USAspending")
