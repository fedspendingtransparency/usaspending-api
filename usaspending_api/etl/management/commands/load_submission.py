import logging
import re
import signal

from datetime import datetime
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import transaction
from usaspending_api.accounts.models import AppropriationAccountBalances, TreasuryAppropriationAccount
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.helpers import get_fiscal_quarter
from usaspending_api.etl.management import load_base
from usaspending_api.etl.management.helpers.load_submission import (
    CertifiedAwardFinancial,
    defc_exists,
    get_object_class,
    get_or_create_program_activity,
    get_publish_history_table,
    get_disaster_emergency_fund,
)
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.helpers import retrive_agency_name_from_code
from usaspending_api.submissions.models import SubmissionAttributes


# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't keep hitting the databroker DB for
# account data
TAS_ID_TO_ACCOUNT = {}

logger = logging.getLogger("script")


class Command(load_base.Command):
    """
    This command will load a single submission from the DATA Act broker. If we've already loaded the specified
    broker submission, this command will remove the existing records before loading them again.
    """

    help = (
        "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable "
        "must set so we can pull submission data from their db."
    )

    def add_arguments(self, parser):
        parser.add_argument("submission_id", help="Broker submission_id to load", type=int)
        super(Command, self).add_arguments(parser)

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):
        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        submission_id = options["submission_id"]

        logger.info(f"Getting submission {submission_id} from Broker...")
        db_cursor.execute(
            f"""
                select
                    s.submission_id,
                    (
                        select  max(updated_at)
                        from    {get_publish_history_table()}
                        where   submission_id = s.submission_id
                    )::timestamptz as published_date,
                    (
                        select  max(updated_at)
                        from    certify_history
                        where   submission_id = s.submission_id
                    )::timestamptz as certified_date,
                    coalesce(s.cgac_code, s.frec_code) as toptier_code,
                    s.reporting_start_date,
                    s.reporting_end_date,
                    s.reporting_fiscal_year,
                    s.reporting_fiscal_period,
                    s.is_quarter_format,
                    s.d2_submission,
                    s.publish_status_id
                from
                    submission as s
                where
                    s.submission_id = {submission_id}
            """
        )

        submission_data = dictfetchall(db_cursor)
        logger.info(f"Finished getting submission {submission_id} from Broker")

        if len(submission_data) == 0:
            raise CommandError(f"Could not find submission with id {submission_id}")
        elif len(submission_data) > 1:
            raise CommandError(f"Found multiple submissions with id {submission_id}")

        submission_data = submission_data[0]

        if submission_data["publish_status_id"] not in (2, 3):
            raise RuntimeError(f"publish_status_id {submission_data['publish_status_id']} is not allowed")
        if submission_data["d2_submission"] is not False:
            raise RuntimeError(f"d2_submission {submission_data['d2_submission']} is not allowed")

        submission_attributes = get_submission_attributes(submission_id, submission_data)

        logger.info("Getting File A data")
        db_cursor.execute(f"SELECT * FROM certified_appropriation WHERE submission_id = {submission_id}")
        appropriation_data = dictfetchall(db_cursor)
        logger.info(
            f"Acquired File A (appropriation) data for {submission_id}, " f"there are {len(appropriation_data):,} rows."
        )
        logger.info("Loading File A data")
        start_time = datetime.now()
        load_file_a(submission_attributes, appropriation_data, db_cursor)
        logger.info(f"Finished loading File A data, took {datetime.now() - start_time}")

        logger.info("Getting File B data")
        prg_act_obj_cls_data = get_file_b(submission_attributes, db_cursor)
        logger.info(
            f"Acquired File B (program activity object class) data for {submission_id}, "
            f"there are {len(prg_act_obj_cls_data):,} rows."
        )
        logger.info("Loading File B data")
        start_time = datetime.now()
        load_file_b(submission_attributes, prg_act_obj_cls_data, db_cursor)
        logger.info(f"Finished loading File B data, took {datetime.now() - start_time}")

        logger.info("Getting File C data")
        certified_award_financial = CertifiedAwardFinancial(submission_attributes)
        logger.info(
            f"Acquired File C (award financial) data for {submission_id}, "
            f"there are {certified_award_financial.count:,} rows."
        )
        logger.info("Loading File C data")
        start_time = datetime.now()
        load_file_c(submission_attributes, db_cursor, certified_award_financial)
        logger.info(f"Finished loading File C data, took {datetime.now() - start_time}")

        # Once all the files have been processed, run any global cleanup/post-load tasks.
        # Cleanup not specific to this submission is run in the `.handle` method
        logger.info(f"Successfully loaded submission {submission_id}.")


def update_skipped_tas(row, tas_rendering_label, skipped_tas):
    if tas_rendering_label not in skipped_tas:
        skipped_tas[tas_rendering_label] = {}
        skipped_tas[tas_rendering_label]["count"] = 1
        skipped_tas[tas_rendering_label]["rows"] = [row["row_number"]]
    else:
        skipped_tas[tas_rendering_label]["count"] += 1
        skipped_tas[tas_rendering_label]["rows"] += [row["row_number"]]


def get_treasury_appropriation_account_tas_lookup(tas_lookup_id, db_cursor):
    """Get the matching TAS object from the broker database and save it to our running list."""
    if tas_lookup_id in TAS_ID_TO_ACCOUNT:
        return TAS_ID_TO_ACCOUNT[tas_lookup_id]
    # Checks the broker DB tas_lookup table for the tas_id and returns the matching TAS object in the datastore
    db_cursor.execute(
        "SELECT * FROM tas_lookup WHERE (financial_indicator2 <> 'F' OR financial_indicator2 IS NULL) "
        "AND account_num = %s",
        [tas_lookup_id],
    )
    tas_data = dictfetchall(db_cursor)

    if tas_data is None or len(tas_data) == 0:
        return None, f"Account number {tas_lookup_id} not found in Broker"

    tas_rendering_label = TreasuryAppropriationAccount.generate_tas_rendering_label(
        ata=tas_data[0]["allocation_transfer_agency"],
        aid=tas_data[0]["agency_identifier"],
        typecode=tas_data[0]["availability_type_code"],
        bpoa=tas_data[0]["beginning_period_of_availa"],
        epoa=tas_data[0]["ending_period_of_availabil"],
        mac=tas_data[0]["main_account_code"],
        sub=tas_data[0]["sub_account_code"],
    )

    TAS_ID_TO_ACCOUNT[tas_lookup_id] = (
        TreasuryAppropriationAccount.objects.filter(tas_rendering_label=tas_rendering_label).first(),
        tas_rendering_label,
    )
    return TAS_ID_TO_ACCOUNT[tas_lookup_id]


def get_submission_attributes(submission_id, submission_data):
    """
    For a specified broker submission, return the existing corresponding usaspending submission record or
    create and return a new one.
    """
    # check if we already have an entry for this submission id; if not, create one
    submission_attributes, created = SubmissionAttributes.objects.get_or_create(submission_id=submission_id)

    if created:
        # this is the first time we're loading this submission
        logger.info(f"Creating submission {submission_id}")

    else:
        # we've already loaded this submission, so delete it before reloading
        logger.info(f"Submission {submission_id} already exists. It will be deleted.")
        call_command("rm_submission", submission_id)

    submission_data["reporting_agency_name"] = retrive_agency_name_from_code(submission_data["toptier_code"])

    # Update and save submission attributes
    field_map = {
        "reporting_period_start": "reporting_start_date",
        "reporting_period_end": "reporting_end_date",
        "quarter_format_flag": "is_quarter_format",
    }

    # Create our value map - specific data to load
    value_map = {"reporting_fiscal_quarter": get_fiscal_quarter(submission_data["reporting_fiscal_period"])}

    new_submission = load_data_into_model(
        submission_attributes, submission_data, field_map=field_map, value_map=value_map, save=True
    )

    return new_submission


def load_file_a(submission_attributes, appropriation_data, db_cursor):
    """ Process and load file A broker data (aka TAS balances, aka appropriation account balances). """
    reverse = re.compile("gross_outlay_amount_by_tas_cpe")

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}

    # Create account objects
    for row in appropriation_data:

        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
            row.get("tas_id"), db_cursor
        )
        if treasury_account is None:
            update_skipped_tas(row, tas_rendering_label, skipped_tas)
            continue

        # Now that we have the account, we can load the appropriation balances
        # TODO: Figure out how we want to determine what row is overriden by what row
        # If we want to correlate, the following attributes are available in the data broker data that might be useful:
        # appropriation_id, row_number appropriation_balances = somethingsomething get appropriation balances...
        appropriation_balances = AppropriationAccountBalances()

        value_map = {
            "treasury_account_identifier": treasury_account,
            "submission": submission_attributes,
            "reporting_period_start": submission_attributes.reporting_period_start,
            "reporting_period_end": submission_attributes.reporting_period_end,
        }

        field_map = {}

        load_data_into_model(
            appropriation_balances, row, field_map=field_map, value_map=value_map, save=True, reverse=reverse
        )

    AppropriationAccountBalances.populate_final_of_fy()

    for key in skipped_tas:
        logger.info(f"Skipped {skipped_tas[key]['count']:,} rows due to missing TAS: {key}")

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info(f"Skipped a total of {total_tas_skipped:,} TAS rows for File A")


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

    # ENABLE_CARES_ACT_FEATURES: Remove this once disaster_emergency_fund_code columns go live in Broker.
    defc_sql = ", disaster_emergency_fund_code" if defc_exists() else ""

    # does this file B have the dupe object class edge case?
    check_dupe_oc = f"""
        select      count(*)
        from        certified_object_class_program_activity
        where       submission_id = %s and length(object_class) = 4
        group by    tas_id, program_activity_code, object_class{defc_sql}
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
                sum(ussgl498200_upward_adjustm_cpe) as ussgl498200_upward_adjustm_cpe
                {defc_sql}
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
                tas_id
                {defc_sql}
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

    test_counter = 0
    for row in prg_act_obj_cls_data:
        test_counter += 1
        try:
            # Check and see if there is an entry for this TAS
            treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
                row.get("tas_id"), db_cursor
            )
            if treasury_account is None:
                update_skipped_tas(row, tas_rendering_label, skipped_tas)
                continue
        except Exception:  # TODO: What is this trying to catch, actually?
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
            "program_activity": get_or_create_program_activity(row, submission_attributes),
            "disaster_emergency_fund": get_disaster_emergency_fund(row),
        }
        load_data_into_model(financial_by_prg_act_obj_cls, row, value_map=value_map, save=True, reverse=reverse)

    FinancialAccountsByProgramActivityObjectClass.populate_final_of_fy()

    for key in skipped_tas:
        logger.info(f"Skipped {skipped_tas[key]['count']:,} rows due to missing TAS: {key}")

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info(f"Skipped a total of {total_tas_skipped:,} TAS rows for File B")


def find_matching_award(piid=None, parent_piid=None, fain=None, uri=None):
    """
        Check for a distinct award that matches based on the parameters provided

        :param piid: PIID associated with a contract
        :param parent_piid: Parent Award ID associated with a contract
        :param fain: FAIN associated with a financial assistance award
        :param uri: URI associated with a financial assistancw award
        :return: Award object containing an exact matched award OR None if no exact match found
    """
    filters = {"latest_transaction_id__isnull": False}

    if not (piid or fain or uri):
        return None

    # check piid and parent_piid
    if piid:
        filters["piid"] = piid
        if parent_piid:
            filters["parent_award_piid"] = parent_piid
    elif fain and not uri:
        # if only the fain is populated, filter on that
        filters["fain"] = fain
    elif not fain and uri:
        # if only the uri is populated, filter on that
        filters["uri"] = uri
    else:
        # if both fain and uri are populated, filter on fain first for an exact match. if no exact match found,
        # then try filtering on the uri
        filters["fain"] = fain
        fain_award_count = Award.objects.filter(**filters).count()

        if fain_award_count != 1:
            del filters["fain"]
            filters["uri"] = uri

    awards = Award.objects.filter(**filters).all()

    if len(awards) == 1:
        return awards[0]
    else:
        return None


def load_file_c(submission_attributes, db_cursor, certified_award_financial):
    """
    Process and load file C broker data.
    Note: this should run AFTER the D1 and D2 files are loaded because we try to join to those records to retrieve some
    additional information about the awarding sub-tier agency.
    """
    # this matches the file b reverse directive, but am repeating it here to ensure that we don't overwrite it as we
    # change up the order of file loading

    if certified_award_financial.count == 0:
        logger.warning("No File C (award financial) data found, skipping...")
        return

    reverse = re.compile(r"(_(cpe|fyb)$)|^transaction_obligated_amount$")

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}
    total_rows = certified_award_financial.count
    start_time = datetime.now()
    awards_touched = []

    for index, row in enumerate(certified_award_financial, 1):
        if not (index % 100):
            logger.info(f"C File Load: Loading row {index:,} of {total_rows:,} ({datetime.now() - start_time})")

        upper_case_dict_values(row)

        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
            row.get("tas_id"), db_cursor
        )
        if treasury_account is None:
            update_skipped_tas(row, tas_rendering_label, skipped_tas)
            continue

        # Find a matching transaction record, so we can use its subtier agency information to match to (or create) an
        # Award record.

        # Find the award that this award transaction belongs to. If it doesn't exist, create it.
        filters = {}
        if row.get("piid"):
            filters["piid"] = row.get("piid")
            filters["parent_piid"] = row.get("parent_award_id")
        else:
            if row.get("fain") and not row.get("uri"):
                filters["fain"] = row.get("fain")
            elif row.get("uri") and not row.get("fain"):
                filters["uri"] = row.get("uri")
            else:
                filters["fain"] = row.get("fain")
                filters["uri"] = row.get("uri")

        award = find_matching_award(**filters)

        if award:
            awards_touched += [award]

        award_financial_data = FinancialAccountsByAwards()

        value_map_faba = {
            "award": award,
            "submission": submission_attributes,
            "reporting_period_start": submission_attributes.reporting_period_start,
            "reporting_period_end": submission_attributes.reporting_period_end,
            "treasury_account": treasury_account,
            "object_class": row.get("object_class"),
            "program_activity": row.get("program_activity"),
            "disaster_emergency_fund": get_disaster_emergency_fund(row),
        }

        # Still using the cpe|fyb regex compiled above for reverse
        load_data_into_model(award_financial_data, row, value_map=value_map_faba, save=True, reverse=reverse)

    for key in skipped_tas:
        logger.info(f"Skipped {skipped_tas[key]['count']:,} rows due to missing TAS: {key}")

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info(f"Skipped a total of {total_tas_skipped:,} TAS rows for File C")

    return [award.id for award in awards_touched if award]
