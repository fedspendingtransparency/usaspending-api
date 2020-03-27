import logging
import re
import signal

from datetime import datetime
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import transaction

from usaspending_api.accounts.models import (
    AppropriationAccountBalances,
    AppropriationAccountBalancesQuarterly,
    TreasuryAppropriationAccount,
    FederalAccount,
)
from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.helpers import get_fiscal_quarter, get_previous_submission
from usaspending_api.etl.management import load_base
from usaspending_api.etl.management.helpers.load_submission import (
    CertifiedAwardFinancial,
    get_object_class,
    get_or_create_program_activity,
)
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass,
    TasProgramActivityObjectClassQuarterly,
)
from usaspending_api.references.helpers import retrive_agency_name_from_code
from usaspending_api.submissions.models import SubmissionAttributes


# This dictionary prevent redundant calls when associating accounts to TAS and Federal Accounts
TAS_ID_TO_ACCOUNT = {}


# Lists to store for update_awards and update_procurement_awards
AWARD_UPDATE_ID_LIST = []

logger = logging.getLogger("console")


class Command(load_base.Command):
    """
    This command will load a single submission from the DATA Act broker. If we've already loaded the specified broker
    submission, this command will remove the existing records before loading them again.
    """

    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable \
                must set so we can pull submission data from their db."

    def add_arguments(self, parser):
        parser.add_argument("submission_id", nargs=1, help="the data broker submission id to load", type=int)
        super(Command, self).add_arguments(parser)

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):
        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        submission_id = options["submission_id"][0]

        logger.info("Getting submission {} from broker...".format(submission_id))
        db_cursor.execute("SELECT * FROM submission WHERE submission_id = %s", [submission_id])

        submission_data = dictfetchall(db_cursor)
        logger.info("Finished getting submission {} from broker".format(submission_id))

        if len(submission_data) == 0:
            raise CommandError("Could not find submission with id " + str(submission_id))
        elif len(submission_data) > 1:
            raise CommandError("Found multiple submissions with id " + str(submission_id))

        submission_data = submission_data[0].copy()
        broker_submission_id = submission_data["submission_id"]
        del submission_data["submission_id"]  # We use broker_submission_id, submission_id is our own PK
        submission_attributes = get_submission_attributes(broker_submission_id, submission_data)

        logger.info("Getting File A data")
        db_cursor.execute("SELECT * FROM certified_appropriation WHERE submission_id = %s", [submission_id])
        appropriation_data = dictfetchall(db_cursor)
        logger.info(
            "Acquired File A (appropriation) data for "
            + str(submission_id)
            + ", there are "
            + str(len(appropriation_data))
            + " rows."
        )
        logger.info("Loading File A data")
        start_time = datetime.now()
        load_file_a(submission_attributes, appropriation_data, db_cursor)
        logger.info("Finished loading File A data, took {}".format(datetime.now() - start_time))

        logger.info("Getting File B data")
        prg_act_obj_cls_data = get_file_b(submission_attributes, db_cursor)
        logger.info(
            "Acquired File B (program activity object class) data for "
            + str(submission_id)
            + ", there are "
            + str(len(prg_act_obj_cls_data))
            + " rows."
        )
        logger.info("Loading File B data")
        start_time = datetime.now()
        load_file_b(submission_attributes, prg_act_obj_cls_data, db_cursor)
        logger.info("Finished loading File B data, took {}".format(datetime.now() - start_time))

        logger.info("Getting File C data")
        certified_award_financial = CertifiedAwardFinancial(submission_attributes)
        logger.info(
            f"Acquired File C (award financial) data for {submission_id}, "
            f"there are {certified_award_financial.count} rows."
        )
        logger.info("Loading File C data")
        start_time = datetime.now()
        load_file_c(submission_attributes, db_cursor, certified_award_financial)
        logger.info("Finished loading File C data, took {}".format(datetime.now() - start_time))

        # Once all the files have been processed, run any global cleanup/post-load tasks.
        # Cleanup not specific to this submission is run in the `.handle` method
        logger.info("Successfully loaded broker submission {}.".format(options["submission_id"][0]))


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
        return None, None, "Account number {} not found in Broker".format(tas_lookup_id)

    tas_rendering_label = TreasuryAppropriationAccount.generate_tas_rendering_label(
        ata=tas_data[0]["allocation_transfer_agency"],
        aid=tas_data[0]["agency_identifier"],
        typecode=tas_data[0]["availability_type_code"],
        bpoa=tas_data[0]["beginning_period_of_availa"],
        epoa=tas_data[0]["ending_period_of_availabil"],
        mac=tas_data[0]["main_account_code"],
        sub=tas_data[0]["sub_account_code"],
    )

    federal_account_main_code = tas_data[0]["main_account_code"]
    federal_account_agency = tas_data[0]["agency_identifier"]

    TAS_ID_TO_ACCOUNT[tas_lookup_id] = (
        FederalAccount.objects.filter(
            main_account_code=federal_account_main_code, agency_identifier=federal_account_agency
        ).first(),
        TreasuryAppropriationAccount.objects.filter(tas_rendering_label=tas_rendering_label).first(),
        tas_rendering_label,
    )

    return TAS_ID_TO_ACCOUNT[tas_lookup_id]


def get_submission_attributes(broker_submission_id, submission_data):
    """
    For a specified broker submission, return the existing corresponding usaspending submission record or create and
    return a new one.
    """
    # check if we already have an entry for this broker submission id; if not, create one
    submission_attributes, created = SubmissionAttributes.objects.get_or_create(
        broker_submission_id=broker_submission_id
    )

    if created:
        # this is the first time we're loading this broker submission
        logger.info("Creating broker submission id {}".format(broker_submission_id))

    else:
        # we've already loaded this broker submission, so delete it before reloading if there's another submission that
        # references this one as a "previous submission" do not proceed.
        # TODO: now that we're chaining submissions together, get clarification on what should happen when a submission
        # in the middle of the chain is deleted

        TasProgramActivityObjectClassQuarterly.refresh_downstream_quarterly_numbers(submission_attributes.submission_id)

        logger.info("Broker submission id {} already exists. It will be deleted.".format(broker_submission_id))
        call_command("rm_submission", broker_submission_id)

    logger.info("Merging CGAC and FREC columns")
    submission_data["toptier_code"] = (
        submission_data["cgac_code"] if submission_data["cgac_code"] else submission_data["frec_code"]
    )
    submission_data["reporting_agency_name"] = retrive_agency_name_from_code(submission_data["toptier_code"])

    # Find the previous submission for this CGAC and fiscal year (if there is one)
    previous_submission = get_previous_submission(
        submission_data["toptier_code"],
        submission_data["reporting_fiscal_year"],
        submission_data["reporting_fiscal_period"],
    )

    # if another submission lists the previous submission as its previous submission, set to null and update later
    potential_conflicts = []
    if previous_submission:
        potential_conflicts = SubmissionAttributes.objects.filter(previous_submission=previous_submission)
        if potential_conflicts:
            logger.info("==== ATTENTION! Previous Submission ID Conflict Detected ====")
            for conflict in potential_conflicts:
                logger.info(
                    "Temporarily setting {}'s Previous Submission ID from {} to null".format(
                        conflict, previous_submission.submission_id
                    )
                )
                conflict.previous_submission = None
                conflict.save()

    # Update and save submission attributes
    field_map = {
        "reporting_period_start": "reporting_start_date",
        "reporting_period_end": "reporting_end_date",
        "quarter_format_flag": "is_quarter_format",
    }

    # Create our value map - specific data to load
    value_map = {
        "broker_submission_id": broker_submission_id,
        "reporting_fiscal_quarter": get_fiscal_quarter(submission_data["reporting_fiscal_period"]),
        "previous_submission": previous_submission,
        # pull in broker's last update date to use as certified date
        "certified_date": submission_data["updated_at"].date()
        if type(submission_data["updated_at"]) == datetime
        else None,
    }

    new_submission = load_data_into_model(
        submission_attributes, submission_data, field_map=field_map, value_map=value_map, save=True
    )

    # If there were any submissions which were temporarily modified, reassign the submission
    for conflict in potential_conflicts:
        remapped_previous = get_previous_submission(
            conflict.toptier_code, conflict.reporting_fiscal_year, conflict.reporting_fiscal_period
        )
        logger.info(
            "New Previous Submission ID for Submission ID {} permanently mapped to {} ".format(
                conflict.submission_id, remapped_previous
            )
        )
        conflict.previous_submission = remapped_previous
        conflict.save()

    return new_submission


def load_file_a(submission_attributes, appropriation_data, db_cursor):
    """
    Process and load file A broker data (aka TAS balances, aka appropriation account balances).
    """
    reverse = re.compile("gross_outlay_amount_by_tas_cpe")

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}

    # Create account objects
    for row in appropriation_data:

        # Check and see if there is an entry for this TAS
        federal_account, treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
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
            "federal_account": federal_account,
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

    # Insert File A quarterly numbers for this submission
    AppropriationAccountBalancesQuarterly.insert_quarterly_numbers(submission_attributes.submission_id)

    for key in skipped_tas:
        logger.info("Skipped %d rows due to missing TAS: %s", skipped_tas[key]["count"], key)

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info("Skipped a total of {} TAS rows for File A".format(total_tas_skipped))


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
    submission_id = submission_attributes.broker_submission_id

    # does this file B have the dupe object class edge case?
    check_dupe_oc = (
        "SELECT count(*) "
        "FROM certified_object_class_program_activity "
        "WHERE submission_id = %s "
        "AND length(object_class) = 4 "
        "GROUP BY tas_id, program_activity_code, object_class "
        "HAVING COUNT(*) > 1"
    )
    db_cursor.execute(check_dupe_oc, [submission_id])
    dupe_oc_count = len(dictfetchall(db_cursor))

    if dupe_oc_count == 0:
        # there are no object class duplicates, so proceed as usual
        db_cursor.execute(
            "SELECT * FROM certified_object_class_program_activity WHERE submission_id = %s", [submission_id]
        )
    else:
        # file b contains at least one case of duplicate 4 digit object classes for the same program activity/tas,
        # so combine the records in question
        combine_dupe_oc = (
            "SELECT  "
            "submission_id, "
            "job_id, "
            "agency_identifier, "
            "allocation_transfer_agency, "
            "availability_type_code, "
            "beginning_period_of_availa, "
            "ending_period_of_availabil, "
            "main_account_code, "
            "RIGHT(object_class, 3) AS object_class, "
            "CASE WHEN length(object_class) = 4 AND LEFT(object_class, 1) = '1' THEN 'D' "
            "WHEN length(object_class) = 4 AND LEFT(object_class, 1) = '2' THEN 'R' "
            "ELSE by_direct_reimbursable_fun END AS by_direct_reimbursable_fun, "
            "tas, "
            "tas_id, "
            "program_activity_code, "
            "program_activity_name, "
            "sub_account_code, "
            "SUM(deobligations_recov_by_pro_cpe) AS deobligations_recov_by_pro_cpe, "
            "SUM(gross_outlay_amount_by_pro_cpe) AS gross_outlay_amount_by_pro_cpe, "
            "SUM(gross_outlay_amount_by_pro_fyb) AS gross_outlay_amount_by_pro_fyb, "
            "SUM(gross_outlays_delivered_or_cpe) AS gross_outlays_delivered_or_cpe, "
            "SUM(gross_outlays_delivered_or_fyb) AS gross_outlays_delivered_or_fyb, "
            "SUM(gross_outlays_undelivered_cpe) AS gross_outlays_undelivered_cpe, "
            "SUM(gross_outlays_undelivered_fyb) AS gross_outlays_undelivered_fyb, "
            "SUM(obligations_delivered_orde_cpe) AS obligations_delivered_orde_cpe, "
            "SUM(obligations_delivered_orde_fyb) AS obligations_delivered_orde_fyb, "
            "SUM(obligations_incurred_by_pr_cpe) AS obligations_incurred_by_pr_cpe, "
            "SUM(obligations_undelivered_or_cpe) AS obligations_undelivered_or_cpe, "
            "SUM(obligations_undelivered_or_fyb) AS obligations_undelivered_or_fyb, "
            "SUM(ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe, "
            "SUM(ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb, "
            "SUM(ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe, "
            "SUM(ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb, "
            "SUM(ussgl483100_undelivered_or_cpe) AS ussgl483100_undelivered_or_cpe, "
            "SUM(ussgl483200_undelivered_or_cpe) AS ussgl483200_undelivered_or_cpe, "
            "SUM(ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe, "
            "SUM(ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe, "
            "SUM(ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe, "
            "SUM(ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe, "
            "SUM(ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe, "
            "SUM(ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb, "
            "SUM(ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe, "
            "SUM(ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe, "
            "SUM(ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb, "
            "SUM(ussgl493100_delivered_orde_cpe) AS ussgl493100_delivered_orde_cpe, "
            "SUM(ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe, "
            "SUM(ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe, "
            "SUM(ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe, "
            "SUM(ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe "
            "FROM certified_object_class_program_activity "
            "WHERE submission_id = %s "
            "GROUP BY  "
            "submission_id, "
            "job_id, "
            "agency_identifier, "
            "allocation_transfer_agency, "
            "availability_type_code, "
            "beginning_period_of_availa, "
            "ending_period_of_availabil, "
            "main_account_code, "
            "RIGHT(object_class, 3), "
            "CASE WHEN length(object_class) = 4 AND LEFT(object_class, 1) = '1' THEN 'D' "
            "WHEN length(object_class) = 4 AND LEFT(object_class, 1) = '2' THEN 'R' "
            "ELSE by_direct_reimbursable_fun END, "
            "program_activity_code, "
            "program_activity_name, "
            "sub_account_code, "
            "tas, "
            "tas_id"
        )
        logger.info(
            "Found {} duplicated File B 4 digit object codes in submission {}. "
            "Aggregating financial values.".format(dupe_oc_count, submission_id)
        )
        # we have at least one instance of duplicated 4 digit object classes so aggregate the financial values together
        db_cursor.execute(combine_dupe_oc, [submission_id])

    data = dictfetchall(db_cursor)
    return data


def load_file_b(submission_attributes, prg_act_obj_cls_data, db_cursor):
    """
    Process and load file B broker data (aka TAS balances by program activity and object class).
    """
    reverse = re.compile(r"(_(cpe|fyb)$)|^transaction_obligated_amount$")

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}

    test_counter = 0
    for row in prg_act_obj_cls_data:
        test_counter += 1
        account_balances = None
        try:
            # Check and see if there is an entry for this TAS
            federal_account, treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
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
            "federal_account": federal_account,
            "treasury_account": treasury_account,
            "appropriation_account_balances": account_balances,
            "object_class": get_object_class(row["object_class"], row["by_direct_reimbursable_fun"]),
            "program_activity": get_or_create_program_activity(row, submission_attributes),
        }

        load_data_into_model(financial_by_prg_act_obj_cls, row, value_map=value_map, save=True, reverse=reverse)

    # Insert File B quarterly numbers for this submission
    TasProgramActivityObjectClassQuarterly.insert_quarterly_numbers(submission_attributes.submission_id)

    FinancialAccountsByProgramActivityObjectClass.populate_final_of_fy()

    for key in skipped_tas:
        logger.info("Skipped %d rows due to missing TAS: %s", skipped_tas[key]["count"], key)

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info("Skipped a total of {} TAS rows for File B".format(total_tas_skipped))


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
            logger.info(
                "C File Load: Loading row {} of {} ({})".format(
                    str(index), str(total_rows), datetime.now() - start_time
                )
            )

        upper_case_dict_values(row)

        # Check and see if there is an entry for this TAS
        federal_account, treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(
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
            "federal_account": federal_account,
            "treasury_account": treasury_account,
            "object_class": row.get("object_class"),
            "program_activity": row.get("program_activity"),
        }

        # Still using the cpe|fyb regex compiled above for reverse
        load_data_into_model(award_financial_data, row, value_map=value_map_faba, save=True, reverse=reverse)

    for key in skipped_tas:
        logger.info("Skipped %d rows due to missing TAS: %s", skipped_tas[key]["count"], key)

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info("Skipped a total of {} TAS rows for File C".format(total_tas_skipped))

    return [award.id for award in awards_touched if award]
