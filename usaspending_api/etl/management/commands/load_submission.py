import logging
import signal

from datetime import datetime
from django.core.management.base import CommandError
from django.db import transaction
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management import load_base
from usaspending_api.etl.submission_loader_helpers.file_a import get_file_a, load_file_a
from usaspending_api.etl.submission_loader_helpers.file_b import get_file_b, load_file_b
from usaspending_api.etl.submission_loader_helpers.file_c import get_file_c, load_file_c
from usaspending_api.etl.submission_loader_helpers.final_of_fy import populate_final_of_fy
from usaspending_api.etl.submission_loader_helpers.program_activities import update_program_activities
from usaspending_api.etl.submission_loader_helpers.submission_attributes import (
    attempt_submission_update_only,
    get_submission_attributes,
)
from usaspending_api.references.helpers import retrive_agency_name_from_code

logger = logging.getLogger("script")


class Command(load_base.Command):
    """
    This command will load a single submission from Data Broker.  If we've already loaded
    the specified Broker submission, this command will either update or reload the submission.
    """

    submission_id = None
    file_c_chunk_size = 100000
    force_reload = False
    skip_final_of_fy_calculation = False
    db_cursor = None

    help = (
        "Loads a single submission from Data Broker. The DATA_BROKER_DATABASE_URL environment variable "
        "must set so we can pull submission data from their db."
    )

    def add_arguments(self, parser):
        parser.add_argument("submission_id", help="Broker submission_id to load", type=int)
        parser.add_argument(
            "--force-reload",
            action="store_true",
            help=(
                "Forces a full reload of the submission if it already exists in USAspending.  "
                "Without this flag, the script will attempt to perform an update only if possible.",
            ),
        )
        parser.add_argument(
            "--skip-final-of-fy-calculation",
            action="store_true",
            help=(
                "This is mainly designed to be used by the multiple_submission_loader.  Prevents "
                "the final_of_fy value from being recalculated for each submission that's loaded.",
            ),
        )
        parser.add_argument(
            "--skip-c-to-d-linkage",
            action="store_true",
            help=(
                "This flag skips the step to perform File C to D Linkages, which updates the "
                "`award_id` field on File C records. File C to D linkages also take place in "
                "subsequent Databricks steps in the pipeline and only takes place in this "
                "command for earlier data consistency. It can safely be skipped in the case of "
                "long running submissions.",
            ),
        )
        parser.add_argument(
            "--file-c-chunk-size",
            type=int,
            default=self.file_c_chunk_size,
            help=(
                "Controls the number of File C records processed in a single batch.  Theoretically, "
                "bigger should be faster... right up until you run out of memory.  Balance carefully."
            ),
        )
        super(Command, self).add_arguments(parser)

    def handle_loading(self, db_cursor, *args, **options):

        self.submission_id = options["submission_id"]
        self.force_reload = options["force_reload"]
        self.file_c_chunk_size = options["file_c_chunk_size"]
        self.skip_final_of_fy_calculation = options["skip_final_of_fy_calculation"]
        self.skip_c_to_d_linkage = options["skip_c_to_d_linkage"]
        self.db_cursor = db_cursor

        logger.info(f"Starting processing for submission {self.submission_id}...")

        # This has to occur outside of the transaction so we don't hang up other loaders that may be
        # running in parallel.  Worst case scenario of running it outside of the main transaction is
        # that is that we load some program activities that are never used due to the submission failing
        # to load and then being deleted from Broker.
        logger.info(f"Checking for new program activities created...")
        new_program_activities = update_program_activities(self.submission_id)
        logger.info(f"{new_program_activities:,} new program activities created")

        self.load_in_transaction()

    @transaction.atomic
    def load_in_transaction(self):
        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception("Received interrupt signal. Aborting...")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info(f"Getting submission {self.submission_id} from Broker...")
        submission_data = self.get_broker_submission()
        logger.info(f"Finished getting submission {self.submission_id} from Broker")
        submission_data = self.validate_submission_data(submission_data)
        agency_name = retrive_agency_name_from_code(submission_data["toptier_code"])
        logger.info(f"Submission {self.submission_id} belongs to {agency_name}")

        if not self.force_reload and attempt_submission_update_only(submission_data):
            logger.info(f"{self.submission_id} did not require a full reload.  Updated.")
            return

        submission_attributes = get_submission_attributes(self.submission_id, submission_data)

        logger.info("Getting File A data")
        appropriation_data = get_file_a(submission_attributes, self.db_cursor)
        logger.info(
            f"Acquired File A (appropriation) data for {self.submission_id}, there are {len(appropriation_data):,} rows."
        )
        logger.info("Loading File A data")
        start_time = datetime.now()
        load_file_a(submission_attributes, appropriation_data, self.db_cursor)
        logger.info(f"Finished loading File A data, took {datetime.now() - start_time}")

        logger.info("Getting File B data")
        prg_act_obj_cls_data = get_file_b(submission_attributes, self.db_cursor)
        logger.info(
            f"Acquired File B (program activity object class) data for {self.submission_id}, "
            f"there are {len(prg_act_obj_cls_data):,} rows."
        )
        logger.info("Loading File B data")
        start_time = datetime.now()
        load_file_b(submission_attributes, prg_act_obj_cls_data, self.db_cursor)
        logger.info(f"Finished loading File B data, took {datetime.now() - start_time}")

        logger.info("Getting File C data")
        published_award_financial = get_file_c(submission_attributes, self.db_cursor, self.file_c_chunk_size)
        logger.info(
            f"Acquired File C (award financial) data for {self.submission_id}, "
            f"there are {published_award_financial.count:,} rows."
        )
        logger.info("Loading File C data")
        start_time = datetime.now()
        load_file_c(submission_attributes, self.db_cursor, published_award_financial, self.skip_c_to_d_linkage)
        logger.info(f"Finished loading File C data, took {datetime.now() - start_time}")

        if self.skip_final_of_fy_calculation:
            logger.info("Skipping final_of_fy calculation as requested.")
        else:
            logger.info("Updating final_of_fy")
            start_time = datetime.now()
            populate_final_of_fy()
            logger.info(f"Finished updating final_of_fy, took {datetime.now() - start_time}")

        # Once all the files have been processed, run any global cleanup/post-load tasks.
        # Cleanup not specific to this submission is run in the `.handle` method
        logger.info(f"Successfully loaded submission {self.submission_id}.")

        logger.info("Committing transaction...")

    def get_broker_submission(self):
        self.db_cursor.execute(
            f"""
                with publish_certify_history as (
                    select
                        distinct_pairings.submission_id,
                        json_agg(
                            json_build_object(
                                'published_date', ph.updated_at::timestamptz,
                                'certified_date', ch.updated_at::timestamptz
                            )
                        ) AS history
                    from
                        (
                            select distinct
                                submission_id,
                                publish_history_id,
                                certify_history_id
                            from published_files_history
                            where submission_id = %s
                        ) as distinct_pairings
                    left outer join
                        publish_history as ph using (publish_history_id)
                    left outer join
                        certify_history as ch using (certify_history_id)
                    group by distinct_pairings.submission_id
                )
                select
                    s.submission_id,
                    (
                        select  max(updated_at)
                        from    publish_history
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
                    s.is_fabs,
                    s.publish_status_id,
                    pch.history
                from
                    submission as s
                inner join
                    publish_certify_history as pch using (submission_id)
            """,
            [self.submission_id],
        )

        return dictfetchall(self.db_cursor)

    def validate_submission_data(self, submission_data):
        if len(submission_data) == 0:
            raise CommandError(f"Could not find submission with id {self.submission_id}")
        elif len(submission_data) > 1:
            raise CommandError(f"Found multiple submissions with id {self.submission_id}")

        submission_data = submission_data[0]

        if submission_data["publish_status_id"] not in (2, 3):
            raise RuntimeError(f"publish_status_id {submission_data['publish_status_id']} is not allowed")
        if submission_data["is_fabs"] is not False:
            raise RuntimeError(f"is_fabs {submission_data['is_fabs']} is not allowed")

        return submission_data
