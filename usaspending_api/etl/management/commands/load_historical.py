from django.utils import timezone
import logging

from django.utils.dateparse import parse_date
from django.db import transaction
from django.core.management.base import CommandError

from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.broker_etl_helpers import dictfetchall

from usaspending_api.etl.management import load_base

logger = logging.getLogger("console")


def before_colon(txt):

    return txt.split(":", 1)[0].strip()


def preprocess_historical_d2_row(row):
    """Change values `code: full description` to simply `code` for certain row keys"""

    for key in ("awarding_sub_tier_agency_c", "business_types", "action_type", "assistance_type"):
        row[key] = before_colon(row[key])
    return row


class Command(load_base.Command):
    """
    This command will load detached submissions from the DATA Act broker.
    """

    help = (
        "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable "
        "must set so we can pull submission data from their db."
    )

    def add_arguments(self, parser):

        super(Command, self).add_arguments(parser)
        parser.add_argument("--contracts", action="store_true", help="Load contracts")
        parser.add_argument("--financial_assistance", action="store_true", help="Load financial assistance")
        parser.add_argument(
            "--action_date_begin", type=parse_date, default=None, help="First action_date to get - YYYY-MM-DD"
        )
        parser.add_argument(
            "--action_date_end", type=parse_date, default=None, help="Last action_date to get - YYYY-MM-DD"
        )
        parser.add_argument("--cgac", default=None)

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):

        submission_attributes = SubmissionAttributes()
        submission_attributes.usaspending_update = timezone.now()
        submission_attributes.save()

        if not options["contracts"] and not options["financial_assistance"]:
            raise CommandError("Must specify either --contracts, --financial_assistance, or both")

        if options["contracts"]:
            procurement_data = self.broker_data(db_cursor, "detached_award_procurement", options)
            load_base.load_file_d1(submission_attributes, procurement_data, db_cursor)

        if options["financial_assistance"]:
            assistance_data = self.broker_data(db_cursor, "published_award_financial_assistance", options)
            load_base.load_file_d2(
                submission_attributes, assistance_data, db_cursor, row_preprocessor=preprocess_historical_d2_row
            )

    def broker_data(self, db_cursor, table_name, options):
        """Applies user-selected filters and gets rows from appropriate broker-side table"""
        filter_sql = []
        filter_values = []
        for (column, filter) in (
            ("action_date_begin", " AND (action_date IS NOT NULL) AND CAST(action_date AS DATE) >= %s"),
            ("action_date_end", " AND (action_date IS NOT NULL) AND CAST(action_date AS DATE) <= %s"),
            ("cgac", " AND awarding_agency_code = %s"),
        ):
            if options[column]:
                filter_sql.append(filter)
                filter_values.append(options[column])
        filter_sql = "\n".join(filter_sql)

        sql = "SELECT * FROM {} WHERE true {}".format(table_name, filter_sql)
        db_cursor.execute(sql, filter_values)
        results = dictfetchall(db_cursor)
        logger.info("Acquired {}, there are {} rows.".format(table_name, len(results)))
        return results
