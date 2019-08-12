from collections import OrderedDict
import logging
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist

from usaspending_api.accounts.models import AppropriationAccountBalances, AppropriationAccountBalancesQuarterly
from usaspending_api.awards.models import (
    FinancialAccountsByAwards,
    TransactionNormalized,
    TransactionFPDS,
    TransactionFABS,
)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass,
    TasProgramActivityObjectClassQuarterly,
)
from usaspending_api.submissions.models import SubmissionAttributes

logger = logging.getLogger("console")


class Command(BaseCommand):
    """Return status about a specific broker submission id."""

    help = "Returns information about the specified data broker submission"
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        parser.add_argument("submission_id", nargs=1, help="a data broker submission id", type=int)

    def handle(self, *args, **options):
        # This will throw an exception and exit the command if the id doesn't exist
        try:
            submission = SubmissionAttributes.objects.get(broker_submission_id=options["submission_id"][0])
        except ObjectDoesNotExist:
            self.logger.error("Submission with broker id {} not found".format(options["submission_id"][0]))
            return

        stats = get_submission_stats(submission)
        logger.info(
            "Info for {}:\nreporting_period_start {}\nreporting_period_end {}\n"
            "quarter_format {}\nupdate_date {}".format(
                submission,
                submission.reporting_period_start,
                submission.reporting_period_end,
                submission.quarter_format_flag,
                submission.update_date,
            )
        )
        logger.info("\nModel counts:")
        for key, value in stats.items():
            logger.info("{}: {}".format(key, value))


def get_submission_stats(submission):
    """Collect information about a specified submission."""
    models = [
        AppropriationAccountBalances,
        AppropriationAccountBalancesQuarterly,
        FinancialAccountsByProgramActivityObjectClass,
        TasProgramActivityObjectClassQuarterly,
        TransactionNormalized,
        TransactionFABS,
        TransactionFPDS,
        FinancialAccountsByAwards,
    ]

    stats = [(m._meta.model_name, m.objects.filter(submission=submission).count()) for m in models]
    stats = OrderedDict(stats)
    return stats
