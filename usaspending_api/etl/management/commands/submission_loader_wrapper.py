import logging
import datetime
import argparse

from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command


from usaspending_api.common.validator.helpers import validate_boolean
from usaspending_api.common.helpers.generic_helper import create_full_time_periods
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):

    def strtobool(arg):
        try:
            return validate_boolean(arg)
        except InvalidParameterException:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    def add_arguments(self, parser):
        parser.add_argument('--all_time', action="store_true", help='all quarters and fiscal years.')
        parser.add_argument('--ids', nargs=1, help='comma-separated list of ids to load', type=str)

    def handle(self, *args, **options):

        if options['all_time']:
            now = datetime.datetime.now()
            start = datetime.datetime(2017, 1, 1, 0, 0, 0)
            time_periods = create_full_time_periods(start, now, "q", {})
            all_periods = [(period['time_period']['fy'], period['time_period']['q']) for period in time_periods]
            for idx, val in enumerate(all_periods):
                fy = val[0]
                q = val[1]
                try:
                    logger.info('Running submission load for fy {}, quarter {}...'.format(fy, q))
                    call_command('load_multiple_submissions', int(fy), int(q))
                except CommandError as e:
                    logger.info('Error with fy/q combination')
                    logger.info(str(e))
                    continue
        elif options['ids'][0]:
            ids = options['ids'][0].split(",")
            logger.info("Running submission load for submitted ids...")
            for idx in ids:
                try:
                    call_command('load_submission', '--noclean', '--nosubawards', idx)
                except CommandError:
                    logger.info('Skipping submission ID {} due to CommandError (bad ID)'.format(idx))
                    continue
