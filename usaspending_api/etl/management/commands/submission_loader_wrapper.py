import logging
import datetime

from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command


from usaspending_api.common.validator.helpers import validate_boolean
from usaspending_api.common.helpers.generic_helper import create_full_time_periods

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):

    # Give it a fiscal year and a quarter. Will list missing subs/agencies and their recent certified dates.
    def add_arguments(self, parser):
        parser.add_argument('all_time', nargs=1, help='all quarters and fiscal years.', type=strtobool)
        parser.add_argument('ids' nargs=1, help='comma-separated list of ids to load', typ=str)

    def strtobool(arg):
        try:
            return validate_boolean(arg)
        except InvalidParameterException:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    def handle(self, *args, **options):

        if options['all_time'][0]:
            now = datetime.datetime.now()
            start = datetime.datetime(1,1,2017,0,0,0)
            time_periods = create_full_time_periods(start,now,"q",{})
            all_periods = [(period['time_period']['fy'], period['time_period']['q']) for period in time_periods]
            for idx, val in enumerate(all_periods):
                fy = val[0]
                q = val[1]
                try:
                    logger.info('Running submission load for fy {}, quarter {}...'.format)(fy, q)
                    call_command('load_multiple_submissions',"{} {}".format(fy, q))
                except CommandError:
                    logger.info('Error with fy/q combination')
                    continue
        else if options['ids'][0]:
            ids = options['ids'][0].split(",")
            logger.info("Running submission load for submitted ids...")
            for idx in ids:
                 try:
                    call_command('load_submission', '--noclean', '--nosubawards', idx)
                except CommandError:
                    logger.info('Skipping submission ID {} due to CommandError (bad ID)'.format(idx))
                    continue

