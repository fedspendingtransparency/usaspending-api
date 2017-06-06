from datetime import datetime
import logging

from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.broker_etl_helpers import dictfetchall

from usaspending_api.etl.management import load_base

logger = logging.getLogger('console')


def to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d').date()


class Command(load_base.Command):
    """
    This command will load detached submissions from the DATA Act broker.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable must set so we can pull submission data from their db."

    def add_arguments(self, parser):

        super(Command, self).add_arguments(parser)
        parser.add_argument('--action_date_begin', type=to_date, default=None, help='First action_date to get - YYYY-MM-DD')
        parser.add_argument('--action_date_end', type=to_date, default=None, help='Last action_date to get - YYYY-MM-DD')
        parser.add_argument('--cgac', default=None, help='awarding toptier agency code')

    def handle_loading(self, db_cursor, *args, **options):

        submission_attributes = SubmissionAttributes()
        submission_attributes.usaspending_update = datetime.now()
        submission_attributes.save()

        # File D1
        sql = 'SELECT * FROM detached_award_procurement WHERE true'
        filter_values = []
        for (column, filter) in (
                ('action_date_begin', ' AND CAST(action_date AS DATE) >= %s'),  # what a performance-killer!
                ('action_date_end', ' AND CAST(action_date AS DATE) <= %s'),
                ('cgac', ' AND awarding_agency_code = %s')):
            if options[column]:
                sql += filter
                filter_values.append(options[column])
        db_cursor.execute(sql, filter_values)
        procurement_data = dictfetchall(db_cursor)
        logger.info('Acquired award procurement data for detached, there are ' + str(len(procurement_data)) + ' rows.')
        load_base.load_file_d1(submission_attributes, procurement_data, db_cursor, date_pattern='%Y-%m-%d %H:%M:%S')
