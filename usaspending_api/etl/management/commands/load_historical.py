from datetime import datetime
import logging

from django.utils.dateparse import parse_date

from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.broker_etl_helpers import dictfetchall

from usaspending_api.etl.management import load_base

logger = logging.getLogger('console')


class Command(load_base.Command):
    """
    This command will load detached submissions from the DATA Act broker.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable must set so we can pull submission data from their db."

    def add_arguments(self, parser):

        super(Command, self).add_arguments(parser)
        parser.add_argument('--contracts', action='store_true', help='Load contracts (not FA')
        parser.add_argument('--financial_assistance', action='store_true', help='Load financial assistance (not contracts)')
        parser.add_argument('--action_date_begin', type=parse_date, default=None, help='First action_date to get - YYYY-MM-DD')
        parser.add_argument('--action_date_end', type=parse_date, default=None, help='Last action_date to get - YYYY-MM-DD')
        parser.add_argument('--cgac', default=None)

    def handle_loading(self, db_cursor, *args, **options):

        submission_attributes = SubmissionAttributes()
        submission_attributes.usaspending_update = datetime.now()
        submission_attributes.save()

        if options['contracts'] and options['financial_assistance']:
            raise CommandError('Default is to load both contracts and financial_assistance')

        if not options['financial_assistance']:
            procurement_data = self.broker_data(db_cursor, 'detached_award_procurement', options)
            load_base.load_file_d1(submission_attributes, procurement_data, db_cursor, date_pattern='%Y-%m-%d %H:%M:%S')

        if not options['contracts']:
            assistance_data = self.broker_data(db_cursor, 'published_award_financial_assistance', options)
            load_base.load_file_d2(submission_attributes, assistance_data, db_cursor)

    def broker_data(self, db_cursor, table_name, options):
        filter_sql = []
        filter_values = []
        for (column, filter) in (
                ('action_date_begin', ' AND (action_date IS NOT NULL) AND CAST(action_date AS DATE) >= %s'),
                ('action_date_end', ' AND (action_date IS NOT NULL) AND CAST(action_date AS DATE) <= %s'),
                ('cgac', ' AND awarding_agency_code = %s'), ):
            if options[column]:
                filter_sql.append(filter)
                filter_values.append(options[column])
        filter_sql = "\n".join(filter_sql)

        sql = 'SELECT * FROM {} WHERE true {}'.format(table_name, filter_sql)
        db_cursor.execute(sql, filter_values)
        results = dictfetchall(db_cursor)
        logger.info('Acquired {}, there are {} rows.'.format(table_name, len(results)))
        return results
