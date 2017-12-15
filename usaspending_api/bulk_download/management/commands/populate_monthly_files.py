import logging
import datetime
import json
import multiprocessing

from django.conf import settings
from django.core.management.base import BaseCommand
from usaspending_api.bulk_download.filestreaming import csv_selection
from usaspending_api.bulk_download.v2.views import BulkDownloadAwardsViewSet
from usaspending_api.common.helpers import generate_fiscal_year
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.bulk_download.models import BulkDownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.references.models import ToptierAgency, SubtierAgency
from usaspending_api.common.helpers import order_nested_object

# Logging
logger = logging.getLogger('console')

award_mappings = {
    'contracts': ['contracts'],
    'assistance': ['grants', 'direct_payments', 'loans', 'other_financial_assistance']
}

class Command(BaseCommand):
    # TODO: This function reveals most of bulk download relies on its REST origins.
    #       This and most of Bulk Download will definitely need to be refactored to address this.
    def bulk_download(self, file_name, award_levels, award_types=None, agency=None, sub_agency=None,
                      date_type=None, start_date=None, end_date=None, columns=[], file_format="csv",
                      monthly_download=False, use_sqs=False):
        date_range = {}
        if start_date:
            date_range['start_date'] = start_date
        if end_date:
            date_range['end_date'] = end_date
        json_request = {'award_levels': award_levels,
                        'filters': {
                            'award_types': award_types,
                            'agency': agency,
                            'date_type': date_type,
                            'date_range': date_range,
                        },
                        'columns': columns,
                        'file_format': file_format}
        sources = BulkDownloadAwardsViewSet().get_csv_sources(json_request=json_request)

        download_job_kwargs = {'job_status_id': JOB_STATUS_DICT['ready'],
                               'monthly_download': monthly_download,
                               'json_request': json.dumps(order_nested_object(json_request)),
                               'file_name': file_name,
                               'date_type': date_type}
        for award_level in award_levels:
            download_job_kwargs[award_level] = True
        for award_type in award_types:
            download_job_kwargs[award_type] = True
        if agency and agency != 'all':
            download_job_kwargs['agency'] = ToptierAgency.objects.filter(toptier_agency_id=agency).first()
        if sub_agency:
            download_job_kwargs['sub_agency'] = SubtierAgency.objects.filter(subtier_agency_id=sub_agency).first()
        if start_date:
            download_job_kwargs['start_date'] = start_date
        if end_date:
            download_job_kwargs['end_date'] = end_date
        download_job = BulkDownloadJob(**download_job_kwargs)
        download_job.save()

        kwargs = {
            'download_job': download_job,
            'file_name': file_name,
            'columns': [],
            'sources': tuple(sources)
        }
        if not use_sqs:
            logger.info('Populating {}'.format(file_name))
            csv_selection.write_csvs(**kwargs)
        else:
            # Send a SQS message that will be processed by another server
            # which will eventually run csv_selection.write_csvs(**kwargs)
            # (see generate_bulk_zip.py)
            message_attributes = {
                'download_job_id': {
                    'StringValue': str(kwargs['download_job'].bulk_download_job_id),
                    'DataType': 'String'
                },
                'file_name': {
                    'StringValue': kwargs['file_name'],
                    'DataType': 'String'
                },
                'columns': {
                    'StringValue': json.dumps(kwargs['columns']),
                    'DataType': 'String'
                },
                'sources': {
                    'StringValue': json.dumps(
                        tuple([source.toJsonDict() for source in kwargs['sources']])),
                    'DataType': 'String'
                }
            }
            queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                              QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody='Test', MessageAttributes=message_attributes)

    def upload_placeholder(self, file_name, empty_file):
        bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
        region = settings.BULK_DOWNLOAD_AWS_REGION

        logger.info('uploading {}'.format(file_name))
        csv_selection.upload(bucket, region, empty_file, file_name, acl='public-read',
                             parallel_processes=multiprocessing.cpu_count())

    def add_arguments(self, parser):
        parser.add_argument(
            '--local',
            action='store_true',
            dest='local',
            default=False,
            help='Generate all the files locally. Note they will still be uploaded to the S3.'
        )

        parser.add_argument(
            '--placeholders',
            action='store_true',
            dest='placeholders',
            default=False,
            help='Upload empty files as placeholders.'
        )

        parser.add_argument(
            '--agencies',
            dest='agencies',
            nargs='+',
            default=None,
            type=int,
            help='Toptier agency ids for testing'
        )

        parser.add_argument(
            '--fiscal_years',
            dest='fiscal_years',
            nargs='+',
            default=None,
            type=int,
            help='Fiscal years for testing'
        )

        parser.add_argument(
            '--empty-asssistance-file',
            dest='empty_asssistance_file',
            default='',
            help='Empty assisstance file for uploading'
        )

        parser.add_argument(
            '--empty-contracts-file',
            dest='empty_contracts_file',
            default='',
            help='Empty contracts file for uploading'
        )

    def handle(self, *args, **options):
        """Run the application."""

        # Make sure
        #   settings.BULK_DOWNLOAD_S3_BUCKET_NAME
        #   settings.BULK_DOWNLOAD_SQS_QUEUE_NAME
        #   settings.BULK_DOWNLOAD_AWS_REGION
        # are properly configured!

        local = options['local']
        placeholders = options['placeholders']
        agencies = options['agencies'] if options['agencies'] else None
        fiscal_years = options['fiscal_years'] if options['fiscal_years'] else None
        empty_asssistance_file = options['empty_asssistance_file']
        empty_contracts_file = options['empty_contracts_file']
        if placeholders and (not empty_asssistance_file or not empty_contracts_file):
            raise Exception('Placeholder arg provided but empty files not provided')

        current_date = datetime.date.today()
        updated_date_timestamp = datetime.datetime.strftime(current_date, '%Y%m%d')

        toptier_agencies = ToptierAgency.objects.all()
        if agencies:
            toptier_agencies = ToptierAgency.objects.filter(toptier_agency_id__in=agencies)
        toptier_agencies = list(toptier_agencies.values('name', 'toptier_agency_id', 'cgac_code'))
        # Adding 'all' to prevent duplication of code
        toptier_agencies.append({'name': 'All', 'toptier_agency_id': 'all', 'cgac_code': 'all'})
        if not fiscal_years:
            fiscal_years = range(2001, generate_fiscal_year(current_date)+1)

        logger.info('Generating {} files...'.format(len(toptier_agencies)*len(fiscal_years)*2))
        for agency in toptier_agencies:
            for fiscal_year in fiscal_years:
                start_date = '{}-10-01'.format(fiscal_year-1)
                end_date = '{}-09-30'.format(fiscal_year)
                for award_type in ['contracts', 'assistance']:
                    file_name = '{}_{}_{}_Full_{}.zip'.format(fiscal_year, agency['cgac_code'],
                                                              award_type.capitalize(), updated_date_timestamp)
                    logger.info('Generating {}'.format(file_name))
                    if placeholders:
                        empty_file = empty_contracts_file if award_type == 'contracts' else empty_asssistance_file
                        self.upload_placeholder(file_name=file_name, empty_file=empty_file)
                    else:
                        self.bulk_download(file_name, ['prime_awards'],
                                           award_types=award_mappings[award_type],
                                           agency=agency['toptier_agency_id'],
                                           date_type='action_date',
                                           start_date=start_date,
                                           end_date=end_date,
                                           monthly_download=True,
                                           use_sqs=(not local))
