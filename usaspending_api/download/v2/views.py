import datetime
import json
import logging
import sys
import threading

from django.conf import settings
from django.db.models import Sum

from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotFound, ParseError
from rest_framework_extensions.cache.decorators import cache_response

from usaspending_api.awards.models import Award, SubAward, TransactionNormalized
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.subaward import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import download_transaction_count
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import order_nested_object
from usaspending_api.download.filestreaming import csv_selection, csv_generation
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob

VALUE_MAPPINGS = {
    # Award Level
    'awards': {
        'table': Award,
        'table_name': 'award',
        'download_name': 'awards',
        'contract_data': 'latest_transaction__contract_data',
        'assistance_data': 'latest_transaction__assistance_data',
        'filter_function': award_filter
    },
    # Transaction Level
    'prime_awards': {
        'table': TransactionNormalized,
        'table_name': 'transaction',
        'download_name': 'awards',
        'contract_data': 'contract_data',
        'assistance_data': 'assistance_data',
        'filter_function': transaction_filter
    },
    # SubAward Level
    'sub_awards': {
        'table': SubAward,
        'table_name': 'subaward',
        'download_name': 'subawards',
        'contract_data': 'award__latest_transaction__contract_data',
        'assistance_data': 'award__latest_transaction__assistance_data',
        'filter_function': subaward_filter
    }
}
SHARED_FILTER_DEFAULTS = {
    'award_type_codes': list(award_type_mapping.keys()),
    'agencies': [],
    'time_period': []
}
YEAR_CONSTRAINT_FILTER_DEFAULTS = {'elasticsearch_keyword': ''}
ROW_CONSTRAINT_FILTER_DEFAULTS = {
    'keyword': '',
    'legal_entities': [],
    'recipient_search_text': '',
    'recipient_scope': '',
    'recipient_locations': [],
    'recipient_type_names': [],
    'place_of_performance_scope': '',
    'place_of_performance_locations': [],
    'award_amounts': [],
    'award_ids': [],
    'program_numbers': [],
    'naics_codes': [],
    'psc_codes': [],
    'contract_pricing_type_codes': [],
    'set_aside_type_codes': [],
    'extent_competed_type_codes': [],
    'federal_account_ids': [],
    'object_class_ids': [],
    'program_activity_ids': []
}

logger = logging.getLogger('console')


class BaseDownloadViewSet(APIView):

    s3_handler = S3Handler()

    def get_download_response(self, file_name):
        """ Generate download response which encompasses various elements to provide accurate status for state
        of a download job"""

        download_job = DownloadJob.objects.filter(file_name=file_name).first()
        if not download_job:
            raise NotFound('Download job with filename {} does not exist.'.format(file_name))

        # compile url to file
        file_path = settings.CSV_LOCAL_PATH + file_name if settings.IS_LOCAL else \
            self.s3_handler.get_simple_url(file_name=file_name)

        # add additional response elements that should be part of anything calling this function
        response = {
            'status': download_job.job_status.name,
            'url': file_path,
            'message': download_job.error_message,
            'file_name': file_name,
            # converting size from bytes to kilobytes if file_size isn't None
            # TODO: 1000 or 1024?  Put units in name?
            'total_size': download_job.file_size / 1000 if download_job.file_size else None,
            'total_columns': download_job.number_of_columns,
            'total_rows': download_job.number_of_rows,
            'seconds_elapsed': download_job.seconds_elapsed()
        }

        return Response(response)

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        csv_sources = self.get_csv_sources(request.data)

        # get timestamped name to provide unique file name
        timestamped_file_name = self.s3_handler.get_timestamped_filename(
            self.DOWNLOAD_NAME + '.zip')

        # create download job in database to track progress. Starts at "ready"
        # status by default.
        download_job = DownloadJob(job_status_id=JOB_STATUS_DICT['ready'],
                                   file_name=timestamped_file_name)
        download_job.save()

        kwargs = {'download_job': download_job,
                  'file_name': timestamped_file_name,
                  'columns': request.data['columns'],
                  'sources': csv_sources}

        if 'pytest' in sys.modules:
            # We are testing, and cannot use threads - the testing db connection
            # is not shared with the thread
            csv_selection.write_csvs(**kwargs)
        else:
            t = threading.Thread(target=csv_selection.write_csvs,
                                 kwargs=kwargs)

            # Thread will stop when csv_selection.write_csvs stops
            t.start()

        return self.get_download_response(file_name=timestamped_file_name)


class DownloadAwardsViewSet(BaseDownloadViewSet):
    def post(self, request):
        request['download_type'] = 'awards'
        return super(BaseDownloadViewSet, self).post(request)

    def get_csv_sources(self, json_request):
        d1_source = csv_selection.CsvSource('award', 'd1')
        d2_source = csv_selection.CsvSource('award', 'd2')
        verify_requested_columns_available((d1_source, d2_source), json_request['columns'])
        filters = json_request['filters']

        queryset = award_filter(filters)

        # Contract file
        d1_source.queryset = queryset & Award.objects.filter(latest_transaction__contract_data__isnull=False)

        # Assistance file
        d2_source.queryset = queryset & Award.objects.filter(latest_transaction__assistance_data__isnull=False)

        return d1_source, d2_source

    DOWNLOAD_NAME = 'awards'


class DownloadTransactionsViewSet(BaseDownloadViewSet):
    def post(self, request):
        request['download_type'] = 'transactions'
        return super(BaseDownloadViewSet, self).post(request)

    def get_csv_sources(self, json_request):
        limit = parse_limit(json_request)
        contract_source = csv_selection.CsvSource('transaction', 'd1')
        assistance_source = csv_selection.CsvSource('transaction', 'd2')
        verify_requested_columns_available((contract_source, assistance_source), json_request['columns'])
        filters = json_request['filters']

        base_qset = transaction_filter(filters).values_list('id')[:limit]

        # Contract file
        queryset = TransactionNormalized.objects.filter(id__in=base_qset, contract_data__isnull=False)
        contract_source.queryset = queryset

        # Assistance file
        queryset = TransactionNormalized.objects.filter(id__in=base_qset, assistance_data__isnull=False)
        assistance_source.queryset = queryset

        return contract_source, assistance_source

    DOWNLOAD_NAME = 'transactions'


class DownloadStatusViewSet(BaseDownloadViewSet):
    def get(self, request):
        """Obtain status for the download job matching the file name provided"""

        get_request = request.query_params
        file_name = get_request.get('file_name')

        if not file_name:
            raise InvalidParameterException(
                'Missing one or more required query parameters: file_name')

        return self.get_download_response(file_name=file_name)


class DownloadTransactionCountViewSet(APIView):

    @cache_response()
    def post(self, request):
        """Returns boolean of whether a download request is greater
        than the max limit. """

        json_request = request.data

        # If no filters in request return empty object to return all transactions
        filters = json_request.get('filters', {})
        is_over_limit = False
        queryset, model = download_transaction_count(filters)

        if model in ['UniversalTransactionView']:
            total_count = queryset.count()
        else:
            # "summary" materialized views are pre-aggregated and contain a counts col
            total_count = queryset.aggregate(total_count=Sum('counts'))['total_count']

        if total_count and total_count > settings.MAX_DOWNLOAD_LIMIT:
            is_over_limit = True

        result = {
            "transaction_rows_gt_limit": is_over_limit
        }

        return Response(result)


class DownloadViewSet(APIView):

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""
        json_request = self.validate_request(request.data)

        # Check if the same request has been called today
        updated_date_timestamp = datetime.datetime.strftime(datetime.datetime.utcnow(), '%Y-%m-%d')
        cached_download = DownloadJob.objects.filter(
            json_request=json.dumps(order_nested_object(json_request)),
            update_date__gte=updated_date_timestamp).exclude(job_status_id=4).values('file_name')
        if cached_download:
            # By returning the cached files, there should be no duplicates on a daily basis
            cached_filename = cached_download[0]['file_name']
            return self.get_download_response(file_name=cached_filename)

        # Create download name and timestamped name for uniqueness
        download_name = '_'.join(VALUE_MAPPINGS[award_level]['download_name']
                                 for award_level in json_request['award_levels'])
        timestamped_file_name = self.s3_handler.get_timestamped_filename(download_name + '.zip')
        download_job = DownloadJob.objects.create({'job_status_id': JOB_STATUS_DICT['ready'],
                                                   'file_name': timestamped_file_name,
                                                   'json_request': json.dumps(order_nested_object(json_request))})
        logger.info('Created Bulk Download Job: {}\nFilename: {}\nRequest Params: {}'.
                    format(download_job.bulk_download_job_id, download_job.file_name, download_job.json_request))

        self.process_request(download_job)

        return self.get_download_response(file_name=timestamped_file_name)

    def validate_request(self, json_request):
        """Analyze request and raise any formatting errors as Exceptions"""

        # Check required parameters
        for required_param in ['award_levels', 'filters']:
            if required_param not in json_request:
                raise InvalidParameterException('{} parameter not provided'.format(required_param))

        if not isinstance(json_request['award_levels'], list):
            raise InvalidParameterException('Award levels parameter not provided as a list')
        elif len(json_request['award_levels']) == 0:
            raise InvalidParameterException('At least one award level is required.')
        for award_level in json_request['award_levels']:
            if award_level not in VALUE_MAPPINGS:
                raise InvalidParameterException('Invalid award_level: {}'.format(award_level))

        if not isinstance(json_request['filters'], dict):
            raise InvalidParameterException('Filters parameter not provided as a dict')
        elif len(json_request['filters']) == 0:
            raise InvalidParameterException('At least one filter is required.')

        # Check other base-level parameters or set to their defaults
        json_request['columns'] = json_request.get('columns', [])
        json_request['file_format'] = json_request.get('file_format', 'csv')

        # Validate shared filter types and assign defaults
        filters = json_request['filters']
        check_types_and_assign_defaults(filters, SHARED_FILTER_DEFAULTS)

        # Validate award types
        for award_type_code in filters['award_type_codes']:
            if award_type_code not in award_type_mapping:
                raise InvalidParameterException('Invalid award_type: {}'.format(award_type_code))

        # Validate time periods
        if len(filters['time_period']) == 0:
            filters['time_period'].append({'start_date': '', 'end_date': '', 'date_type': 'action_date'})
        default_date_values = {
            'start_date': '1000-01-01',
            'end_date': datetime.datetime.strftime(datetime.datetime.utcnow(), '%Y-%m-%d'),
            'date_type': 'action_date'
        }
        total_range_count = 0
        for date_range in filters['time_period']:
            check_types_and_assign_defaults(date_range, default_date_values)
            try:
                d1 = datetime.datetime.strptime(date_range['start_date'], "%Y-%m-%d")
                d2 = datetime.datetime.strptime(date_range['end_date'], "%Y-%m-%d")
            except ValueError:
                raise InvalidParameterException('Date Ranges must be in the format YYYY-MM-DD.')
            total_range_count += (d2 - d1).days
            # Derive date type
            if date_range['date_type'] not in ['action_date', 'last_modified_date']:
                raise InvalidParameterException(
                    'Invalid parameter within time_period\'s date_type: {}'.format(filters['date_type']))

        if filters.keys() in ROW_CONSTRAINT_FILTER_DEFAULTS.keys() or total_range_count > 365:
            # Generate files with a row count constraint
            json_request['constraint_type'] = 'row_count'

            # Validate row_count-constrainted filter types and assign defaults
            check_types_and_assign_defaults(filters, ROW_CONSTRAINT_FILTER_DEFAULTS)
        else:
            # Generate files with a 365-day time constraint
            json_request['constraint_type'] = 'year'

            # Validate year-constrainted filter types and assign defaults
            check_types_and_assign_defaults(filters, YEAR_CONSTRAINT_FILTER_DEFAULTS)

            # TODO: Is this for both download types or just Bulk?
            # Overriding all other filters if the keyword filter is provided
            if 'elasticsearch_keyword' in json_request['filters']:
                json_request['filters'] = {'elasticsearch_keyword': json_request['filters']['elasticsearch_keyword']}
                return json_request

        return json_request

    def process_request(self, download_job):
        if settings.IS_LOCAL:
            # We are testing, and cannot use SQS - the testing db connection is not shared with the thread
            csv_generation.generate_csvs(download_job=download_job)
        else:
            # Send a SQS message that will be processed by another server which will eventually run
            # csv_selection.write_csvs(**kwargs) (see generate_zip.py)
            queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                              QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody=download_job.download_job_id)

    def get_download_response(self, file_name):
        """Generate download response which encompasses various elements to provide accurate status for state of a
        download job"""
        download_job = DownloadJob.objects.filter(file_name=file_name).first()
        if not download_job:
            raise NotFound('Download job with filename {} does not exist.'.format(file_name))

        # Compile url to file
        file_path = settings.CSV_LOCAL_PATH + file_name if settings.IS_LOCAL else \
            self.s3_handler.get_simple_url(file_name=file_name)

        # Add additional response elements that should be part of anything calling this function
        response = {
            'status': download_job.job_status.name,
            'url': file_path,
            'message': download_job.error_message,
            'file_name': file_name,
            # converting size from bytes to kilobytes if file_size isn't None
            'total_size': download_job.file_size / 1000 if download_job.file_size else None,
            'total_columns': download_job.number_of_columns,
            'total_rows': download_job.number_of_rows,
            'seconds_elapsed': download_job.seconds_elapsed()
        }

        return Response(response)


def verify_requested_columns_available(sources, requested):
    bad_cols = set(requested)
    for source in sources:
        bad_cols -= set(source.columns(requested))
    if bad_cols:
        raise InvalidParameterException('Unknown columns: {}'.format(bad_cols))


def check_types_and_assign_defaults(request_dict, defaults_dict):
    for field in defaults_dict.keys():
        request_dict[field] = request_dict.get(field, defaults_dict[field])
        if not isinstance(request_dict[field], type(defaults_dict[field])):
            type_name = type(defaults_dict[field]).__name__
            raise InvalidParameterException('{} parameter not provided as a {}'.format(field, type_name))


def parse_limit(json_request):
    """Extracts the `limit` from a request and validates"""
    limit = json_request.get('limit')
    if limit:
        try:
            limit = int(json_request['limit'])
        except (ValueError, TypeError):
            raise ParseError('limit must be integer; {} given'.format(limit))
        if limit > settings.MAX_DOWNLOAD_LIMIT:
            msg = 'Requested limit {} beyond max supported ({})'
            raise ParseError(msg.format(limit, settings.MAX_DOWNLOAD_LIMIT))
    else:
        limit = settings.MAX_DOWNLOAD_LIMIT
    return limit   # None is a workable slice argument
