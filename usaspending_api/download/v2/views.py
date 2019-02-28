import json

from datetime import datetime, timezone

from django.conf import settings
from rest_framework.exceptions import NotFound
from rest_framework.response import Response

from usaspending_api.awards.v2.filters.location_filter_geocode import location_error_handling
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import order_nested_object
from usaspending_api.common.logging import get_remote_addr
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.helpers import (check_types_and_assign_defaults, parse_limit, validate_time_periods,
                                              write_to_download_log as write_to_log)
from usaspending_api.download.lookups import (JOB_STATUS_DICT, VALUE_MAPPINGS, SHARED_AWARD_FILTER_DEFAULTS,
                                              YEAR_CONSTRAINT_FILTER_DEFAULTS, ROW_CONSTRAINT_FILTER_DEFAULTS,
                                              ACCOUNT_FILTER_DEFAULTS)
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.download_utils import create_unique_filename


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class BaseDownloadViewSet(APIDocumentationView):
    s3_handler = S3Handler(
        bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME,
        redirect_dir=settings.BULK_DOWNLOAD_S3_REDIRECT_DIR
    )

    def post(self, request, request_type='award'):
        """Push a message to SQS with the validated request JSON"""
        if request_type == 'award':
            json_request = self.validate_award_request(request.data)
        else:
            json_request = self.validate_account_request(request.data)

        json_request['request_type'] = request_type
        ordered_json_request = json.dumps(order_nested_object(json_request))

        # Check if the same request has been called today
        updated_date_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d")
        cached_download = (
            DownloadJob.objects.filter(json_request=ordered_json_request, update_date__gte=updated_date_timestamp)
            .exclude(job_status_id=JOB_STATUS_DICT["failed"])
            .values("download_job_id", "file_name")
            .first()
        )

        if cached_download and not settings.IS_LOCAL:
            # By returning the cached files, there should be no duplicates on a daily basis
            write_to_log(
                message='Generating file from cached download job ID: {}'.format(cached_download['download_job_id'])
            )
            cached_filename = cached_download['file_name']
            return self.get_download_response(file_name=cached_filename)

        request_agency = json_request.get('filters', {}).get('agency', None)
        final_file_name = create_unique_filename(json_request["download_types"], request_agency)

        download_job = DownloadJob.objects.create(
            job_status_id=JOB_STATUS_DICT['ready'],
            file_name=final_file_name,
            json_request=ordered_json_request
        )

        write_to_log(
            message='Starting new download job [{}]'.format(download_job.download_job_id),
            download_job=download_job,
            other_params={'request_addr': get_remote_addr(request)}
        )

        self.process_request(download_job)

        return self.get_download_response(file_name=final_file_name)

    def validate_award_request(self, request_data):
        """Analyze request and raise any formatting errors as Exceptions"""
        json_request = {}
        constraint_type = request_data.get('constraint_type', None)

        # Validate required parameters
        for required_param in ['award_levels', 'filters']:
            if required_param not in request_data:
                raise InvalidParameterException(
                    'Missing one or more required query parameters: {}'.format(required_param))

        if not isinstance(request_data['award_levels'], list):
            raise InvalidParameterException('Award levels parameter not provided as a list')
        elif len(request_data['award_levels']) == 0:
            raise InvalidParameterException('At least one award level is required.')
        for award_level in request_data['award_levels']:
            if award_level not in VALUE_MAPPINGS:
                raise InvalidParameterException('Invalid award_level: {}'.format(award_level))
        json_request['download_types'] = request_data['award_levels']

        # Overriding all other filters if the keyword filter is provided in year-constraint download
        # Make sure this is after checking the award_levels
        if constraint_type == 'year' and 'elasticsearch_keyword' in request_data['filters']:
            json_request['filters'] = {'elasticsearch_keyword': request_data['filters']['elasticsearch_keyword'],
                                       'award_type_codes': list(award_type_mapping.keys())}
            json_request['limit'] = settings.MAX_DOWNLOAD_LIMIT
            return json_request

        if not isinstance(request_data['filters'], dict):
            raise InvalidParameterException('Filters parameter not provided as a dict')
        elif len(request_data['filters']) == 0:
            raise InvalidParameterException('At least one filter is required.')
        json_request['filters'] = {}

        # Set defaults of non-required parameters
        json_request['columns'] = request_data.get('columns', [])
        json_request['file_format'] = request_data.get('file_format', 'csv')

        # Validate shared filter types and assign defaults
        filters = request_data['filters']
        check_types_and_assign_defaults(filters, json_request['filters'], SHARED_AWARD_FILTER_DEFAULTS)

        # Validate award type types
        if not filters.get('award_type_codes', None) or len(filters['award_type_codes']) < 1:
            filters['award_type_codes'] = list(award_type_mapping.keys())
        for award_type_code in filters['award_type_codes']:
            if award_type_code not in award_type_mapping:
                raise InvalidParameterException('Invalid award_type: {}'.format(award_type_code))
        json_request['filters']['award_type_codes'] = filters['award_type_codes']

        # Validate locations
        for location_filter in ['place_of_performance_locations', 'recipient_locations']:
            if filters.get(location_filter):
                for location_dict in filters[location_filter]:
                    if not isinstance(location_dict, dict):
                        raise InvalidParameterException('Location is not a dictionary: {}'.format(location_dict))
                    location_error_handling(location_dict.keys())
                json_request['filters'][location_filter] = filters[location_filter]

        # Validate time periods
        total_range_count = validate_time_periods(filters, json_request)

        if constraint_type == 'row_count':
            # Validate limit exists and is below MAX_DOWNLOAD_LIMIT
            json_request['limit'] = parse_limit(request_data)

            # Validate row_count-constrainted filter types and assign defaults
            check_types_and_assign_defaults(filters, json_request['filters'], ROW_CONSTRAINT_FILTER_DEFAULTS)
        elif constraint_type == 'year':
            # Validate combined total dates within one year (allow for leap years)
            if total_range_count > 366:
                raise InvalidParameterException('Invalid Parameter: time_period total days must be within a year')

            # Validate year-constrainted filter types and assign defaults
            check_types_and_assign_defaults(filters, json_request['filters'], YEAR_CONSTRAINT_FILTER_DEFAULTS)
        else:
            raise InvalidParameterException('Invalid parameter: constraint_type must be "row_count" or "year"')

        return json_request

    def validate_account_request(self, request_data):
        json_request = {}

        json_request['columns'] = request_data.get('columns', [])

        # Validate required parameters
        for required_param in ["account_level", "filters"]:
            if required_param not in request_data:
                raise InvalidParameterException(
                    'Missing one or more required query parameters: {}'.format(required_param))

        # Validate account_level parameters
        if request_data.get('account_level', None) not in ["federal_account", "treasury_account"]:
            raise InvalidParameterException(
                'Invalid Parameter: account_level must be either "federal_account" or "treasury_account"'
            )
        json_request['account_level'] = request_data['account_level']

        # Validate the filters parameter and its contents
        json_request['filters'] = {}
        filters = request_data['filters']
        if not isinstance(filters, dict):
            raise InvalidParameterException('Filters parameter not provided as a dict')
        elif len(filters) == 0:
            raise InvalidParameterException('At least one filter is required.')

        # Validate required filters
        for required_filter in ["fy", "quarter"]:
            if required_filter not in filters:
                raise InvalidParameterException('Missing one or more required filters: {}'.format(required_filter))
            else:
                try:
                    filters[required_filter] = int(filters[required_filter])
                except (TypeError, ValueError):
                    raise InvalidParameterException('{} filter not provided as an integer'.format(required_filter))
            json_request['filters'][required_filter] = filters[required_filter]

        # Validate fiscal_quarter
        if json_request['filters']['quarter'] not in [1, 2, 3, 4]:
            raise InvalidParameterException('quarter filter must be a valid fiscal quarter (1, 2, 3, or 4)')

        # Validate submission_type parameters
        if filters.get('submission_type', None) not in ["account_balances", "object_class_program_activity",
                                                        "award_financial"]:
            raise InvalidParameterException('Invalid Parameter: submission_type must be "account_balances", '
                                            '"object_class_program_activity", or "award_financial"')
        json_request['download_types'] = [filters['submission_type']]

        # Validate the rest of the filters
        check_types_and_assign_defaults(filters, json_request['filters'], ACCOUNT_FILTER_DEFAULTS)

        return json_request

    def process_request(self, download_job):
        if settings.IS_LOCAL:
            # Locally, we do not use SQS
            csv_generation.generate_csvs(download_job=download_job)
        else:
            # Send a SQS message that will be processed by another server which will eventually run
            # csv_generation.write_csvs(**kwargs) (see generate_zip.py)
            write_to_log(
                message='Passing download_job {} to SQS'.format(download_job.download_job_id),
                download_job=download_job
            )
            queue = sqs_queue(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody=str(download_job.download_job_id))

    def get_download_response(self, file_name):
        """Generate download response which encompasses various elements to provide accurate status for state of a
        download job"""
        download_job = DownloadJob.objects.filter(file_name=file_name).first()
        if not download_job:
            raise NotFound('Download job with filename {} does not exist.'.format(file_name))

        # Compile url to file
        if settings.IS_LOCAL:
            file_path = settings.CSV_LOCAL_PATH + file_name
        else:
            file_path = self.s3_handler.get_simple_url(file_name=file_name)

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


class RowLimitedAwardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

    endpoint_doc: /download/advanced_search_award_download.md
    """

    def post(self, request):
        request.data['award_levels'] = ['awards', 'sub_awards']
        request.data['constraint_type'] = 'row_count'
        return BaseDownloadViewSet.post(self, request, 'award')


class RowLimitedTransactionDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of transaction data in CSV form for
    download.

    endpoint_doc: /download/advanced_search_transaction_download.md
    """

    def post(self, request):
        request.data['award_levels'] = ['transactions', 'sub_awards']
        request.data['constraint_type'] = 'row_count'
        return BaseDownloadViewSet.post(self, request, 'award')


class RowLimitedSubawardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of subaward data in CSV form for download.

    endpoint_doc: /download/advanced_search_subaward_download.md
    """

    def post(self, request):
        request.data['award_levels'] = ['sub_awards']
        request.data['constraint_type'] = 'row_count'
        return BaseDownloadViewSet.post(self, request, 'award')


class AccountDownloadViewSet(BaseDownloadViewSet):
    """This route sends a request to begin generating a zipfile of account data in CSV form for download."""

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""

        return BaseDownloadViewSet.post(self, request, 'account')
