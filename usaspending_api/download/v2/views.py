import datetime
import json
import os
import pandas as pd

from django.conf import settings
from django.db.models import F
from rest_framework.exceptions import NotFound
from rest_framework.response import Response

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.awards.models import Agency
from usaspending_api.awards.v2.filters.location_filter_geocode import location_error_handling
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping, all_award_types_mappings
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
from usaspending_api.download.lookups import (JOB_STATUS_DICT, VALUE_MAPPINGS, SHARED_AWARD_FILTER_DEFAULTS, CFO_CGACS,
                                              YEAR_CONSTRAINT_FILTER_DEFAULTS, ROW_CONSTRAINT_FILTER_DEFAULTS,
                                              ACCOUNT_FILTER_DEFAULTS)
from usaspending_api.download.models import DownloadJob
from usaspending_api.references.models import ToptierAgency


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class BaseDownloadViewSet(APIDocumentationView):
    s3_handler = S3Handler(bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME,
                           redirect_dir=settings.BULK_DOWNLOAD_S3_REDIRECT_DIR)

    def post(self, request, request_type='award'):
        """Push a message to SQS with the validated request JSON"""
        json_request = (self.validate_award_request(request.data) if request_type == 'award' else
                        self.validate_account_request(request.data))
        json_request['request_type'] = request_type
        ordered_json_request = json.dumps(order_nested_object(json_request))

        # Check if the same request has been called today
        updated_date_timestamp = datetime.datetime.strftime(datetime.datetime.utcnow(), '%Y-%m-%d')
        cached_download = DownloadJob.objects. \
            filter(json_request=ordered_json_request, update_date__gte=updated_date_timestamp). \
            exclude(job_status_id=JOB_STATUS_DICT["failed"]).values('download_job_id', 'file_name')
        if cached_download and not settings.IS_LOCAL:
            # By returning the cached files, there should be no duplicates on a daily basis
            write_to_log(message='Generating file from cached download job ID: {}'
                         .format(cached_download[0]['download_job_id']))
            cached_filename = cached_download[0]['file_name']
            return self.get_download_response(file_name=cached_filename)

        # Create download name and timestamped name for uniqueness
        toptier_agency_filter = ToptierAgency.objects.filter(
            toptier_agency_id=json_request.get('filters', {}).get('agency', None)).first()
        download_name = '{}_{}'.format(toptier_agency_filter.cgac_code if toptier_agency_filter else 'all',
                                       '_'.join(VALUE_MAPPINGS[award_level]['download_name']
                                                for award_level in json_request['download_types']))
        timestamped_file_name = self.s3_handler.get_timestamped_filename(download_name + '.zip')

        download_job = DownloadJob.objects.create(job_status_id=JOB_STATUS_DICT['ready'],
                                                  file_name=timestamped_file_name,
                                                  json_request=ordered_json_request)

        write_to_log(message='Starting new download job'.format(download_job.download_job_id),
                     download_job=download_job, other_params={'request_addr': get_remote_addr(request)})
        self.process_request(download_job)

        return self.get_download_response(file_name=timestamped_file_name)

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
            raise InvalidParameterException('Invalid Parameter: account_level must be either "federal_account" or '
                                            '"treasury_account"')
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
            write_to_log(message='Passing download_job {} to SQS'.format(download_job.download_job_id),
                         download_job=download_job)
            queue = sqs_queue(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody=str(download_job.download_job_id))

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


class YearLimitedDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

    endpoint_doc: /download/custom_award_data_download.md
    """

    def post(self, request):
        request.data['constraint_type'] = 'year'

        # TODO: update front end to use the Common Filter Object and get rid of this function
        self.process_filters(request.data)

        return BaseDownloadViewSet.post(self, request, 'award')

    def process_filters(self, request_data):
        """Filter function to update Bulk Download parameters to shared parameters"""

        # Validate filter parameter
        filters = request_data.get('filters', None)
        if not filters:
            raise InvalidParameterException('Missing one or more required query parameters: filters')

        # Validate keyword search first, remove all other filters
        keyword_filter = filters.get('keyword', None) or filters.get('keywords', None)
        if keyword_filter and len(filters.keys()) == 1:
            request_data['filters'] = {'elasticsearch_keyword': keyword_filter}
            return

        # Validate other parameters previously required by the Bulk Download endpoint
        for required_param in ['award_types', 'agency', 'date_type', 'date_range']:
            if required_param not in filters:
                raise InvalidParameterException('Missing one or more required query parameters: {}'.
                                                format(required_param))

        # Replacing award_types with award_type_codes
        filters['award_type_codes'] = []
        try:
            for award_type_code in filters['award_types']:
                if award_type_code in all_award_types_mappings:
                    filters['award_type_codes'].extend(all_award_types_mappings[award_type_code])
                else:
                    raise InvalidParameterException('Invalid award_type: {}'.format(award_type_code))
            del filters['award_types']
        except TypeError:
            raise InvalidParameterException('award_types parameter not provided as a list')

        # Replacing date_range with time_period
        date_range_copied = filters['date_range'].copy()
        date_range_copied['date_type'] = filters['date_type']
        filters['time_period'] = [date_range_copied]
        del filters['date_range']
        del filters['date_type']

        # Replacing agency with agencies
        if filters['agency'] != 'all':
            toptier_name = (ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).values('name'))
            if not toptier_name:
                raise InvalidParameterException('Toptier ID not found: {}'.format(filters['agency']))
            toptier_name = toptier_name[0]['name']
            if 'sub_agency' in filters:
                if filters['sub_agency']:
                    filters['agencies'] = [{'type': 'awarding', 'tier': 'subtier', 'name': filters['sub_agency'],
                                           'toptier_name': toptier_name}]
                del filters['sub_agency']
            else:
                filters['agencies'] = [{'type': 'awarding', 'tier': 'toptier', 'name': toptier_name}]
        del filters['agency']

        request_data['filters'] = filters


class AccountDownloadViewSet(BaseDownloadViewSet):
    """This route sends a request to begin generating a zipfile of account data in CSV form for download."""

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""

        return BaseDownloadViewSet.post(self, request, 'account')


class DownloadListAgenciesViewSet(APIDocumentationView):
    """
    This route lists all the agencies and the subagencies or federal accounts associated under specific agencies.

    endpoint_doc: /download/list_agencies.md
    """
    modified_agencies_list = os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                                          'modified_authoritative_agency_list.csv')
    sub_agencies_map = {}

    def pull_modified_agencies_cgacs_subtiers(self):
        # Get a dict of used subtiers and their associated CGAC code pulled from
        # modified_agencies_list
        with open(self.modified_agencies_list, encoding='Latin-1') as modified_agencies_list_csv:
            mod_gencies_list_df = pd.read_csv(modified_agencies_list_csv, dtype=str)
        mod_gencies_list_df = mod_gencies_list_df[['CGAC AGENCY CODE', 'SUBTIER CODE', 'FREC', 'IS_FREC']]
        mod_gencies_list_df['CGAC AGENCY CODE'] = mod_gencies_list_df['CGAC AGENCY CODE'].apply(lambda x: x.zfill(3))
        mod_gencies_list_df['FREC'] = mod_gencies_list_df['FREC'].apply(lambda x: x.zfill(4))
        for _, row in mod_gencies_list_df.iterrows():
            # cgac_code in the database can be either agency cgac or frec code (if a frec agency)
            self.sub_agencies_map[row['SUBTIER CODE']] = row['FREC'] \
                if row['IS_FREC'].upper() == 'TRUE' else row['CGAC AGENCY CODE']

    def post(self, request):
        """Return list of agencies if no POST data is provided.
        Otherwise, returns sub_agencies/federal_accounts associated with the agency provided"""
        response_data = {'agencies': [],
                         'sub_agencies': [],
                         'federal_accounts': []}
        if not self.sub_agencies_map:
            # populate the sub_agencies dictionary
            self.pull_modified_agencies_cgacs_subtiers()
        used_cgacs = set(self.sub_agencies_map.values())

        agency_id = None
        post_data = request.data
        if post_data:
            if 'agency' not in post_data:
                raise InvalidParameterException('Missing one or more required query parameters: agency')
            agency_id = post_data['agency']

        # Get all the top tier agencies
        toptier_agencies = list(ToptierAgency.objects.filter(cgac_code__in=used_cgacs)
                                .values('name', 'toptier_agency_id', 'cgac_code'))

        if not agency_id:
            # Return all the agencies if no agency id provided
            cfo_agencies = sorted(list(filter(lambda agency: agency['cgac_code'] in CFO_CGACS, toptier_agencies)),
                                  key=lambda agency: CFO_CGACS.index(agency['cgac_code']))
            other_agencies = sorted([agency for agency in toptier_agencies if agency not in cfo_agencies],
                                    key=lambda agency: agency['name'])
            response_data['agencies'] = {'cfo_agencies': cfo_agencies,
                                         'other_agencies': other_agencies}
        else:
            # Get the top tier agency object based on the agency id provided
            top_tier_agency = list(filter(lambda toptier: toptier['toptier_agency_id'] == agency_id, toptier_agencies))
            if not top_tier_agency:
                raise InvalidParameterException('Agency ID not found')
            top_tier_agency = top_tier_agency[0]
            # Get the sub agencies and federal accounts associated with that top tier agency
            # Removed distinct subtier_agency_name since removing subtiers with multiple codes that aren't in the
            # modified list
            response_data['sub_agencies'] = Agency.objects.filter(toptier_agency_id=agency_id)\
                .values(subtier_agency_name=F('subtier_agency__name'),
                        subtier_agency_code=F('subtier_agency__subtier_code'))\
                .order_by('subtier_agency_name')
            # Tried converting this to queryset filtering but ran into issues trying to
            # double check the right used subtier_agency by cross checking the cgac_code
            # see the last 2 lines of the list comprehension below
            response_data['sub_agencies'] = [subagency for subagency in response_data['sub_agencies']
                                             if subagency['subtier_agency_code'] in self.sub_agencies_map and
                                             self.sub_agencies_map[subagency['subtier_agency_code']] ==
                                             top_tier_agency['cgac_code']]
            for subagency in response_data['sub_agencies']:
                del subagency['subtier_agency_code']

            response_data['federal_accounts'] = FederalAccount.objects\
                .filter(agency_identifier=top_tier_agency['cgac_code'])\
                .values(federal_account_name=F('account_title'), federal_account_id=F('id'))\
                .order_by('federal_account_name')
        return Response(response_data)
