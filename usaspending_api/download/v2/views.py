import sys
import threading

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotFound, ParseError

from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.filestreaming import csv_selection
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT


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
            'total_size': download_job.file_size / 1000  # TODO: 1000 or 1024?  Put units in name?
            if download_job.file_size else None,
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
    return limit   # None is a workable slice argument


def verify_requested_columns_available(sources, requested):
    bad_cols = set(requested)
    for source in sources:
        bad_cols -= set(source.columns(requested))
    if bad_cols:
        raise InvalidParameterException('Unknown columns: {}'.format(bad_cols))


class DownloadAwardsViewSet(BaseDownloadViewSet):
    def get_csv_sources(self, json_request):
        d1_source = csv_selection.CsvSource('award', 'd1')
        d2_source = csv_selection.CsvSource('award', 'd2')
        verify_requested_columns_available((d1_source, d2_source), json_request['columns'])
        filters = json_request['filters']
        queryset = award_filter(filters)
        d1_source.queryset = queryset & Award.objects.filter(latest_transaction__contract_data__isnull=False)
        d2_source.queryset = queryset & Award.objects.filter(latest_transaction__assistance_data__isnull=False)
        return d1_source, d2_source

    DOWNLOAD_NAME = 'awards'


class DownloadTransactionsViewSet(BaseDownloadViewSet):
    def get_csv_sources(self, json_request):
        limit = parse_limit(json_request)
        limit = limit//2 if limit is not None else None
        contract_source = csv_selection.CsvSource('transaction', 'd1')
        assistance_source = csv_selection.CsvSource('transaction', 'd2')
        verify_requested_columns_available((contract_source, assistance_source), json_request['columns'])
        filters = json_request['filters']
        base_qset = transaction_filter(filters)
        queryset = base_qset & TransactionNormalized.objects.filter(contract_data__isnull=False)
        contract_source.queryset = queryset[:limit]
        queryset = base_qset & TransactionNormalized.objects.filter(assistance_data__isnull=False)
        assistance_source.queryset = queryset[:limit]
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
    def post(self, request):
        """Returns boolean of whether adownload request is greater than the maximum limit. """
        json_request = request.data

        # If no filters in request return empty object to return all transactions
        filters = json_request.get('filters', {})
        is_over_limit = False
        try:
            transaction_filter(filters)[settings.MAX_DOWNLOAD_LIMIT]
            is_over_limit = True
        except IndexError:
            pass

        result = {
            "transaction_rows_gt_limit": is_over_limit
        }

        return Response(result)
