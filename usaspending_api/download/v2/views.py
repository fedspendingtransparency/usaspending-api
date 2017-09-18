import sys
import threading

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.download.v2 import download_column_lookups
from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.filters.transaction_assistance import transaction_assistance_filter
from usaspending_api.awards.v2.filters.transaction_contract import transaction_contract_filter
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.filestreaming import csv_selection
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.lookups import AWARD_DOWNLOAD_COLUMNS, JOB_STATUS_DICT


class BaseDownloadViewSet(APIView):

    s3_handler = S3Handler()

    def get_download_response(self, file_name):
        """ Generate download response which encompasses various elements to provide accurate status for state
        of a download job"""

        download_job = DownloadJob.objects.filter(file_name=file_name).first()
        if not download_job:
            raise Exception('Download job does not exist.')

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

        # TODO: Need to map column names to DAIMS

        kwargs = {'download_job': download_job,
                  'file_name': timestamped_file_name,
                  'upload_name': timestamped_file_name,
                  'columns': request.data['columns'],
                  'sources': csv_sources}

        if 'pytest' in sys.modules:
            # We are testing, and cannot use threads - the testing db connection
            # is not shared with the thread
            csv_selection.write_csvs(**kwargs)
        else:
            t = threading.Thread(target=csv_selection.write_csvs,
                                 kwargs=kwargs)
            t.start()

        return self.get_download_response(file_name=timestamped_file_name)


class DownloadAwardsViewSet(BaseDownloadViewSet):
    def get_csv_sources(self, json_request):
        filters = json_request['filters']
        queryset = award_filter(filters)
        from usaspending_api.awards.models import Award
        d1_queryset = queryset & Award.objects.filter(latest_transaction__contract_data__isnull=False)
        d1_source = csv_selection.CsvSource(d1_queryset, 'award', 'd1')
        d2_queryset = queryset & Award.objects.filter(latest_transaction__assistance_data__isnull=False)
        d2_source = csv_selection.CsvSource(queryset, 'award', 'd2')
        return (d1_source, d2_source)

    DOWNLOAD_NAME = 'awards'


class DownloadTransactionsViewSet(BaseDownloadViewSet):
    def get_csv_sources(self, json_request):
        filters = json_request['filters']
        transaction_contract_queryset = transaction_contract_filter(filters)
        transaction_assistance_queryset = transaction_assistance_filter(
            filters)
        return (csv_selection.CsvSource(transaction_contract_queryset, 'transaction', 'd1'),
                csv_selection.CsvSource(transaction_assistance_queryset, 'transaction', 'd2'))

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
