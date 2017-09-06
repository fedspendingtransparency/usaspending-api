import threading

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

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
            self.s3_handler.get_signed_url(path='', file_name=file_name)

        # add additional response elements that should be part of anything calling this function
        response = {
            'status': download_job.job_status.name,
            'url': file_path,
            'message': download_job.error_message,
            'file_name': file_name,
            # converting size from bytes to kilobytes if file_size isn't None
            'total_size': download_job.file_size / 1000 if download_job.file_size else None,
            'total_columns': download_job.number_of_columns,
            'total_rows': download_job.number_of_rows
        }

        return Response(response)


class DownloadAwardsViewSet(BaseDownloadViewSet):

    def post(self, request):
        """ Kick off CSV download job for Award data """

        json_request = request.data
        filters = json_request['filters']
        # columns = json_request['columns']
        # columns = [f.name for f in Award._meta.get_fields()]

        # filter Awards based on filter input
        queryset = award_filter(filters)

        # get timestamped name to provide unique file name
        timestamped_file_name = self.s3_handler.get_timestamped_filename('awards_download.csv')

        # create download job in database to track progress. Starts at "ready" status by default.
        download_job = DownloadJob(job_status_id=JOB_STATUS_DICT['ready'], file_name=timestamped_file_name)
        download_job.save()

        # TODO: Need to map column names to DAIMS
        columns = AWARD_DOWNLOAD_COLUMNS.keys()
        data = [row.values() for row in queryset.values()]

        # TODO: Start asynchronously in thread
        # TODO: Do not fetch data during post?
        t = threading.Thread(target=csv_selection.write_csv, kwargs={'download_job': download_job,
                                                                     'file_name': timestamped_file_name,
                                                                     'upload_name': timestamped_file_name,
                                                                     'header': columns, 'body': data})
        t.start()

        return self.get_download_response(file_name=timestamped_file_name)


class DownloadStatusViewSet(BaseDownloadViewSet):

    def get(self, request):
        """Obtain status for the download job matching the file name provided"""

        get_request = request.query_params
        file_name = get_request.get('file_name')

        if not file_name:
            raise InvalidParameterException('Missing one or more required query parameters: file_name')

        return self.get_download_response(file_name=file_name)


class DownloadTransactionsViewSet(BaseDownloadViewSet):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        json_request = request.data
        filters = json_request['filters']
        columns = json_request['columns']

        # filter Transactions based on filter input
        transaction_contract_queryset = transaction_contract_filter(filters)
        transaction_assistance_queryset = transaction_assistance_filter(filters)

        # get timestamped name to provide unique file name
        timestamped_file_name = self.s3_handler.get_timestamped_filename('transactions_download.csv')

        # create download job in database to track progress. Starts at "ready" status by default.
        download_job = DownloadJob(job_status_id=JOB_STATUS_DICT['ready'], file_name=timestamped_file_name)
        download_job.save()

        # TODO: Need to map column names to DAIMS
        # TODO: transaction columns?

        # TODO: Start asynchronously in thread
        # TODO: Do not fetch data during post?
        t = threading.Thread(target=csv_selection.write_csv_from_querysets, kwargs={'download_job': download_job,
                                                                     'file_name': timestamped_file_name,
                                                                     'upload_name': timestamped_file_name,
                                                                     'querysets': (transaction_contract_queryset, transaction_assistance_queryset)})
        t.start()

        return self.get_download_response(file_name=timestamped_file_name)







        # get timestamped name to provide unique file name
        timestamped_file_name = self.s3_handler.get_timestamped_filename('transactions_download.csv')

        # create download job in database to track progress. Starts at "ready" status by default.
        download_job = DownloadJob(job_status_id=JOB_STATUS_DICT['ready'], file_name=timestamped_file_name)
        download_job.save()

        csv_selection.write_csv(file_name=timestamped_file_name, upload_name=timestamped_file_name, is_local=True,
                                header=columns, body=None) # TODO: Combine transaction querysets here and print to CSV

        # # craft response
        # response = {
        #     "total_size": result["total_size"],
        #     "total_columns": len(columns),
        #     "total_rows": result["total_rows"],
        #     "file_name": result["file_name"]
        # }

        return self.download_response(file_name=file_name)
