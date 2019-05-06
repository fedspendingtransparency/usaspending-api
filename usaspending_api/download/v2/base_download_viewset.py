import json

from datetime import datetime, timezone

from django.conf import settings
from rest_framework.exceptions import NotFound
from rest_framework.response import Response

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.helpers.dict_helpers import order_nested_object
from usaspending_api.common.sqs_helpers import get_sqs_queue_resource
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.download.download_utils import create_unique_filename, log_new_download_job
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.request_validations import (
    validate_award_request, validate_idv_request, validate_account_request)


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class BaseDownloadViewSet(APIDocumentationView):
    s3_handler = S3Handler(
        bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME, redirect_dir=settings.BULK_DOWNLOAD_S3_REDIRECT_DIR
    )

    def post(self, request, request_type='award'):
        if request_type == 'award':
            json_request = validate_award_request(request.data)
        elif request_type == 'idv':
            json_request = validate_idv_request(request.data)
        else:
            json_request = validate_account_request(request.data)

        json_request['request_type'] = request_type
        ordered_json_request = json.dumps(order_nested_object(json_request))

        # Check if the same request has been called today
        # TODO!!! Use external_data_load_date to determine data freshness
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
        final_output_zip_name = create_unique_filename(json_request, request_agency)
        download_job = DownloadJob.objects.create(
            job_status_id=JOB_STATUS_DICT['ready'], file_name=final_output_zip_name, json_request=ordered_json_request
        )

        log_new_download_job(request, download_job)
        self.process_request(download_job)

        return self.get_download_response(file_name=final_output_zip_name)

    def process_request(self, download_job):
        if settings.IS_LOCAL:
            # Locally, we do not use SQS
            csv_generation.generate_csvs(download_job=download_job)
        else:
            # Send a SQS message that will be processed by another server which will eventually run
            # csv_generation.write_csvs(**kwargs) (see download_sqs_worker.py)
            write_to_log(
                message='Passing download_job {} to SQS'.format(download_job.download_job_id), download_job=download_job
            )
            queue = get_sqs_queue_resource(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
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
            'seconds_elapsed': download_job.seconds_elapsed(),
        }

        return Response(response)
