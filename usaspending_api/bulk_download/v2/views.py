import sys
import threading

from django.conf import settings
from django.db.models import F, Sum, Value, CharField, Q
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotFound, ParseError

from usaspending_api.awards.v2.filters.award import award_filter
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.models import Award, TransactionNormalized, Agency
from usaspending_api.references.models import ToptierAgency, SubtierAgency
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.bulk_download.filestreaming import csv_selection
from usaspending_api.bulk_download.filestreaming.s3_handler import S3Handler
from usaspending_api.bulk_download.models import BulkDownloadJob
from usaspending_api.bulk_download.lookups import JOB_STATUS_DICT


class BaseDownloadViewSet(APIView):

    s3_handler = S3Handler()

    def get_download_response(self, file_name):
        """ Generate download response which encompasses various elements to provide accurate status for state
        of a download job"""

        download_job = BulkDownloadJob.objects.filter(file_name=file_name).first()
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
        download_job = BulkDownloadJob(job_status_id=JOB_STATUS_DICT['ready'],
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
            # REPLACE WITH AWS LAMBDA
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


class BulkDownloadListAgenciesViewSet(APIView):
    def post(self, request):
        """Return list of agencies if no POST data is provided.
        Otherwise, returns sub_agencies/federal_accounts associated with the agency provided"""
        response_data = {"agencies": [],
                         "sub_agencies": [],
                         "federal_accounts": []}

        agency_id = None
        post_data = request.data
        if post_data:
            if 'agency' not in post_data:
                raise InvalidParameterException('Agency Parameter not provided')
            agency_id = post_data['agency']

        toptier_agencies = list(ToptierAgency.objects.all().values("name", "toptier_agency_id", "cgac_code"))

        if not agency_id:
            response_data["agencies"] = toptier_agencies
        else:
            top_tier_agency = list(filter(lambda toptier: toptier["toptier_agency_id"] == agency_id, toptier_agencies))
            if not top_tier_agency:
                raise InvalidParameterException('Agency ID not found')
            top_tier_agency = top_tier_agency[0]
            response_data["sub_agencies"] = Agency.objects.filter(toptier_agency_id=agency_id)\
                .annotate(subtier_agency_name=F("subtier_agency__name"),subtier_agency_id=F("subtier_agency__subtier_agency_id"))\
                .values("subtier_agency_name", "subtier_agency_id")
            response_data["federal_accounts"] = FederalAccount.objects.filter(agency_identifier=top_tier_agency["cgac_code"]) \
                .annotate(federal_account_name=F("account_title"),
                          federal_account_id=F("id"))\
                .values("federal_account_name", "federal_account_id")
        return Response(response_data)



class BulkDownloadAwardsViewSet(BaseDownloadViewSet):
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


class BulkDownloadStatusViewSet(BaseDownloadViewSet):
    def get(self, request):
        """Obtain status for the download job matching the file name provided"""

        get_request = request.query_params
        file_name = get_request.get('file_name')

        if not file_name:
            raise InvalidParameterException(
                'Missing one or more required query parameters: file_name')

        return self.get_download_response(file_name=file_name)


class BulkDownloadTransactionCountViewSet(APIView):
    def post(self, request):
        """Returns boolean of whether a download request is greater
        than the max limit. """

        json_request = request.data

        # If no filters in request return empty object to return all transactions
        filters = json_request.get('filters', {})
        is_over_limit = False

        queryset = transaction_filter(filters)

        try:
            queryset[settings.MAX_DOWNLOAD_LIMIT]
            is_over_limit = True

        except IndexError:
            pass

        result = {
            "transaction_rows_gt_limit": False
        }

        return Response(result)
