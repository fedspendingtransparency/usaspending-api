import sys
import itertools
import json
import time
import logging

from django.conf import settings
from django.db.models import F, Sum, Value, CharField, Q
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotFound, ParseError

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, \
    grant_type_mapping, direct_payment_type_mapping, loan_type_mapping, other_type_mapping
from usaspending_api.awards.models import Award, Subaward, TransactionNormalized, Agency
from usaspending_api.references.models import ToptierAgency, SubtierAgency
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.bulk_download.filestreaming import csv_selection
from usaspending_api.bulk_download.filestreaming.s3_handler import S3Handler
from usaspending_api.bulk_download.models import BulkDownloadJob
from usaspending_api.bulk_download.lookups import JOB_STATUS_DICT

logger = logging.getLogger('console')

award_type_mappings = {
    "contracts": list(contract_type_mapping.keys()),
    "grants": list(grant_type_mapping.keys()),
    "direct_payments": list(direct_payment_type_mapping.keys()),
    "loans": list(loan_type_mapping.keys()),
    "other_financial_assistance": list(other_type_mapping.keys())
}

value_mappings = {
    "prime_awards": {
        "table": Award,
        "action_date": "latest_transaction__action_date",
        "last_modified_date": "last_modified_date",
        "type": "type",
        "awarding_agency_id": "awarding_agency_id",
        "funding_agency_id": "funding_agency_id"

    },
    "sub_awards": {
        "table": Subaward,
        "action_date": "action_date",
        "last_modified_date": "award__last_modified_date",
        "type": "award__type",
        "awarding_agency_id": "awarding_agency_id",
        "funding_agency_id": "funding_agency_id"
    }
}

queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                  QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)


class BaseDownloadViewSet(APIView):

    s3_handler = S3Handler(name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME, region=settings.BULK_DOWNLOAD_AWS_REGION)

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
            generating_sqs_request_start = time.now()
            message_attributes = {
                "download_job_id": {
                    "StringValue": str(kwargs['download_job'].bulk_download_job_id),
                    'DataType': 'String'
                },
                "file_name": {
                    "StringValue": kwargs['file_name'],
                    'DataType': 'String'
                },
                "columns": {
                    "StringValue": json.dumps(kwargs['columns']),
                    'DataType': 'String'
                },
                'sources': {
                    "StringValue": json.dumps(tuple([source.toJsonDict() for source in kwargs['sources']])),
                    'DataType': 'String'
                }
            }
            logger.info("generating_sqs_request took {} seconds".format(time.now()-generating_sqs_request_start))
            response = queue.send_message(MessageBody="Test", MessageAttributes=message_attributes)

        return self.get_download_response(file_name=timestamped_file_name)

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
                raise InvalidParameterException('agency parameter not provided')
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
    def process_filters(self, filters, award_level):
        process_filters_start = time.now()
        and_queryset_filters = {}

        # Adding award type filter
        award_types = []
        if "award_types" in filters:
            if not isinstance(filters["award_types"], list):
                raise InvalidParameterException('award_types parameter not provided as a list')
            for award_type in filters["award_types"]:
                if award_type in award_type_mappings:
                    award_types.extend(award_type_mappings[award_type])
                else:
                    raise InvalidParameterException('Invalid parameter for award_types: {}'.format(award_type))
            # if the filter is calling everything, just remove the filter, save on the query performance
            if set(award_types) != set(itertools.chain(*award_type_mappings.values())):
                and_queryset_filters["{}__in".format(value_mappings[award_level]["type"])] = award_types

        # Adding date range filters
        date_attribute = None
        if "date_range" in filters and "date_type" not in filters:
            raise InvalidParameterException('date_range provided when not date_type is provided')
        elif "date_type" in filters and "date_range" not in filters:
            raise InvalidParameterException('date_type provided when not date_range is provided')
        elif "date_range" in filters:
            if filters["date_type"] == "action_date":
                date_attribute = value_mappings[award_level]["action_date"]
            elif filters["date_type"] == "last_modified_date":
                date_attribute = value_mappings[award_level]["last_modified_date"]
            else:
                raise InvalidParameterException('Invalid parameter for date_type: {}'.format(filters["date_type"]))
            # Get the date ranges
            if not isinstance(filters["date_range"], dict):
                raise InvalidParameterException('date_range parameter not provided as an object')
            elif "start_date" not in filters["date_range"]:
                raise InvalidParameterException('start_date parameter not provided in date_range')
            elif "end_date" not in filters["date_range"]:
                raise InvalidParameterException('end_date parameter not provided in date_range')
            else:
                and_queryset_filters["{}__gte".format(date_attribute)] = filters["date_range"]["start_date"]
                and_queryset_filters["{}__lte".format(date_attribute)] = filters["date_range"]["end_date"]

        # Agencies are to be OR'd together and then AND'd to the major query
        # TODO: use the value_mappings_table
        or_queryset = None
        if "sub_agency" in filters and "agency" not in filters:
            raise InvalidParameterException('sub_agency provided when not agency is provided')
        elif "agency" in filters:
            or_queryset = (Q(awarding_agency_id=filters["agency"]) |
                                   Q(funding_agency_id=filters["agency"]))
            if "sub_agency" in filters:
                or_queryset = (or_queryset & (Q(awarding_agency_id=filters["sub_agency"]) |
                                              Q(funding_agency_id=filters["sub_agency"])))

        table = value_mappings[award_level]["table"]
        if and_queryset_filters and or_queryset:
            filtered_queryset = table.objects.filter(**and_queryset_filters).filter(or_queryset)
        elif and_queryset_filters:
            filtered_queryset = table.objects.filter(**and_queryset_filters)
        elif or_queryset:
            filtered_queryset = table.objects.filter(or_queryset)
        else:
            filtered_queryset = table.objects.all()

        # TODO: Ordering by date for testing/verification, include sorting here
        order_by = date_attribute if date_attribute else value_mappings[award_level]["action_date"]
        filtered_queryset = filtered_queryset.order_by(order_by)

        logger.info("process_filters took {} seconds".format(time.now() - process_filters_start))
        return filtered_queryset


    def get_csv_sources(self, json_request):
        get_csv_sources_start = time.now()
        for required_param in ["file_format", "award_levels"]:
            if required_param not in json_request:
                raise InvalidParameterException('{} parameter not provided'.format(required_param))

        award_levels = json_request['award_levels']
        # TODO: Send use file_format
        file_format = json_request['file_format']

        csv_sources = []
        if not isinstance(award_levels, list):
            raise InvalidParameterException('award_levels parameter not provided as a list')
        for award_level in award_levels:
            table = value_mappings[award_level]["table"]
            if "filters" in json_request:
                queryset = self.process_filters(json_request['filters'], award_level)
            else:
                queryset = table.objects.all()
            if award_level == "prime_awards":
                d1_source = csv_selection.CsvSource('award', 'd1')
                d2_source = csv_selection.CsvSource('award', 'd2')
                verify_requested_columns_available((d1_source, d2_source), json_request['columns'])
                # TODO: move this into the process_filters
                d1_source.queryset = queryset & Award.objects.filter(latest_transaction__contract_data__isnull=False)
                d2_source.queryset = queryset & Award.objects.filter(latest_transaction__assistance_data__isnull=False)
                csv_sources.extend([d1_source, d2_source])
            elif award_level == "sub_awards":
                # TODO: Not fully implemented
                d1_source = csv_selection.CsvSource('subaward', 'd1')
                d2_source = csv_selection.CsvSource('subaward', 'd2')
                verify_requested_columns_available((d1_source, d2_source), json_request['columns'])
            else:
                raise InvalidParameterException('Invalid parameter for award_levels: {}'.format(award_level))

        logger.info("get_csv_sources took {} seconds".format(time.now() - get_csv_sources_start))

        return tuple(csv_sources)

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
        result = {
            "transaction_rows_gt_limit": False
        }

        return Response(result)
