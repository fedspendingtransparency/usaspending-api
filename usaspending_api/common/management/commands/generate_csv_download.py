from django.core.management.base import BaseCommand, CommandError
from usaspending_api.common.models import RequestCatalog, CSVdownloadableResponse

from djqscsv import write_csv

import logging

from usaspending_api.common.csv_helpers import resolve_path_to_view, create_filename_from_options, format_path


class Command(BaseCommand):
    help = "Generates a CSV file from the specified request checksum and path \
            and writes the file to the csv_downloads folder with a pre-determined name"
    logger = logging.getLogger('console')

    CSV_DOWNLOAD_FOLDER_LOCATION = "csv_downloads"

    def add_arguments(self, parser):
        parser.add_argument('request_path', nargs=1, help='The API path of the request, e.g. /api/v1/awards/')
        parser.add_argument('request_checksum', nargs=1, help='The request checksum, for looking up via RequestCatalog')

    def handle(self, *args, **options):
        request_path = options['request_path'][0]
        request_checksum = options['request_checksum'][0]

        # Check if we have a request for that checksum
        request_catalog = RequestCatalog.objects.filter(checksum=request_checksum).first()

        if not request_catalog:
            self.logger.critical("No request catalog found for checksum: {}, aborting...".format(request_checksum))
            return

        csv_downloadable_response, created = CSVdownloadableResponse.get_or_create_from_parameters(request_path, request_catalog)

        # If we didn't create this request, AND it's also not requested (via URL), something else is handling it so we exit
        # If it is in an error'd state, we can retry.
        if not created and csv_downloadable_response.status_code != CSVdownloadableResponse.STATUS.REQUESTED_CODE.value and csv_downloadable_response.status_code != CSVdownloadableResponse.STATUS.ERROR_CODE.value:
            self.logger.info("Status of file: {}\nFilename: {}".format(csv_downloadable_response.status_description, csv_downloadable_response.file_name))
            return

        try:
            self.logger.info("No CSV file record found, beginning generation...")
            self.logger.info("Destination filename: {}".format(csv_downloadable_response.file_name))
            # We have a valid request, but our csv_downloadable_response was created. Therefore we need to generate the file now
            # Set that CSV to the generating status
            csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.GENERATING_CODE.value
            csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.GENERATING_DESCRIPTION.value
            csv_downloadable_response.save()

            self.logger.info("Resolving path: {}".format(csv_downloadable_response.request_path))

            view = resolve_path_to_view(request_path)

            if not view:
                self.logger.info("Path does not resolve to a currently supported CSV bulk view")
                csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.ERROR_CODE.value
                csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.ERROR_DESCRIPTION.value
                csv_downloadable_response.save()
                return

            self.logger.info("Path resolved to view {}".format(view))

            view.req = csv_downloadable_response.request
            view.request = csv_downloadable_response.request.request

            # Get the queryset
            query_set = view.get_queryset()

            self.logger.info("Number of records: {}".format(query_set.count()))

            # Check the request for a list of requested fields
            fields = request_catalog.request["data"].get("fields", []) + request_catalog.request["query_params"].get("fields", [])

            if len(fields) > 0:
                self.logger.info("Requested fields: {}".format(", ".join(fields)))
                query_set = query_set.values(*fields)
            else:
                self.logger.info("No specific fields requested, will render all fields.")

            with open('/'.join([self.CSV_DOWNLOAD_FOLDER_LOCATION, csv_downloadable_response.file_name]), 'wb') as csv_file:
                write_csv(query_set, csv_file)

            csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.READY_CODE.value
            csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.READY_DESCRIPTION.value
            csv_downloadable_response.save()

            self.logger.info("Output complete.")
        except:
            # Any error here should flag the response as "errored"
            csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.ERROR_CODE.value
            csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.ERROR_DESCRIPTION.value
            csv_downloadable_response.save()
