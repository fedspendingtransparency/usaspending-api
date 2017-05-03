from django.core.management.base import BaseCommand, CommandError

from django.core.urlresolvers import resolve

from usaspending_api.common.models import RequestCatalog, CSVdownloadableResponse
from django.utils.six.moves.urllib.parse import urlparse
from djqscsv import write_csv

import logging


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

        # See if we have a downloadable response for this path and checksum
        filename = self.create_filename_from_options(request_path, request_checksum)
        csv_downloadable_response, created = CSVdownloadableResponse.objects.get_or_create(request=request_catalog,
                                                                                           request_path=request_path,
                                                                                           file_name=filename)

        if not created:
            self.logger.info("Status of file: {}\nFilename: {}".format(csv_downloadable_response.status_description, csv_downloadable_response.file_name))
            return

        self.logger.info("No CSV file record found, beginning generation...")
        self.logger.info("Destination filename: {}".format(filename))
        # We have a valid request, but our csv_downloadable_response was created. Therefore we need to generate the file now
        # Set that CSV to the generating status
        csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.GENERATING_CODE.value
        csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.GENERATING_DESCRIPTION.value
        csv_downloadable_response.save()

        self.logger.info("Resolving path: {}".format(request_path))

        # Resolve the path to a view
        view, args, kwargs = resolve(urlparse(request_path)[2])

        self.logger.info("Path resolved to view {}".format(view))

        # Instantiate the view and pass the request in
        view = view.cls()
        view.req = request_catalog
        view.request = request_catalog.request

        if not hasattr(view, "get_queryset"):
            self.logger.info("Resolved view does not have get_queryset method, aborting.")
            csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.ERROR_CODE.value
            csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.ERROR_DESCRIPTION.value
            csv_downloadable_response.save()
            return

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

        with open('/'.join([self.CSV_DOWNLOAD_FOLDER_LOCATION, filename]), 'wb') as csv_file:
            write_csv(query_set, csv_file)

        csv_downloadable_response.status_code = CSVdownloadableResponse.STATUS.READY_CODE.value
        csv_downloadable_response.status_description = CSVdownloadableResponse.STATUS.READY_DESCRIPTION.value

        self.logger.info("Output complete.")

    def create_filename_from_options(self, path, checksum):
        split_path = [x for x in path.split("/") if len(x) > 0 and x != "api"]
        split_path.append(checksum)

        filename = "{}.csv".format("_".join(split_path))

        return filename
