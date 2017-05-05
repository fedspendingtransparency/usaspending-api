from django.core.management.base import BaseCommand, CommandError
from usaspending_api.common.models import RequestCatalog

from djqscsv import write_csv

import logging
import sys

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
        request_path = format_path(options['request_path'][0])
        request_checksum = options['request_checksum'][0]

        # Check if we have a request for that checksum
        request_catalog = RequestCatalog.objects.filter(checksum=request_checksum).first()
        file_name = create_filename_from_options(request_path, request_checksum)

        if not request_catalog:
            self.logger.critical("No request catalog found for checksum: {}, aborting...".format(request_checksum))
            sys.exit(0)

        try:
            self.logger.info("Beginning generation...")
            self.logger.info("Destination filename: {}".format(file_name))

            self.logger.info("Resolving path: {}".format(request_path))

            view = resolve_path_to_view(request_path)

            if not view:
                self.logger.info("Path does not resolve to a currently supported CSV bulk view")
                sys.exit(0)

            self.logger.info("Path resolved to view {}".format(view))

            view.req = request_catalog
            view.request = request_catalog.request

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

            with open('/'.join([self.CSV_DOWNLOAD_FOLDER_LOCATION, file_name]), 'wb') as csv_file:
                write_csv(query_set, csv_file)

            self.logger.info("Output complete.")
            sys.exit(1)
        except Exception as e:
            self.logger.exception(e)
            sys.exit(0)
