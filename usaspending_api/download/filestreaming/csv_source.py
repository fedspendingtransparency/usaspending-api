from usaspending_api.download.v2 import download_column_historical_lookups
from usaspending_api.references.models import ToptierAgency


class CsvSource:
    def __init__(self, model_type, file_type, source_type, agency_id):
        self.model_type = model_type
        self.file_type = file_type
        self.source_type = source_type
        self.query_paths = download_column_historical_lookups.query_paths[model_type][file_type]
        self.human_names = list(self.query_paths.keys())
        if agency_id == 'all':
            self.agency_code = 'all'
        else:
            self.agency_code = ToptierAgency.objects.filter(toptier_agency_id=agency_id).first().cgac_code
        self.queryset = None

    def values(self, header):
        query_paths = [self.query_paths[hn] for hn in header]
        return self.queryset.values_list(query_paths).iterator()

    def columns(self, requested):
        """Given a list of column names requested, returns the ones available in the source"""
        result = self.human_names
        if requested:
            result = [header for header in requested if header in self.human_names]

        # remove headers that we don't have a query path for
        result = [header for header in result if header in self.query_paths]

        return result

    def row_emitter(self, headers_requested):
        headers = self.columns(headers_requested)
        query_paths = [self.query_paths[hn] for hn in headers]
        return self.queryset.values(*query_paths)
