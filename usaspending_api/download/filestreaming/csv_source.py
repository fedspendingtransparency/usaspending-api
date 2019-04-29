from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.download.v2 import download_column_historical_lookups
from usaspending_api.references.models import ToptierAgency


class CsvSource:

    def __init__(self, model_type, file_type, source_type, agency_id):
        self.model_type = model_type
        self.file_type = file_type
        self.source_type = source_type
        self.query_paths = download_column_historical_lookups.query_paths[model_type][file_type]
        self.human_names = list(self.query_paths.keys())
        if agency_id == "all":
            self.agency_code = agency_id
        else:
            agency = ToptierAgency.objects.filter(toptier_agency_id=agency_id).first()
            if agency:
                self.agency_code = agency.cgac_code
            else:
                raise InvalidParameterException("Agency with that ID does not exist")
        self.queryset = None
        self.file_name = None
        self.is_for_idv = VALUE_MAPPINGS[source_type].get('is_for_idv', False)

    def __repr__(self):
        return "CsvSource('{}', '{}', '{}', '{}')".format(
            self.model_type, self.file_type, self.source_type, self.agency_code
        )

    def __str__(self):
        return self.__repr__()

    def values(self, header):
        query_paths = [self.query_paths[hn] for hn in header]
        return self.queryset.values_list(query_paths).iterator()

    def columns(self, requested):
        """Given a list of column names requested, returns the ones available in the source"""
        result = self.human_names
        if requested:
            result = [header for header in requested if header in self.human_names]

        # remove headers that we don't have a query path for
        return [header for header in result if header in self.query_paths]

    def row_emitter(self, headers_requested):
        headers = self.columns(headers_requested)
        query_paths = [self.query_paths[hn] for hn in headers]
        return self.queryset.values(*query_paths)
