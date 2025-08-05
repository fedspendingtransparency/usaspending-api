import copy

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.download.v2 import download_column_historical_lookups
from usaspending_api.references.models import ToptierAgency


class DownloadSource:
    def __init__(self, model_type, file_type, source_type, agency_id, filters=None, extra_file_type=None):
        self.model_type = model_type
        self.file_type = file_type
        self.source_type = source_type
        self.query_paths = copy.deepcopy(download_column_historical_lookups.query_paths)[model_type][file_type]
        self.human_names = list(self.query_paths.keys())
        if agency_id == "all":
            self.agency_code = agency_id
        else:
            agency = ToptierAgency.objects.filter(toptier_agency_id=agency_id).first()
            if agency:
                self.agency_code = agency.toptier_code
            else:
                raise InvalidParameterException("Agency with that ID does not exist")
        self._queryset = None
        self.file_name = None
        self.is_for_idv = VALUE_MAPPINGS[source_type].get("is_for_idv", False)
        self.is_for_contract = VALUE_MAPPINGS[source_type].get("is_for_contract", False)
        self.is_for_assistance = VALUE_MAPPINGS[source_type].get("is_for_assistance", False)
        self.award_category = None
        self.filters = filters or {}
        self.extra_file_type = extra_file_type

    def __repr__(self):
        return "DownloadSource('{}', '{}', '{}', '{}')".format(
            self.model_type, self.file_type, self.source_type, self.agency_code
        )

    def __str__(self):
        return self.__repr__()

    @property
    def annotations(self):
        annotations_function = VALUE_MAPPINGS[self.source_type].get("annotations_function")
        annotations = annotations_function(self.filters, self.file_type) if annotations_function is not None else {}
        return annotations

    @property
    def queryset(self):
        return self._queryset

    @queryset.setter
    def queryset(self, queryset):
        """
        If there are annotations listed in VALUE_MAPPINGS that have not already been applied to
        the queryset, apply them.  This function added to ensure annotations are always available
        to consumers of this class.  Previously, annotations weren't added until row_emitter
        which occasionally prevented them from being applied if the queryset was used outside
        of row_emitter (as was the case where a UNION was applied to the queryset).
        """
        for field, annotation in self.annotations.items():
            if field not in queryset.query.annotation_select:
                queryset = queryset.annotate(**{field: annotation})
        self._queryset = queryset

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
        query_paths = []
        for hn in headers:
            if self.query_paths[hn] is None:
                if self.annotations[hn] is None:
                    raise Exception("Annotated column {} is not in annotations function".format(hn))
                query_paths.append(hn)
            else:
                query_paths.append(self.query_paths[hn])

        return self.queryset.values(*query_paths)
