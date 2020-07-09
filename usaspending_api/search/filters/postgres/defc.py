from django.db.models import Q

from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters


class DefCodes:
    underscore_name = "def_codes"

    @classmethod
    def build_def_codes_filter(cls, queryset, filter_values):
        filter_values = {"def_codes": filter_values}
        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(filter_values)
        search = AwardSearch().filter(filter_query)
        response = search.handle_execute()
        award_ids = [res.to_dict()["award_id"] for res in response]
        return Q(award_id__in=list(award_ids))
