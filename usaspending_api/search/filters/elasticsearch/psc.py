from elasticsearch_dsl import Q as ES_Q
from usaspending_api.search.filters.mixins.psc import PSCCodesMixin
from usaspending_api.search.filters.elasticsearch.filter import _Filter, QueryType
from usaspending_api.search.filters.elasticsearch.HierarchicalFilter import HierarchicalFilter, Node


class PSCCodes(PSCCodesMixin, _Filter, HierarchicalFilter):
    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: QueryType, **options) -> ES_Q:
        cls.validate_filter_values(filter_values)
        require, exclude = cls.split_filter_values(filter_values)
        require = cls.handle_tier1_names(require)
        exclude = cls.handle_tier1_names(exclude)
        return ES_Q(
            "query_string", query=cls._query_string(require, exclude), default_field="product_or_service_code.keyword"
        )

    @staticmethod
    def node(code, positive, positive_psc, negative_psc):
        return PSCNode(code, positive, positive_psc, negative_psc)


class PSCNode(Node):
    def _basic_search_unit(self):
        def build_search_pattern(code):
            """All PSC leaf codes are four digits.  Anything shorter than that is a prefix."""
            return (code + "*") if len(code) < 4 else code

        patterns = [build_search_pattern(code) for code in self.ancestors] + [build_search_pattern(self.code)]
        return f'({" AND ".join(patterns)})'

    def clone(self, code, positive, positive_naics, negative_naics):
        return PSCNode(code, positive, positive_naics, negative_naics)
