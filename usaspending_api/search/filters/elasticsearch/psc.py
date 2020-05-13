from django.db.models import QuerySet, Q
from elasticsearch_dsl import Q as ES_Q
from re import compile
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.references.v2.views.filter_tree.psc_filter_tree import PSC_GROUPS
from usaspending_api.search.filters.elasticsearch.filter import _Filter, _QueryType
from usaspending_api.search.filters.elasticsearch.HierarchicalFilter import HierarchicalFilter, Node


class PSCCodes(_Filter, HierarchicalFilter):
    underscore_name = "psc_codes"
    validation_pattern = compile("|".join(list(PSC_GROUPS) + ["[A-Z0-9]{1,4}"]))

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: _QueryType) -> ES_Q:
        require, exclude = cls.validate_filter_values(filter_values)
        require = cls.expand_psc_list(require)
        exclude = cls.expand_psc_list(exclude)
        return ES_Q(
            "query_string", query=cls._query_string(require, exclude), default_field="product_or_service_code.keyword"
        )

    @classmethod
    def generate_postgres_query(cls, filter_values, queryset) -> QuerySet:
        require, exclude = cls.validate_filter_values(filter_values)
        require = cls.expand_psc_list(require)
        exclude = cls.expand_psc_list(exclude)
        requires = Q()
        for r in require:
            requires |= Q(product_or_service_code__startswith=r[-1])
        excludes = Q()
        for e in exclude:
            excludes &= ~Q(product_or_service_code__startswith=e[-1])
        return queryset.filter(requires & excludes)

    @classmethod
    def validate_filter_values(cls, filter_values):
        def validate_codes(codes):
            for code in codes:
                if not cls.validation_pattern.fullmatch(code):
                    raise UnprocessableEntityException(
                        f"PSC codes must be one to four character alphanumeric strings or one of the "
                        f"Tier1 category names: {tuple(PSC_GROUPS)}.  Offending code: '{code}'."
                    )
            return codes

        # legacy functionality permits sending a single list of psc codes, which is treated as the required list
        if isinstance(filter_values, list):
            require = validate_codes(filter_values)
            exclude = []
        elif isinstance(filter_values, dict):
            require = validate_codes(filter_values.get("require") or [])
            exclude = validate_codes(filter_values.get("exclude") or [])
        else:
            raise UnprocessableEntityException(f"psc_codes must be an array or object")

        return require, exclude

    @staticmethod
    def expand_psc_list(codes):
        """
        The PSC list can contain PSC codes or Tier1 names.  Tier1 names map to lists of partial codes
        (see PSC_GROUPS).  This function replaces Tier1 names with their equivalent partial codes and
        joins them together with any PSC codes provided by the caller.  Note that the tree view search
        framework expects lists of lists so we perform that conversion here as well.

        For example

            ["ABCD", "Product", "1234"]

        should convert to

            [["ABCD"], ["1"], ["2"], ["3"], ["4"], ["5"], ["6"], ["7"], ["8"], ["9"], ["1234"]]
        """
        flattened_list = []
        for code in codes:
            flattened_list.extend(PSC_GROUPS.get(code, {}).get("search_terms") or [code])
        return [[f] for f in flattened_list]

    @staticmethod
    def node(code, positive, positive_psc, negative_psc):
        return PSCNode(code, positive, positive_psc, negative_psc)


class PSCNode(Node):
    def _basic_search_unit(self):
        retval = self.code
        if len(self.code) < 4:
            retval += "*"
        return retval

    def clone(self, code, positive, positive_psc, negative_psc):
        return PSCNode(code, positive, positive_psc, negative_psc)
