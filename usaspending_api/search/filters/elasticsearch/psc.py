from django.db.models import QuerySet, Q
from elasticsearch_dsl import Q as ES_Q
from re import compile
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.references.v2.views.filter_tree.psc_filter_tree import PSC_GROUPS
from usaspending_api.search.filters.elasticsearch.filter import _Filter, _QueryType
from usaspending_api.search.filters.elasticsearch.HierarchicalFilter import HierarchicalFilter, Node


class PSCCodes(_Filter, HierarchicalFilter):
    underscore_name = "psc_codes"
    validation_pattern = compile("[A-Z0-9]{1,4}")

    @classmethod
    def generate_elasticsearch_query(cls, filter_values, query_type: _QueryType) -> ES_Q:
        cls.validate_filter_values(filter_values)
        require, exclude = cls.split_filter_values(filter_values)
        require = cls.handle_tier1_names(require)
        exclude = cls.handle_tier1_names(exclude)
        return ES_Q(
            "query_string", query=cls._query_string(require, exclude), default_field="product_or_service_code.keyword"
        )

    @classmethod
    def generate_postgres_query(cls, filter_values, queryset) -> QuerySet:
        cls.validate_filter_values(filter_values)
        require, exclude = cls.split_filter_values(filter_values)
        require = cls.handle_tier1_names(require)
        exclude = cls.handle_tier1_names(exclude)
        requires = Q()
        for r in require:
            requires |= Q(product_or_service_code__startswith=r[-1])
        excludes = Q()
        for e in exclude:
            excludes &= ~Q(product_or_service_code__startswith=e[-1])
        return queryset.filter(requires & excludes)

    @classmethod
    def validate_filter_values(cls, filter_values):
        if isinstance(filter_values, list):
            # Legacy.
            for code in filter_values:
                if not isinstance(code, str) or not cls.validation_pattern.fullmatch(code):
                    raise UnprocessableEntityException(
                        f"PSC codes must be one to four character uppercased alphanumeric strings.  "
                        f"Offending code: '{code}'."
                    )
        elif isinstance(filter_values, dict):
            # PSCCodeObject
            for key in ("require", "exclude"):
                code_lists = filter_values.get(key) or []
                if not isinstance(code_lists, list):
                    raise UnprocessableEntityException(f"require and exclude properties must be arrays.")
                for code_list in code_lists:
                    if not isinstance(code_list, list):
                        raise UnprocessableEntityException(f"require and exclude properties must be arrays of arrays.")
                    for seq, code in enumerate(code_list):
                        if seq == 0 and code not in PSC_GROUPS:
                            raise UnprocessableEntityException(
                                f"Tier1 PSC filter values must be one of: {tuple(PSC_GROUPS)}.  "
                                f"Offending code: '{code}'."
                            )
                        elif seq > 0 and (not isinstance(code, str) or not cls.validation_pattern.fullmatch(code)):
                            raise UnprocessableEntityException(
                                f"PSC codes must be one to four character uppercased alphanumeric strings.  "
                                f"Offending code: '{code}'."
                            )
        else:
            raise UnprocessableEntityException(f"psc_codes must be an array or object")

    @classmethod
    def split_filter_values(cls, filter_values):
        """ Here we assume that filter_values has already been run through validate_filter_values. """
        if isinstance(filter_values, list):
            # Legacy is treated as a "require" filter.
            require = [[f] for f in filter_values]
            exclude = []
        elif isinstance(filter_values, dict):
            # PSCCodeObject
            require = filter_values.get("require") or []
            exclude = filter_values.get("exclude") or []
        else:
            raise UnprocessableEntityException(f"psc_codes must be an array or object")

        return require, exclude

    @staticmethod
    def handle_tier1_names(code_lists):
        """
        The PSC lists can contain PSC codes and/or Tier1 names.  Tier1 names map to lists of prefix codes
        (see PSC_GROUPS).  If we have only been supplied a Tier1 name, we need to expand it out into something
        the database can understand.  If we have been supplied a tree branch, we need to remove the Tier1 name
        since it has no meaning in the database.

        For example

            [["Service", "B", "B5"], ["Product"]]

        should become

            [["B", "B5"], ["0"], ["1"], ["2"], ["3"], ["4"], ["5"], ["6"], ["7"], ["8"], ["9"]]

        Here we assume that code_lists has already been run through validate_filter_values.
        """
        expanded_list = []
        for code_list in code_lists:
            if code_list[0] in PSC_GROUPS:
                if len(code_list) == 1:
                    # Replace group name with terms.
                    expanded_list.extend(PSC_GROUPS[code_list[0]]["expanded_terms"])
                else:
                    # Remove group name.
                    expanded_list.append(code_list[1:])
            else:
                # Legacy won't have group names.
                expanded_list.append(code_list)
        return expanded_list

    @staticmethod
    def code_is_parent_of(code, other):
        return other[: len(code)] == code and len(code) < len(other)

    @staticmethod
    def node(code, positive, positive_psc, negative_psc):
        return PSCNode(code, positive, positive_psc, negative_psc)


class PSCNode(Node):
    def _basic_search_unit(self):
        """ All PSC leaf codes are four digits.  Anything shorter than that is a prefix. """
        return (self.code + "*") if len(self.code) < 4 else self.code

    def clone(self, code, positive, positive_psc, negative_psc):
        return PSCNode(code, positive, positive_psc, negative_psc)
