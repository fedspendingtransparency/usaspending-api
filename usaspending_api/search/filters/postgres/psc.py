from django.db.models import Q
from usaspending_api.search.filters.mixins.psc import PSCCodesMixin
from usaspending_api.search.filters.postgres.HierarchicalFilter import HierarchicalFilter, Node


class PSCCodes(PSCCodesMixin, HierarchicalFilter):
    @classmethod
    def build_tas_codes_filter(cls, filter_values):
        cls.validate_filter_values(filter_values)
        require, exclude = cls.split_filter_values(filter_values)
        require = cls.handle_tier1_names(require)
        exclude = cls.handle_tier1_names(exclude)

        positive_nodes = [
            cls.node(code, True, require, exclude) for code in require if cls._has_no_parents(code, require + exclude)
        ]

        negative_nodes = [
            cls.node(code, False, require, exclude) for code in exclude if cls._has_no_parents(code, require + exclude)
        ]

        q = Q()
        for node in positive_nodes:
            # cancel out any require codes that also are excluded at top level
            if node.code not in [neg_node.code for neg_node in negative_nodes]:
                q |= node.get_query()
            else:
                q |= Q(pk__in=[])
        for node in negative_nodes:
            if node.children or node.code not in [pos_node.code for pos_node in positive_nodes]:
                q |= node.get_query()

        return q

    @staticmethod
    def node(code, positive, positive_psc, negative_psc):
        return PSCNode(code, positive, positive_psc, negative_psc)


class PSCNode(Node):
    def _basic_search_unit(self):
        if len(self.code) < 4:
            return Q(product_or_service_code__istartswith=self.code)
        return Q(product_or_service_code=self.code)

    def clone(self, code, positive, positive_naics, negative_naics):
        return PSCNode(code, positive, positive_naics, negative_naics)
