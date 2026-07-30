import re

from django.db.models import Q, QuerySet

from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.search.filters.postgres.HierarchicalFilter import HierarchicalFilter, Node
from usaspending_api.search.filters.shared.utils import escape_regex_chars


class TasCodes(HierarchicalFilter):
    underscore_name = "tas_codes"

    @classmethod
    def build_tas_codes_filter(cls, queryset: QuerySet, filter_values: list | dict) -> Q:
        if isinstance(filter_values, list):
            # This is a legacy usage, and will be dealt with by the other filter
            return TreasuryAccounts.build_tas_codes_filter(queryset, filter_values)
        elif isinstance(filter_values, dict):
            require = filter_values.get("require") or []
            exclude = filter_values.get("exclude") or []
        else:
            raise InvalidParameterException("tas_codes must be an array or object")

        return Q(
            treasury_account_identifiers__overlap=list(
                cls._query_string(TreasuryAppropriationAccount.objects.all(), require, exclude).values_list(
                    "treasury_account_identifier", flat=True
                )
            )
        )

    @staticmethod
    def node(
        code: list[str], positive: bool, positive_naics: list[list[str]], negative_naics: list[list[str]]
    ) -> "TASNode":
        return TASNode(code, positive, positive_naics, negative_naics)


def string_to_dictionary(string: str, true_agency_name: str) -> dict:
    if len(string.split("-")) == 1:
        return {true_agency_name: string}
    elif len(string.split("-")) == 2:
        return FederalAccount.fa_rendering_label_to_component_dictionary(string)
    else:
        return TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary(string)


def search_regex_of(v: str | dict) -> str:
    if isinstance(v, str):
        v = string_to_dictionary(v, "aid")

    code_lookup = {
        component: (escape_regex_chars(v[component]) if v.get(component) else (None if component == "ata" else ".*"))
        for component in ["ata", "aid", "main", "sub", "bpoa", "epoa", "a"]
    }

    # This is NOT the order of elements as displayed in the tas rendering label, but instead the order in the
    # award_delta_view and transaction_delta_view
    search_regex = TreasuryAppropriationAccount.generate_tas_rendering_label(
        code_lookup["ata"],
        code_lookup["aid"],
        code_lookup["a"],
        code_lookup["bpoa"],
        code_lookup["epoa"],
        code_lookup["main"],
        code_lookup["sub"],
    )

    # TODO: move this to a Tinyshield filter
    if not re.match(r"^(\d|\w|-|\*|\.|\\)+$", search_regex):
        raise UnprocessableEntityException(f"Unable to parse TAS filter {search_regex}")

    return search_regex


class TASNode(Node):
    def _basic_search_unit(self) -> Q:
        if len(self.ancestors) == 2:
            return Q(**{"tas_rendering_label__iregex": search_regex_of(self.code)})
        elif len(self.ancestors) == 1:
            return Q(**{"federal_account__federal_account_code": self.code})
        else:
            return Q(**{"federal_account__parent_toptier_agency__toptier_code": self.code})

    def clone(
        self, code: list[str], positive: bool, positive_naics: list[list[str]], negative_naics: list[list[str]]
    ) -> "TASNode":
        return TASNode(code, positive, positive_naics, negative_naics)


class TreasuryAccounts:
    underscore_name = "treasury_account_components"

    @staticmethod
    def build_tas_codes_filter(queryset: QuerySet, tas_filters: list) -> Q:
        if not tas_filters:
            return Q(treasury_account_identifiers__overlap=[])

        tas_qs = Q()
        for tas_filter in tas_filters:
            tas_qs |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas_filter.items()})

        return Q(
            treasury_account_identifiers__overlap=list(
                TreasuryAppropriationAccount.objects.filter(tas_qs).values_list(
                    "treasury_account_identifier", flat=True
                )
            )
        )
