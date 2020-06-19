from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from django.db.models import Q, Sum, Count, F, Value, DecimalField
from django.db.models.functions import Concat
from rest_framework.response import Response
from typing import List, Dict

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, PaginationMixin, LoansMixin
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@dataclass_json
@dataclass
class TreasuryAccount:
    id: int
    code: str
    description: str
    count: int = 0
    obligation: float = 0
    outlay: float = 0
    total_budgetary_resources: float = 0


@dataclass_json
@dataclass
class FederalAccount(TreasuryAccount):
    children: List[TreasuryAccount] = field(default_factory=list)

    def __hash__(self):
        return hash(f"{self.id}-{self.code}")

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.id == other.id
            and self.code == other.code
            and self.description == other.description
        )

    def include(self, val):
        self.children.append(val)


@dataclass_json
@dataclass
class FAResults:
    _federal_accounts: Dict[FederalAccount, FederalAccount] = field(default_factory=dict)

    def __contains__(self, val):
        return val in self._federal_accounts

    def __setitem__(self, key, val):
        self._federal_accounts[key] = val

    def __getitem__(self, key):
        return self._federal_accounts[key]

    def __len__(self):
        return len(self._federal_accounts)

    def rollup(self):
        for row in self._federal_accounts:
            for child in row.children:
                row.outlay += child.outlay
                row.obligation += child.obligation
                row.total_budgetary_resources = None  # += child.total_budgetary_resources
                row.count += 1

    def add_if_missing(self, val):
        if val not in self._federal_accounts:
            self._federal_accounts[val] = val

    def sort(self, field, direction):
        for row in self._federal_accounts:
            row.children = self.sort_results(row.children, field, direction)

        self._federal_accounts = self.sort_results(self._federal_accounts, field, direction)

    def slice(self, start, end):
        results = []
        for i, fa in enumerate(self._federal_accounts):
            if i >= start and i < end:
                results.append(fa)
        return results

    def finalize(self, sort_key, sort_order, start, end):
        self.rollup()
        self.sort(sort_key, sort_order)
        return list(fa.to_dict() for fa in self.slice(start, end))

    @staticmethod
    def sort_results(items, field, direction="desc"):
        reverse = True
        if direction == "asc":
            reverse = False
        return sorted(items, key=lambda x: getattr(x, field), reverse=reverse)


class Spending(PaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):

        filters = [
            Q(final_of_fy=True),  # TODO ASAP! Only include Account data from "closed periods" using lookup table
            Q(
                Q(Q(submission__reporting_fiscal_year=2020) & Q(submission__reporting_fiscal_period__gte=1))
                | Q(submission__reporting_fiscal_year__gte=2021)
            ),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
        ]
        annotations = {
            "fa_code": Concat(
                F("treasury_account__main_account_code"), Value("-"), F("treasury_account__sub_account_code")
            ),
            "count": Count("*"),
            "description": F("treasury_account__tas_rendering_label"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Sum("obligations_incurred_by_program_object_class_cpe"),
            "outlay": Sum("gross_outlay_amount_by_program_object_class_cpe"),
            "total_budgetary_resources": Value(None, DecimalField(max_digits=23, decimal_places=2)),  # Temporary: GTAS
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        results = list(
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )

        federal_accounts = FAResults()
        for row in results:
            FA = FederalAccount(id=row["fa_id"], code=row["fa_code"], description=row["fa_description"],)
            TAS = TreasuryAccount(
                id=row["id"],
                code=row["code"],
                count=row["count"],
                description=row["description"],
                obligation=row["obligation"],
                outlay=row["outlay"],
                total_budgetary_resources=row["total_budgetary_resources"],
            )

            federal_accounts.add_if_missing(FA)
            federal_accounts[FA].include(TAS)

        page_metadata = get_pagination_metadata(len(federal_accounts), self.pagination.limit, self.pagination.page)

        return Response(
            {
                "results": federal_accounts.finalize(
                    self.pagination.sort_key,
                    self.pagination.sort_order,
                    self.pagination.lower_limit,
                    self.pagination.upper_limit,
                ),
                "page_metadata": page_metadata,
            }
        )


class Loans(LoansMixin, PaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):
        return Response({"endpoint": self.endpoint_doc})
