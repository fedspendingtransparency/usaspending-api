from django.db.models import Sum, Q
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any, List
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import BureauTitleLookup
from usaspending_api.submissions.helpers import get_latest_submission_ids_for_fiscal_year


class FederalAccountList(PaginationMixin, AgencyBase):
    """
    Obtain the list of federal accounts and treasury accounts for a specific agency in a
    single fiscal year based on whether or not that federal account/treasury account has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/federal_account.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "filter"]

    def format_results(self, rows):
        non_distinct = [
            {
                "code": row["treasury_account__federal_account__federal_account_code"],
                "name": row["treasury_account__federal_account__account_title"],
            }
            for row in rows
        ]
        distinct = set()
        accounts = []
        order = self.pagination.sort_order == "desc"
        for account in non_distinct:
            if account["code"] not in distinct:
                distinct.add(account["code"])
                accounts.append(account)
        for item in accounts:
            bureau = BureauTitleLookup.objects.filter(federal_account_code=item['code']).values()
            item['bureau_slug'] = bureau[0]['bureau_slug']
            item["children"] = [
                {
                    "name": row["treasury_account__account_title"],
                    "code": row["treasury_account__tas_rendering_label"],
                    "obligated_amount": row["obligated_amount"],
                    "gross_outlay_amount": row["gross_outlay_amount"],
                }
                for row in rows
                if item["code"] == row["treasury_account__federal_account__federal_account_code"]
            ]
            item["obligated_amount"] = sum([x["obligated_amount"] for x in item["children"]])
            item["gross_outlay_amount"] = sum([x["gross_outlay_amount"] for x in item["children"]])
            item["children"] = sorted(item["children"], key=lambda x: x[self.pagination.sort_key], reverse=order)
        accounts = sorted(accounts, key=lambda x: x[self.pagination.sort_key], reverse=order)
        return accounts

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "obligated_amount", "gross_outlay_amount"]
        self.default_sort_column = "obligated_amount"
        results = self.format_results(self.get_federal_account_list())
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "results": results,
                "messages": self.standard_response_messages,
            }
        )

    def get_federal_account_list(self) -> List[dict]:
        submission_ids = get_latest_submission_ids_for_fiscal_year(self.fiscal_year)
        filters = [
            Q(treasury_account__funding_toptier_agency=self.toptier_agency),
            Q(submission_id__in=submission_ids),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
        ]
        if self.filter:
            filters.append(
                Q(
                    Q(treasury_account__account_title__icontains=self.filter)
                    | Q(treasury_account__tas_rendering_label__icontains=self.filter)
                    | Q(treasury_account__federal_account__account_title__icontains=self.filter)
                    | Q(treasury_account__federal_account__federal_account_code__icontains=self.filter)
                ),
            )

        results = (
            (FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters))
            .values(
                "treasury_account__tas_rendering_label",
                "treasury_account__account_title",
                "treasury_account__federal_account__account_title",
                "treasury_account__federal_account__federal_account_code",
            )
            .annotate(
                obligated_amount=Sum("obligations_incurred_by_program_object_class_cpe"),
                gross_outlay_amount=Sum("gross_outlay_amount_by_program_object_class_cpe"),
            )
        )
        return results
