from typing import List

from django.db.models import Sum, Q, F, Subquery, OuterRef
from django.utils.functional import cached_property
from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.exceptions import NotFound

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.accounts.models.appropriation_account_balances import AppropriationAccountBalances
from usaspending_api.accounts.models.federal_account import FederalAccount
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.submissions.helpers import get_latest_submission_ids_for_fiscal_year


class FederalAccountCode(APIView):
    """
    Obtain the list of treasury accounts for a specific federal account in a
    single fiscal year.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/federal_accounts/federal_account_code.md"

    @cached_property
    def federal_account(self):
        federal_account_code_param = self.kwargs['federal_account_code']

        federal_account = FederalAccount.objects.filter(federal_account_code=federal_account_code_param).first()
        if not federal_account:
            raise NotFound(f"Cannot find Federal Account with the code of '{federal_account_code_param}'")
        return federal_account


    @cache_response()
    def get(self, request: Request, federal_account_code: str):
        self.fiscal_year = self.request.query_params.get("fiscal_year", current_fiscal_year())
        child_treasury_accounts = self.get_treasury_accounts()
        fa_obligated_amount = sum(entry['obligated_amount'] for entry in child_treasury_accounts)
        fa_gross_outlay_amount = sum(entry['gross_outlay_amount'] for entry in child_treasury_accounts)

        return Response(
            {
                "fiscal_year": self.fiscal_year,
                "name": self.federal_account.account_title,
                "total_obligated_amount": fa_obligated_amount,
                "total_gross_outlay_amount": fa_gross_outlay_amount,
                "total_budgetary_resources": 0,  # TODO calculate this
                "children": child_treasury_accounts,
                "message": []  # TODO calculate this
            }
        )


    def get_treasury_accounts(self) -> List[dict]:
        """Get the Treasury Accounts (using File A and B) associated with the given Federal Account during a specific
            fiscal year.

        Returns:
            List[dict]: List of Treasury Accounts
        """

        submission_ids = get_latest_submission_ids_for_fiscal_year(self.fiscal_year)
        query_filters = [
            Q(submission__reporting_fiscal_year=self.fiscal_year),
            Q(submission_id__in=submission_ids),
            Q(treasury_account__federal_account__federal_account_code=self.federal_account.federal_account_code),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
        ]

        tbr_subquery = Subquery(AppropriationAccountBalances.objects.filter(
            Q(submission__reporting_fiscal_year=self.fiscal_year),
            Q(submission_id__in=submission_ids),
            Q(treasury_account_identifier__federal_account__federal_account_code=self.federal_account.federal_account_code),
            Q(treasury_account_identifier__tas_rendering_label=OuterRef('treasury_account__tas_rendering_label'))
        ).values("total_budgetary_resources_amount_cpe"))

        results = (
            (FinancialAccountsByProgramActivityObjectClass.objects.filter(*query_filters))
            .values(
                name = F('treasury_account__account_title'),
                code = F('treasury_account__tas_rendering_label'),
            )
            .annotate(
                obligated_amount = Sum("obligations_incurred_by_program_object_class_cpe"),
                gross_outlay_amount = Sum("gross_outlay_amount_by_program_object_class_cpe"),
                budgetary_resources_amount = Sum(tbr_subquery)
            )
        )
        return results
