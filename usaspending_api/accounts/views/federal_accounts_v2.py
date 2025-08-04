import ast
from collections import OrderedDict
from typing import List

from django.db.models import DecimalField, Exists, F, Func, OuterRef, Q, Subquery, Sum
from django.utils.dateparse import parse_date
from django.utils.functional import cached_property
from django_cte import With
from fiscalyear import FiscalDateTime
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.accounts.models import AppropriationAccountBalances, FederalAccount, TreasuryAppropriationAccount
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models.bureau_title_lookup import BureauTitleLookup
from usaspending_api.submissions.models import SubmissionAttributes


class ObjectClassFederalAccountsViewSet(APIView):
    """
    Returns financial spending data by object class.
    """

    endpoint_doc = (
        "usaspending_api/api_contracts/contracts/v2/federal_accounts/federal_account_id/available_object_classes.md"
    )

    @cache_response()
    def get(self, request, pk, format=None):
        """Return the view's queryset."""
        # create response
        response = {"results": {}}

        # get federal account id from url
        fa_id = int(pk)

        # get FA row
        fa = FederalAccount.objects.filter(id=fa_id).first()
        if fa is None:
            return Response(response)

        # get tas related to FA
        tas_ids = TreasuryAppropriationAccount.objects.filter(federal_account=fa).values_list(
            "treasury_account_identifier", flat=True
        )

        # get fin based on tas, select oc, make distinct values
        financial_account_queryset = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(treasury_account__in=tas_ids)
            .select_related("object_class")
            .distinct("object_class")
        )

        # Retrieve only unique major class ids and names
        major_classes = {
            (obj.object_class.major_object_class, obj.object_class.major_object_class_name)
            for obj in financial_account_queryset
        }
        result = [
            {
                "id": maj[0],
                "name": maj[1],
                "minor_object_class": [
                    {"id": obj[0], "name": obj[1]}
                    for obj in {
                        (oc.object_class.object_class, oc.object_class.object_class_name)
                        for oc in financial_account_queryset
                        if oc.object_class.major_object_class == maj[0]
                    }
                ],
            }
            for maj in major_classes
        ]
        return Response({"results": result})


class FiscalYearSnapshotFederalAccountsViewSet(APIView):
    """
    This route sends a request to the backend to retrieve budget information for a federal account.
    If no fiscal year is used, the federal accounts most recent fiscal year is used.
    """

    endpoint_doc = (
        "usaspending_api/api_contracts/contracts/v2/federal_accounts/federal_account_id/fiscal_year_snapshot.md"
    )

    @cache_response()
    def get(self, request, pk, fy=0, format=None):
        fy = int(fy) or SubmissionAttributes.latest_available_fy()
        queryset = AppropriationAccountBalances.objects.filter(
            submission__is_final_balances_for_fy=True, treasury_account_identifier__federal_account_id=int(pk)
        ).filter(submission__reporting_fiscal_year=fy)
        queryset = queryset.aggregate(
            outlay=Sum("gross_outlay_amount_by_tas_cpe"),
            budget_authority=Sum("total_budgetary_resources_amount_cpe"),
            obligated=Sum("obligations_incurred_total_by_tas_cpe"),
            unobligated=Sum("unobligated_balance_cpe"),
            balance_brought_forward=Sum(
                F("budget_authority_unobligated_balance_brought_forward_fyb")
                + F("adjustments_to_unobligated_balance_brought_forward_cpe")
            ),
            other_budgetary_resources=Sum("other_budgetary_resources_amount_cpe"),
            appropriations=Sum("budget_authority_appropriated_amount_cpe"),
        )
        if queryset["outlay"] is not None:
            name = FederalAccount.objects.filter(id=int(pk)).values("account_title").first()["account_title"]
            queryset["name"] = name
            return Response({"results": queryset})
        else:
            return Response({})


class SpendingOverTimeFederalAccountsViewSet(APIView):
    """
    This route takes a federal_account DB ID and returns the data required to visualized the spending over time graphic.
    """

    @cache_response()
    def post(self, request, pk, format=None):
        # create response
        response = {"results": {}}

        # get federal account id from url
        json_request = request.data
        group = json_request.get("group", None)
        filters = json_request.get("filters", None)
        if filters:
            filters = federal_account_filter(filters, "treasury_account_identifier__program_balances__")

        nested_order = ""
        group_results = OrderedDict()  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000

        financial_account_queryset = AppropriationAccountBalances.objects.filter(
            submission__is_final_balances_for_fy=True, treasury_account_identifier__federal_account_id=int(pk)
        )
        if group == "fy" or group == "fiscal_year":

            filtered_fa = financial_account_queryset
            if filters:
                filtered_fa = filtered_fa.filter(filters)
            filtered_fa = filtered_fa.annotate(
                outlay=F("gross_outlay_amount_by_tas_cpe"),
                obligations_incurred_filtered=F("obligations_incurred_total_by_tas_cpe"),
            ).values(
                "appropriation_account_balances_id",
                "submission__reporting_fiscal_year",
                "outlay",
                "obligations_incurred_filtered",
            )

            unfiltered_fa = financial_account_queryset.annotate(
                obligations_incurred_other=F("obligations_incurred_total_by_tas_cpe"),
                unobliged_balance=F("unobligated_balance_cpe"),
            ).values(
                "appropriation_account_balances_id",
                "submission__reporting_fiscal_year",
                "obligations_incurred_other",
                "unobliged_balance",
            )

            for trans in filtered_fa:
                if trans["submission__reporting_fiscal_year"] is None:
                    continue
                fy = trans["submission__reporting_fiscal_year"]

                key = {"fiscal_year": str(fy)}
                key = str(key)
                if key not in group_results:
                    group_results[key] = {
                        "outlay": trans["outlay"] if trans["outlay"] else 0,
                        "obligations_incurred_filtered": (
                            trans["obligations_incurred_filtered"] if trans["obligations_incurred_filtered"] else 0
                        ),
                        "obligations_incurred_other": 0,
                        "unobliged_balance": 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": (
                            group_results[key]["outlay"] + trans["outlay"]
                            if trans["outlay"]
                            else group_results[key]["outlay"]
                        ),
                        "obligations_incurred_filtered": (
                            group_results[key]["obligations_incurred_filtered"] + trans["obligations_incurred_filtered"]
                            if trans["obligations_incurred_filtered"]
                            else group_results[key]["obligations_incurred_filtered"]
                        ),
                        "obligations_incurred_other": group_results[key]["obligations_incurred_other"],
                        "unobliged_balance": group_results[key]["unobliged_balance"],
                    }

            for trans in unfiltered_fa:
                if trans["submission__reporting_fiscal_year"] is None:
                    continue
                fy = trans["submission__reporting_fiscal_year"]
                key = {"fiscal_year": str(fy)}
                key = str(key)

                if key not in group_results:
                    group_results[key] = {
                        "outlay": 0,
                        "obligations_incurred_filtered": 0,
                        "obligations_incurred_other": (
                            trans["obligations_incurred_other"] if trans["obligations_incurred_other"] else 0
                        ),
                        "unobliged_balance": trans["unobliged_balance"] if trans["unobliged_balance"] else 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": group_results[key]["outlay"],
                        "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"],
                        "obligations_incurred_other": (
                            group_results[key]["obligations_incurred_other"] + trans["obligations_incurred_other"]
                            if trans["obligations_incurred_other"]
                            else group_results[key]["obligations_incurred_other"]
                        ),
                        "unobliged_balance": (
                            group_results[key]["unobliged_balance"] + trans["unobliged_balance"]
                            if trans["unobliged_balance"]
                            else group_results[key]["unobliged_balance"]
                        ),
                    }

        else:  # quarterly, take months and add them up
            filtered_fa = financial_account_queryset
            if filters:
                filtered_fa = filtered_fa.filter(filters)
            filtered_fa = filtered_fa.annotate(
                outlay=F("gross_outlay_amount_by_tas_cpe"),
                obligations_incurred_filtered=F("obligations_incurred_total_by_tas_cpe"),
            ).values(
                "appropriation_account_balances_id",
                "submission__reporting_fiscal_year",
                "submission__reporting_fiscal_quarter",
                "outlay",
                "obligations_incurred_filtered",
            )

            unfiltered_fa = financial_account_queryset.annotate(
                obligations_incurred_other=F("obligations_incurred_total_by_tas_cpe"),
                unobliged_balance=F("unobligated_balance_cpe"),
            ).values(
                "appropriation_account_balances_id",
                "submission__reporting_fiscal_year",
                "submission__reporting_fiscal_quarter",
                "obligations_incurred_other",
                "unobliged_balance",
            )
            for trans in filtered_fa:
                if trans["submission__reporting_fiscal_year"] is None:
                    continue
                fy = trans["submission__reporting_fiscal_year"]
                fq = trans["submission__reporting_fiscal_quarter"]
                key = {"fiscal_year": str(fy), "quarter": str(fq)}
                key = str(key)
                if key not in group_results:
                    group_results[key] = {
                        "outlay": trans["outlay"] if trans["outlay"] else 0,
                        "obligations_incurred_filtered": (
                            trans["obligations_incurred_filtered"] if trans["obligations_incurred_filtered"] else 0
                        ),
                        "obligations_incurred_other": 0,
                        "unobliged_balance": 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": (
                            group_results[key]["outlay"] + trans["outlay"]
                            if trans["outlay"]
                            else group_results[key]["outlay"]
                        ),
                        "obligations_incurred_filtered": (
                            group_results[key]["obligations_incurred_filtered"] + trans["obligations_incurred_filtered"]
                            if trans["obligations_incurred_filtered"]
                            else group_results[key]["obligations_incurred_filtered"]
                        ),
                        "obligations_incurred_other": group_results[key]["obligations_incurred_other"],
                        "unobliged_balance": group_results[key]["unobliged_balance"],
                    }
            for trans in unfiltered_fa:
                if trans["submission__reporting_fiscal_year"] is None:
                    continue
                fy = trans["submission__reporting_fiscal_year"]
                fq = trans["submission__reporting_fiscal_quarter"]
                key = {"fiscal_year": str(fy), "quarter": str(fq)}
                key = str(key)
                if key not in group_results:
                    group_results[key] = {
                        "outlay": 0,
                        "obligations_incurred_filtered": 0,
                        "obligations_incurred_other": (
                            trans["obligations_incurred_other"] if trans["obligations_incurred_other"] else 0
                        ),
                        "unobliged_balance": trans["unobliged_balance"] if trans["unobliged_balance"] else 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": group_results[key]["outlay"],
                        "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"],
                        "obligations_incurred_other": (
                            group_results[key]["obligations_incurred_other"] + trans["obligations_incurred_other"]
                            if trans["obligations_incurred_other"]
                            else group_results[key]["obligations_incurred_other"]
                        ),
                        "unobliged_balance": (
                            group_results[key]["unobliged_balance"] + trans["unobliged_balance"]
                            if trans["unobliged_balance"]
                            else group_results[key]["unobliged_balance"]
                        ),
                    }
            nested_order = "quarter"

        # convert result into expected format, sort by key to meet front-end specs
        results = []
        # Expected results structure
        # [{
        # 'time_period': {'fy': '2017', 'quarter': '3'},
        # 	'aggregated_amount': '200000000'
        # }]
        sorted_group_results = sorted(
            group_results.items(),
            key=lambda k: (
                (ast.literal_eval(k[0])["fiscal_year"], int(ast.literal_eval(k[0])[nested_order]))
                if nested_order
                else (ast.literal_eval(k[0])["fiscal_year"])
            ),
        )

        for key, value in sorted_group_results:
            key_dict = ast.literal_eval(key)
            value["time_period"] = key_dict
            result = value
            results.append(result)
        response["results"] = results

        return Response(response)


def filter_on(prefix, key, values):
    if not isinstance(values, (list, tuple)):
        values = [values]
    return Q(**{"{}__{}__in".format(prefix, key): values})


def orred_filter_list(prefix, subfilters):
    """Produces Q-object for a list of dicts

    Each dict's (key: value) pairs are ANDed together (rows must satisfy all k:v)
    List items are ORred together (satisfying any one is enough)
    """
    result = Q()
    for filter in subfilters:
        subresult = Q()
        for key, values in filter.items():
            subresult &= filter_on(prefix, key, values)
        result |= subresult
    return result


def orred_date_filter_list(date_ranges):
    """Produces Q-object for a list of dicts, each of which may include start and/or end date

    Each dict's (key: value) pairs are ANDed together (rows must satisfy all k:v)
    List items are ORred together (satisfying any one is enough)
    """
    result = Q()
    for date_range in date_ranges:
        subresult = Q()
        if "start_date" in date_range:
            start_date = parse_date(date_range["start_date"])
            subresult &= Q(reporting_period_start__gte=start_date)
        if "end_date" in date_range:
            end_date = parse_date(date_range["end_date"])
            subresult &= Q(reporting_period_end__lte=end_date)
        result |= subresult
    return result


def federal_account_filter(filters, extra=""):

    result = Q()
    for key, values in filters.items():
        if key == "object_class":
            result &= orred_filter_list(prefix=extra + "object_class", subfilters=values)
        elif key == "program_activity":
            result &= filter_on(extra + "program_activity", "program_activity_code", values)
        elif key == "time_period":
            result &= orred_date_filter_list(date_ranges=values)
    return result


class SpendingByCategoryFederalAccountsViewSet(APIView):
    """
    This route takes a federal_account DB ID and returns the data required to visualized
    the Spending By Category graphic.
    """

    @cache_response()
    def post(self, request, pk, format=None):
        json_request = request.data
        filters = json_request.get("filters", None)

        # get fin based on tas, select oc, make distinct values
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.filter(
            treasury_account__federal_account_id=int(pk)
        )

        # `category` from request determines what to sum over
        # `.annotate`... `F()` is much like using SQL column aliases
        if request.data.get("category") == "program_activity":
            queryset = queryset.annotate(
                id=F("program_activity_id"),
                code=F("program_activity__program_activity_code"),
                name=F("program_activity__program_activity_name"),
            )
        elif request.data.get("category") == "object_class":
            queryset = queryset.annotate(
                id=F("object_class_id"), code=F("object_class__object_class"), name=F("object_class__object_class_name")
            )
        elif request.data.get("category") == "treasury_account":
            queryset = queryset.annotate(
                id=F("treasury_account_id"),
                code=F("treasury_account__treasury_account_identifier"),
                name=F("treasury_account__tas_rendering_label"),
            )
        else:
            raise InvalidParameterException("category must be one of: program_activity, object_class, treasury_account")
        if filters:
            filters = federal_account_filter(filters)
            queryset = queryset.filter(filters)

        queryset = (
            queryset.values("id", "code", "name")
            .annotate(Sum("obligations_incurred_by_program_object_class_cpe"))
            .order_by("-obligations_incurred_by_program_object_class_cpe__sum")
        )

        result = {"results": {q["name"]: q["obligations_incurred_by_program_object_class_cpe__sum"] for q in queryset}}

        return Response(result)


class FederalAccountViewSet(APIView):
    """
    This route sends a request to the backend to retrieve a federal account based on its federal_account_code.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/federal_accounts/account_number.md"

    @cached_property
    def federal_account(self):
        federal_account_code_param = self.kwargs["federal_account_code"]

        # Get the bureau_title associated to the current Federal Account code
        bureau_name_subquery = Subquery(
            BureauTitleLookup.objects.filter(federal_account_code=federal_account_code_param).values("bureau_title")
        )

        # Get the bureau_slug associated to the current Federal Account code
        bureau_slug_subquery = Subquery(
            BureauTitleLookup.objects.filter(federal_account_code=federal_account_code_param).values("bureau_slug")
        )

        federal_account = (
            FederalAccount.objects.filter(federal_account_code=federal_account_code_param)
            .annotate(
                parent_agency_toptier_code=F("parent_toptier_agency__toptier_code"),
                parent_agency_name=F("parent_toptier_agency__name"),
            )
            .values(
                "id",
                "agency_identifier",
                "main_account_code",
                "account_title",
                "federal_account_code",
                "parent_agency_toptier_code",
                "parent_agency_name",
            )
            .annotate(bureau_slug=bureau_slug_subquery, bureau_name=bureau_name_subquery)
            .first()
        )
        if not federal_account:
            raise InvalidParameterException(
                f"Cannot find Federal Account with the code of '{federal_account_code_param}'"
            )
        return federal_account

    def _get_treasury_accounts(self) -> List[dict]:
        """Get the Treasury Accounts (using File A and B) associated with the given Federal Account during a specific
            fiscal year.

        Returns:
            List[dict]: List of Treasury Accounts
            or
            None: No Treasury Accounts were found for the given Federal Account in the given Fiscal Year
        """

        submission_ids = list(
            SubmissionAttributes.objects.filter(
                reporting_fiscal_year=self.fiscal_year,
                submission_window__submission_reveal_date__lte=now(),
                is_final_balances_for_fy=True,
            ).values_list("submission_id", flat=True)
        )

        # If no applicable submission IDs are found, return an empty list
        if not submission_ids:
            return []

        file_b_calculations = FileBCalculations()

        fabpaoc_query_filters = [
            file_b_calculations.is_non_zero_total_spending(),
            Q(submission_id__in=submission_ids),
            Q(treasury_account__federal_account__federal_account_code=self.federal_account["federal_account_code"]),
        ]

        tbr_query_filters = [
            Q(submission_id__in=submission_ids),
            Q(
                treasury_account_identifier__federal_account__federal_account_code=self.federal_account[
                    "federal_account_code"
                ]
            ),
            Q(Q(total_budgetary_resources_amount_cpe__gt=0) | Q(total_budgetary_resources_amount_cpe__lt=0)),
        ]

        tbr_cte = With(
            AppropriationAccountBalances.objects.filter(*tbr_query_filters)
            .values("treasury_account_identifier")
            .annotate(total_budgetary_resources=Sum("total_budgetary_resources_amount_cpe"))
        )

        results = (
            # Join the total budgetary resources from File A to the corresponding Treasury Account in File B
            tbr_cte.join(
                (
                    FinancialAccountsByProgramActivityObjectClass.objects.filter(*fabpaoc_query_filters)
                    .values(
                        name=F("treasury_account__account_title"),
                        code=F("treasury_account__tas_rendering_label"),
                    )
                    .annotate(
                        obligated_amount=Sum(file_b_calculations.get_obligations()),
                        gross_outlay_amount=Sum(file_b_calculations.get_outlays()),
                    )
                ),
                treasury_account__treasury_account_identifier=tbr_cte.col.treasury_account_identifier,
            )
            .with_cte(tbr_cte)
            .annotate(budgetary_resources_amount=tbr_cte.col.total_budgetary_resources)
        )

        return results

    @cache_response()
    def get(self, request: Request, federal_account_code: str, format=None):
        self.fiscal_year = self.request.query_params.get("fiscal_year", current_fiscal_year())
        child_treasury_accounts = self._get_treasury_accounts()

        if len(child_treasury_accounts) > 0:
            fa_obligated_amount = sum(entry["obligated_amount"] for entry in child_treasury_accounts)
            fa_gross_outlay_amount = sum(entry["gross_outlay_amount"] for entry in child_treasury_accounts)
            fa_total_budgetary_resources = sum(entry["budgetary_resources_amount"] for entry in child_treasury_accounts)
        else:
            fa_obligated_amount = None
            fa_gross_outlay_amount = None
            fa_total_budgetary_resources = None

        return Response(
            {
                "fiscal_year": self.fiscal_year,
                "id": self.federal_account["id"],
                "agency_identifier": self.federal_account["agency_identifier"],
                "main_account_code": self.federal_account["main_account_code"],
                "account_title": self.federal_account["account_title"],
                "federal_account_code": self.federal_account["federal_account_code"],
                "parent_agency_toptier_code": self.federal_account["parent_agency_toptier_code"],
                "parent_agency_name": self.federal_account["parent_agency_name"],
                "bureau_name": self.federal_account["bureau_name"],
                "bureau_slug": self.federal_account["bureau_slug"],
                "total_obligated_amount": fa_obligated_amount,
                "total_gross_outlay_amount": fa_gross_outlay_amount,
                "total_budgetary_resources": fa_total_budgetary_resources,
                "children": child_treasury_accounts,
            }
        )


class FederalAccountsViewSet(APIView):
    """
    This route sends a request to the backend to retrieve a list of federal accounts.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/federal_accounts.md"

    def _parse_and_validate_request(self, request_dict):
        """Validate the Request object includes the required fields"""
        fy_range = [str(i) for i in range(2001, FiscalDateTime.today().year + 1)]
        last_fy = str(SubmissionAttributes.latest_available_fy()) or str(FiscalDateTime.today().year)
        request_settings = [
            {
                "key": "sort",
                "name": "sort",
                "type": "object",
                "optional": True,
                "object_keys": {
                    "field": {
                        "type": "enum",
                        "enum_values": ["budgetary_resources", "managing_agency", "account_name", "account_number"],
                        "optional": True,
                        "default": "budgetary_resources",
                    },
                    "direction": {"type": "enum", "enum_values": ["asc", "desc"], "optional": True, "default": "asc"},
                },
                "default": {"field": "budgetary_resources", "direction": "asc"},
            },
            {"key": "page", "name": "page", "type": "integer", "default": 1, "min": 1, "optional": True},
            {"key": "limit", "name": "limit", "type": "integer", "default": 10, "min": 1, "max": 100, "optional": True},
            {
                "key": "filters",
                "name": "filters",
                "type": "object",
                "optional": True,
                "object_keys": {
                    "agency_identifier": {
                        "name": "agency_identifier",
                        "type": "text",
                        "text_type": "search",
                        "optional": True,
                    },
                    "fy": {"type": "enum", "enum_values": fy_range, "optional": True, "default": last_fy},
                },
                "default": {"fy": last_fy},
            },
            {"key": "keyword", "name": "keyword", "type": "text", "text_type": "search", "optional": True},
        ]

        validated_request_data = TinyShield(request_settings).block(request_dict)
        validated_request_data["filters"]["fy"] = validated_request_data["filters"].get("fy", last_fy)
        return validated_request_data

    @cache_response()
    def post(self, request, format=None):
        """Return all high-level Federal Account information"""
        request_data = self._parse_and_validate_request(request.data)
        limit = request_data["limit"]
        page = request_data["page"]
        sort_field = request_data["sort"]["field"]
        sort_direction = request_data["sort"]["direction"]
        keyword = request_data.get("keyword", None)
        fy = request_data["filters"]["fy"]
        agency_id = request_data["filters"].get("agency_identifier")

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        account_filter = {}
        if agency_id:
            account_filter["parent_toptier_agency__toptier_code"] = agency_id

        # Only return federal accounts that have ever had a submission.  Only return budgetary_resources for
        # the fiscal year requested.  Note that we use Func instead of Sum below because Sum wants to perform a
        # grouping when we don't want one.
        queryset = (
            FederalAccount.objects.annotate(
                has_submission=Exists(
                    SubmissionAttributes.objects.filter(
                        appropriationaccountbalances__treasury_account_identifier__federal_account_id=OuterRef("id")
                    ).values("pk")
                )
            )
            .filter(**account_filter, has_submission=True)
            .annotate(
                account_id=F("id"),
                account_name=F("account_title"),
                account_number=F("federal_account_code"),
                budgetary_resources=Subquery(
                    AppropriationAccountBalances.objects.filter(
                        submission__reporting_fiscal_year=fy,
                        submission__is_final_balances_for_fy=True,
                        treasury_account_identifier__federal_account_id=OuterRef("id"),
                    )
                    .annotate(the_sum=Func(F("total_budgetary_resources_amount_cpe"), function="SUM"))
                    .values("the_sum"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                managing_agency=F("parent_toptier_agency__name"),
                managing_agency_acronym=F("parent_toptier_agency__abbreviation"),
            )
        )

        # add keyword filter, if it exists
        if keyword:
            queryset = queryset.filter(
                Q(account_name__icontains=keyword)
                | Q(account_number__contains=keyword)
                | Q(managing_agency__icontains=keyword)
                | Q(managing_agency_acronym__contains=keyword.upper())
            )

        if sort_direction == "desc":
            queryset = queryset.order_by(F(sort_field).desc(nulls_last=True), "-federal_account_code")
        else:
            queryset = queryset.order_by(F(sort_field).asc(), "federal_account_code")

        result = {"count": queryset.count(), "limit": limit, "page": page, "fy": fy, "keyword": keyword}
        resultset = queryset.values(
            "account_id",
            "account_number",
            "account_name",
            "budgetary_resources",
            "agency_identifier",
            "managing_agency",
            "managing_agency_acronym",
        )
        resultset = resultset[lower_limit : upper_limit + 1]
        page_metadata = get_simple_pagination_metadata(len(resultset), limit, page)
        result.update(page_metadata)
        resultset = resultset[:limit]

        result["results"] = resultset
        return Response(result)
