import ast
from collections import OrderedDict
from django.db.models import F, Func, OuterRef, Q, Subquery, Sum
from django.utils.dateparse import parse_date
from fiscalyear import FiscalDateTime
from rest_framework.response import Response

from usaspending_api.accounts.models import AppropriationAccountBalances, FederalAccount, TreasuryAppropriationAccount
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC, DOD_CGAC


class ObjectClassFederalAccountsViewSet(APIDocumentationView):
    """Returns financial spending data by object class.
    endpoint_doc: /federal_account/avalible_object_classes.md"""

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


class FiscalYearSnapshotFederalAccountsViewSet(APIDocumentationView):
    """
    This route sends a request to the backend to retrieve budget information for a federal account.
    If no fiscal year is used, the federal accounts most recent fiscal year is used.

    endpoint_doc: /federal_account/fiscal_year_snapshot.md
    """

    @cache_response()
    def get(self, request, pk, fy=0, format=None):
        fy = int(fy) or SubmissionAttributes.last_certified_fy()
        queryset = AppropriationAccountBalances.final_objects.filter(
            treasury_account_identifier__federal_account_id=int(pk)
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


class SpendingOverTimeFederalAccountsViewSet(APIDocumentationView):
    """
    This route takes a federal_account DB ID and returns the data reqired to visualized the spending over time graphic.

    endpoint_doc: /federal_account/spending_over_time.md
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

        financial_account_queryset = AppropriationAccountBalances.final_objects.filter(
            treasury_account_identifier__federal_account_id=int(pk)
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
                        "obligations_incurred_filtered": trans["obligations_incurred_filtered"]
                        if trans["obligations_incurred_filtered"]
                        else 0,
                        "obligations_incurred_other": 0,
                        "unobliged_balance": 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": group_results[key]["outlay"] + trans["outlay"]
                        if trans["outlay"]
                        else group_results[key]["outlay"],
                        "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"]
                        + trans["obligations_incurred_filtered"]
                        if trans["obligations_incurred_filtered"]
                        else group_results[key]["obligations_incurred_filtered"],
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
                        "obligations_incurred_other": trans["obligations_incurred_other"]
                        if trans["obligations_incurred_other"]
                        else 0,
                        "unobliged_balance": trans["unobliged_balance"] if trans["unobliged_balance"] else 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": group_results[key]["outlay"],
                        "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"],
                        "obligations_incurred_other": group_results[key]["obligations_incurred_other"]
                        + trans["obligations_incurred_other"]
                        if trans["obligations_incurred_other"]
                        else group_results[key]["obligations_incurred_other"],
                        "unobliged_balance": group_results[key]["unobliged_balance"] + trans["unobliged_balance"]
                        if trans["unobliged_balance"]
                        else group_results[key]["unobliged_balance"],
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
                        "obligations_incurred_filtered": trans["obligations_incurred_filtered"]
                        if trans["obligations_incurred_filtered"]
                        else 0,
                        "obligations_incurred_other": 0,
                        "unobliged_balance": 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": group_results[key]["outlay"] + trans["outlay"]
                        if trans["outlay"]
                        else group_results[key]["outlay"],
                        "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"]
                        + trans["obligations_incurred_filtered"]
                        if trans["obligations_incurred_filtered"]
                        else group_results[key]["obligations_incurred_filtered"],
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
                        "obligations_incurred_other": trans["obligations_incurred_other"]
                        if trans["obligations_incurred_other"]
                        else 0,
                        "unobliged_balance": trans["unobliged_balance"] if trans["unobliged_balance"] else 0,
                    }
                else:
                    group_results[key] = {
                        "outlay": group_results[key]["outlay"],
                        "obligations_incurred_filtered": group_results[key]["obligations_incurred_filtered"],
                        "obligations_incurred_other": group_results[key]["obligations_incurred_other"]
                        + trans["obligations_incurred_other"]
                        if trans["obligations_incurred_other"]
                        else group_results[key]["obligations_incurred_other"],
                        "unobliged_balance": group_results[key]["unobliged_balance"] + trans["unobliged_balance"]
                        if trans["unobliged_balance"]
                        else group_results[key]["unobliged_balance"],
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
            key=lambda k: (ast.literal_eval(k[0])["fiscal_year"], int(ast.literal_eval(k[0])[nested_order]))
            if nested_order
            else (ast.literal_eval(k[0])["fiscal_year"]),
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
        for (key, values) in filter.items():
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
    for (key, values) in filters.items():
        if key == "object_class":
            result &= orred_filter_list(prefix=extra + "object_class", subfilters=values)
        elif key == "program_activity":
            result &= filter_on(extra + "program_activity", "program_activity_code", values)
        elif key == "time_period":
            result &= orred_date_filter_list(date_ranges=values)
    return result


class SpendingByCategoryFederalAccountsViewSet(APIDocumentationView):
    """
    This route takes a federal_account DB ID and returns the data required to visualized
    the Spending By Category graphic.

    endpoint_doc: /federal_account/spending_by_category.md
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


class FederalAccountViewSet(APIDocumentationView):
    """
    This route sends a request to the backend to retrieve a federal account based on its federal_account_code.
    endpoint_doc: /federal_account/federal_account.md
    """

    @cache_response()
    def get(self, request, fed_acct_code, format=None):
        federal_account = FederalAccount.objects.filter(federal_account_code=fed_acct_code).values(
            "id", "agency_identifier", "main_account_code", "account_title", "federal_account_code"
        )

        if not federal_account:
            raise InvalidParameterException("Federal Account with code {} does not exist.".format(fed_acct_code))

        return Response(federal_account[0])


class FederalAccountsViewSet(APIDocumentationView):
    """
    This route sends a request to the backend to retrieve a list of federal accounts.
    endpoint_doc: /federal_account/federal_account.md
    """

    def _parse_and_validate_request(self, request_dict):
        """ Validate the Request object includes the required fields """
        fy_range = [str(i) for i in range(2001, FiscalDateTime.today().year + 1)]
        last_fy = str(SubmissionAttributes.last_certified_fy()) or str(FiscalDateTime.today().year)
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
        """ Return all high-level Federal Account information """
        request_data = self._parse_and_validate_request(request.data)
        limit = request_data["limit"]
        page = request_data["page"]
        sort_field = request_data["sort"]["field"]
        sort_direction = request_data["sort"]["direction"]
        keyword = request_data.get("keyword", None)
        fy = request_data["filters"]["fy"]
        agency_id = request_data["filters"].get("agency_identifier", None)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        agency_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef("corrected_agency_identifier"))
        queryset = (
            FederalAccount.objects.filter(
                treasuryappropriationaccount__account_balances__final_of_fy=True,
                treasuryappropriationaccount__account_balances__submission__reporting_period_start__fy=fy,
            )
            .annotate(corrected_agency_identifier=Func(F("agency_identifier"), function="CORRECTED_CGAC"))
            .annotate(
                account_id=F("id"),
                account_name=F("account_title"),
                account_number=F("federal_account_code"),
                budgetary_resources=Sum(
                    "treasuryappropriationaccount__account_balances__total_budgetary_resources_amount_cpe"
                ),
                managing_agency=Subquery(agency_subquery.values("name")[:1]),
                managing_agency_acronym=Subquery(agency_subquery.values("abbreviation")[:1]),
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

        if agency_id is not None:
            tta_list = DOD_ARMED_FORCES_CGAC if agency_id == DOD_CGAC else [agency_id]
            tta_filter = Q()
            for tta in tta_list:
                tta_filter |= Q(account_number__startswith=tta)
            queryset &= queryset.filter(tta_filter)

        if sort_direction == "desc":
            queryset = queryset.order_by(F(sort_field).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(sort_field).asc())

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
