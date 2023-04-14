from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any, List
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.submissions.helpers import get_latest_submission_ids_for_fiscal_year
from django.db.models import Q, Sum, F
from usaspending_api.references.models import ObjectClass
from usaspending_api.references.models import RefProgramActivity


class TASObjectClassList(PaginationMixin, AgencyBase):
    """
    Obtain the list of object classes for a specific agency's treasury
    account (TAS).
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/treasury_account/tas/object_class.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "filter"]
        self._submission_ids = None

    @property
    def tas_rendering_label(self):
        return self.kwargs["tas"]

    @property
    def submission_ids(self):
        if self._submission_ids is not None:
            return self._submission_ids
        self._submission_ids = get_latest_submission_ids_for_fiscal_year(self.fiscal_year)
        return self._submission_ids

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "obligated_amount", "gross_outlay_amount"]
        self.default_sort_column = "obligated_amount"
        order = self.pagination.sort_order == "desc"
        results = list(self.get_object_class_list())
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        pagination_limit_results = results[: self.pagination.limit]
        response_dict = {
            "treasury_account_symbol": self.tas_rendering_label,
            "fiscal_year": self.fiscal_year,
            "page_metadata": page_metadata,
            "results": pagination_limit_results,
            "messages": self.standard_response_messages,
        }
        # There isn't a direct relationship between ObjectClass and RefProgramActivity
        # That's why we opted to query for RefProgramActivity for each ObjectClass
        for idx, object_class_row in enumerate(pagination_limit_results):
            program_activity_results = self.get_program_activity_by_object_class_list(object_class_row["id"])
            child_response_dict = self.format_program_activity_children_response(program_activity_results)
            # "id" column should not be in the response
            # Instead of adding another for loop to remove this column from the object class list results
            # Just delete it during an already existing loop over the list
            del response_dict["results"][idx]["id"]
            child_response_dict = sorted(child_response_dict, key=lambda x: x[self.pagination.sort_key], reverse=order)
            response_dict["results"][idx]["children"] = child_response_dict

        return Response(response_dict)

    def get_object_class_list(self) -> List[dict]:
        filters = [
            Q(financialaccountsbyprogramactivityobjectclass__submission_id__in=self.submission_ids),
            Q(
                financialaccountsbyprogramactivityobjectclass__treasury_account__tas_rendering_label=self.tas_rendering_label
            ),
            # Filters are consistent with object class by agency, DEV-4923
            Q(
                Q(financialaccountsbyprogramactivityobjectclass__obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(
                    financialaccountsbyprogramactivityobjectclass__obligations_incurred_by_program_object_class_cpe__lt=0
                )
                | Q(
                    financialaccountsbyprogramactivityobjectclass__gross_outlay_amount_by_program_object_class_cpe__gt=0
                )
                | Q(
                    financialaccountsbyprogramactivityobjectclass__gross_outlay_amount_by_program_object_class_cpe__lt=0
                )
            ),
        ]
        if self.filter:
            filters.append(Q(object_class_name__icontains=self.filter))
        queryset_results = (
            ObjectClass.objects.filter(*filters)
            .annotate(
                name=F("object_class_name"),
                obligated_amount=Sum(
                    "financialaccountsbyprogramactivityobjectclass__obligations_incurred_by_program_object_class_cpe"
                ),
                gross_outlay_amount=Sum(
                    "financialaccountsbyprogramactivityobjectclass__gross_outlay_amount_by_program_object_class_cpe"
                ),
            )
            .order_by(f"{'-' if self.pagination.sort_order == 'desc' else ''}{self.pagination.sort_key}")
            .values("id", "name", "obligated_amount", "gross_outlay_amount")
        )
        return queryset_results

    def get_program_activity_by_object_class_list(self, object_class_id) -> List[dict]:
        filters = [
            Q(financialaccountsbyprogramactivityobjectclass__submission_id__in=self.submission_ids),
            Q(financialaccountsbyprogramactivityobjectclass__object_class__id=object_class_id),
            Q(
                financialaccountsbyprogramactivityobjectclass__treasury_account__tas_rendering_label=self.tas_rendering_label
            ),
            Q(
                Q(financialaccountsbyprogramactivityobjectclass__obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(
                    financialaccountsbyprogramactivityobjectclass__obligations_incurred_by_program_object_class_cpe__lt=0
                )
                | Q(
                    financialaccountsbyprogramactivityobjectclass__gross_outlay_amount_by_program_object_class_cpe__gt=0
                )
                | Q(
                    financialaccountsbyprogramactivityobjectclass__gross_outlay_amount_by_program_object_class_cpe__lt=0
                )
            ),
        ]
        queryset_results = (
            RefProgramActivity.objects.filter(*filters)
            .annotate(
                name=F("program_activity_name"),
                obligated_amount=Sum(
                    "financialaccountsbyprogramactivityobjectclass__obligations_incurred_by_program_object_class_cpe"
                ),
                gross_outlay_amount=Sum(
                    "financialaccountsbyprogramactivityobjectclass__gross_outlay_amount_by_program_object_class_cpe"
                ),
            )
            .order_by(f"{'-' if self.pagination.sort_order == 'desc' else ''}{self.pagination.sort_key}")
            .values("name", "obligated_amount", "gross_outlay_amount")
        )
        return queryset_results

    def format_program_activity_children_response(self, children):
        response = []
        for child in children:
            response.append(
                {
                    "name": child.get("name"),
                    "obligated_amount": child.get("obligated_amount"),
                    "gross_outlay_amount": child.get("gross_outlay_amount"),
                }
            )
        return response
