from django.db.models import F, Sum, Case, Value, When
from usaspending_api.accounts.serializers import (
    ObjectClassFinancialSpendingSerializer,
    MinorObjectClassFinancialSpendingSerializer,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import Agency
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC, DOD_CGAC
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.exceptions import InvalidParameterException


class ObjectClassFinancialSpendingViewSet(CachedDetailViewSet):
    """
    Returns financial spending data by object class for the latest quarter based on the given fiscal year.
    endpoint_doc: /financial_spending/major_object_class.md
    """

    serializer_class = ObjectClassFinancialSpendingSerializer

    def get_queryset(self):
        # retrieve post request payload
        json_request = self.request.query_params

        # retrieve fiscal_year & agency_id from request
        fiscal_year = json_request.get("fiscal_year", None)
        funding_agency_id = json_request.get("funding_agency_id", None)

        # required query parameters were not provided
        if not (fiscal_year and funding_agency_id):
            raise InvalidParameterException(
                "Missing one or more required query parameters: fiscal_year, funding_agency_id"
            )

        toptier_agency = Agency.objects.filter(id=funding_agency_id).first()
        if toptier_agency is None:
            return FinancialAccountsByProgramActivityObjectClass.objects.none()
        toptier_agency = toptier_agency.toptier_agency

        submission_queryset = SubmissionAttributes.objects.all()
        submission_queryset = (
            submission_queryset.filter(cgac_code=toptier_agency.cgac_code, reporting_fiscal_year=fiscal_year)
            .order_by("-reporting_fiscal_year", "-reporting_fiscal_quarter")
            .annotate(fiscal_year=F("reporting_fiscal_year"), fiscal_quarter=F("reporting_fiscal_quarter"))
        )
        submission = submission_queryset.first()

        if submission is None:
            return FinancialAccountsByProgramActivityObjectClass.objects.none()
        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.fiscal_quarter

        # using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.all()
        # get the incoming agency's toptier agency, because that's what we'll
        # need to filter on
        # (used filter() instead of get() b/c we likely don't want to raise an
        # error on a bad agency id)
        # DS-1655: if the AID is "097" (DOD), Include the branches of the military in the queryset
        if toptier_agency.cgac_code == DOD_CGAC:
            tta_list = DOD_ARMED_FORCES_CGAC
            queryset = queryset.filter(
                submission__reporting_fiscal_year=active_fiscal_year,
                submission__reporting_fiscal_quarter=active_fiscal_quarter,
                treasury_account__funding_toptier_agency__cgac_code__in=tta_list,
            )
        else:
            queryset = queryset.filter(
                submission__reporting_fiscal_year=active_fiscal_year,
                submission__reporting_fiscal_quarter=active_fiscal_quarter,
                treasury_account__funding_toptier_agency=toptier_agency,
            )
        # Special case: major object class name for class 00 should be reported
        # as Unknown Object Type, overriding actual value in database
        queryset = queryset.annotate(
            major_object_class_name=Case(
                When(object_class__major_object_class="00", then=Value("Unknown Object Type")),
                default="object_class__major_object_class_name",
            ),
            major_object_class_code=F("object_class__major_object_class"),
        )
        # sum obligated_mount by object class
        queryset = queryset.values("major_object_class_name", "major_object_class_code").annotate(
            obligated_amount=Sum("obligations_incurred_by_program_object_class_cpe")
        )
        # get minor object class vars

        return queryset


class MinorObjectClassFinancialSpendingViewSet(CachedDetailViewSet):
    """
    Returns financial spending data by object class for the latest quarter in the given fiscal year.
    endpoint_doc: /financial_spending/object_class.md
    """

    serializer_class = MinorObjectClassFinancialSpendingSerializer

    def get_queryset(self):
        # retrieve post request payload
        json_request = self.request.query_params

        # retrieve fiscal_year & agency_id from request
        fiscal_year = json_request.get("fiscal_year", None)
        funding_agency_id = json_request.get("funding_agency_id", None)
        major_object_class_code = json_request.get("major_object_class_code", None)

        # required query parameters were not provided
        if not (fiscal_year and funding_agency_id and major_object_class_code):
            raise InvalidParameterException(
                "Missing one or more required query parameters: fiscal_year, funding_agency_id, major_object_class_code"
            )

        toptier_agency = Agency.objects.filter(id=funding_agency_id).first()
        if toptier_agency is None:
            return FinancialAccountsByProgramActivityObjectClass.objects.none()
        toptier_agency = toptier_agency.toptier_agency

        submission_queryset = SubmissionAttributes.objects.all()
        submission_queryset = (
            submission_queryset.filter(cgac_code=toptier_agency.cgac_code, reporting_fiscal_year=fiscal_year)
            .order_by("-reporting_fiscal_year", "-reporting_fiscal_quarter")
            .annotate(fiscal_year=F("reporting_fiscal_year"), fiscal_quarter=F("reporting_fiscal_quarter"))
        )
        submission = submission_queryset.first()

        if submission is None:
            return FinancialAccountsByProgramActivityObjectClass.objects.none()
        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.fiscal_quarter

        # using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.all()
        # get the incoming agency's toptier agency, because that's what we'll
        # need to filter on
        # (used filter() instead of get() b/c we likely don't want to raise an
        # error on a bad agency id)
        queryset = queryset.filter(
            submission__reporting_fiscal_year=active_fiscal_year,
            submission__reporting_fiscal_quarter=active_fiscal_quarter,
            treasury_account__funding_toptier_agency=toptier_agency,
            object_class__major_object_class=major_object_class_code,
        )
        queryset = queryset.annotate(
            object_class_name=F("object_class__object_class_name"), object_class_code=F("object_class__object_class")
        )
        # sum obligated_mount by object class
        queryset = queryset.values("object_class_name", "object_class_code").annotate(
            obligated_amount=Sum("obligations_incurred_by_program_object_class_cpe")
        )
        # get minor object class vars
        return queryset
