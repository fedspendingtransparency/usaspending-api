from django.db.models import F, Sum
from rest_framework.exceptions import ParseError

from usaspending_api.accounts.serializers import ObjectClassFinancialSpendingSerializer
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass)
from usaspending_api.references.models import Agency
from usaspending_api.common.views import DetailViewSet
from usaspending_api.common.exceptions import InvalidParameterException


class ObjectClassFinancialSpendingViewSet(DetailViewSet):
    """Returns financial spending data by object class."""

    serializer_class = ObjectClassFinancialSpendingSerializer

    def get_queryset(self):
        # retrieve post request payload
        json_request = self.request.query_params

        # retrieve fiscal_year & agency_id from request
        fiscal_year = json_request.get('fiscal_year', None)
        funding_agency_id = json_request.get('funding_agency_id', None)

        # required query parameters were not provided
        if not (fiscal_year and funding_agency_id):
            raise InvalidParameterException('Missing one or more required query parameters: fiscal_year, funding_agency_id')

        # using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.all()
        # get the incoming agency's toptier agency, because that's what we'll
        # need to filter on
        # (used filter() instead of get() b/c we likely don't want to raise an
        # error on a bad agency id)
        toptier_agency = Agency.objects.filter(id=funding_agency_id).first().toptier_agency
        print(queryset)
        queryset = queryset.filter(
            submission__reporting_fiscal_year=fiscal_year,
            treasury_account__funding_toptier_agency=toptier_agency
        )
        print(queryset)
        queryset = queryset.annotate(
            major_object_class_name=F('object_class__major_object_class_name'),
            major_object_class_code=F('object_class__major_object_class'))
        # sum obligated_mount by object class
        queryset = queryset.values('major_object_class_name', 'major_object_class_code').annotate(
            obligated_amount=Sum('obligations_incurred_by_program_object_class_cpe'))

        return queryset
