from django.db.models import F, Sum
from rest_framework.exceptions import ParseError

from usaspending_api.accounts.serializers import AgenciesFinancialBalancesSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models import Agency
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC, DOD_CGAC
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.views import DetailViewSet
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.mixins import SuperLoggingMixin


class AgenciesFinancialBalancesViewSet(SuperLoggingMixin, DetailViewSet):
    """Returns financial balances by agency and the latest quarter for the given fiscal year."""

    serializer_class = AgenciesFinancialBalancesSerializer

    def get_queryset(self, pk=None):
        # retrieve post request payload
        json_request = self.request.query_params
        # retrieve fiscal_year from request
        fiscal_year = json_request.get('fiscal_year', None)
        funding_agency_id = json_request.get('funding_agency_id', None)

        # required query parameters were not provided
        if not (fiscal_year and funding_agency_id):
            raise InvalidParameterException('Missing one or more required query parameters: fiscal_year, funding_agency_id')

        toptier_agency = Agency.objects.filter(id=funding_agency_id).first()
        if toptier_agency is None:
            return AppropriationAccountBalances.objects.none()
        toptier_agency = toptier_agency.toptier_agency

        submission_queryset = SubmissionAttributes.objects.all()
        submission_queryset = submission_queryset.filter(cgac_code=toptier_agency.cgac_code,
                                                         reporting_fiscal_year=fiscal_year).\
            order_by('-reporting_fiscal_year', '-reporting_fiscal_quarter').\
            annotate(fiscal_year=F('reporting_fiscal_year'), fiscal_quarter=F('reporting_fiscal_quarter'))
        submission = submission_queryset.first()

        if submission is None:
            return AppropriationAccountBalances.objects.none()
        active_fiscal_year = submission.reporting_fiscal_year
        active_fiscal_quarter = submission.fiscal_quarter

        # using final_objects below ensures that we're only pulling the latest
        # set of financial information for each fiscal year
        queryset = AppropriationAccountBalances.final_objects.all()
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
                treasury_account_identifier__funding_toptier_agency__cgac_code__in=tta_list
            )
        else:
            queryset = queryset.filter(
                submission__reporting_fiscal_year=active_fiscal_year,
                submission__reporting_fiscal_quarter=active_fiscal_quarter,
                treasury_account_identifier__funding_toptier_agency=toptier_agency
            )

        queryset = queryset.annotate(
            fiscal_year=F('submission__reporting_fiscal_year'))

        # sum balances by treasury appropriation account (TAS)
        queryset = queryset.values('fiscal_year').annotate(
            budget_authority_amount=Sum('budget_authority_available_amount_total_cpe'),
            obligated_amount=Sum('obligations_incurred_total_by_tas_cpe'),
            outlay_amount=Sum('gross_outlay_amount_by_tas_cpe'))

        return queryset
