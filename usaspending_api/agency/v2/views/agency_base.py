from re import fullmatch

from django.conf import settings
from rest_framework.exceptions import NotFound
from rest_framework.views import APIView
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.references.models import ToptierAgency


class AgencyBase(APIView):
    @property
    def toptier_code(self):
        # We don't have to do any validation here because Django has already checked this to be
        # either a three or four digit numeric string based on the regex pattern in our route url.
        return self.kwargs["toptier_code"]

    @property
    def fiscal_year(self):
        fiscal_year = str(self.request.query_params.get("fiscal_year", current_fiscal_year()))
        if not fullmatch("[0-9]{4}", fiscal_year):
            raise UnprocessableEntityException("Unrecognized fiscal_year format. Should be YYYY.")
        min_fiscal_year = fy(settings.API_SEARCH_MIN_DATE)
        fiscal_year = int(fiscal_year)
        if fiscal_year < min_fiscal_year:
            raise UnprocessableEntityException(
                f"fiscal_year is currently limited to an earliest year of {min_fiscal_year}."
            )
        if fiscal_year > current_fiscal_year():
            raise UnprocessableEntityException(
                f"fiscal_year may not exceed current fiscal year of {current_fiscal_year()}."
            )
        return fiscal_year

    @property
    def toptier_agency(self):
        toptier_agency = ToptierAgency.objects.account_agencies().filter(toptier_code=self.toptier_code).first()
        if not toptier_agency:
            raise NotFound(f"Agency with a toptier code of '{self.toptier_code}' does not exist")
        return toptier_agency
