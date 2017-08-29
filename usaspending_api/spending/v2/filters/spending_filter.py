import urllib
from urllib.error import HTTPError

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException


def spending_filter(filters):
    # 'budget_function',
    # 'budget_subfunction',
    # 'federal_account',
    # 'program_activity',
    # 'object_class',
    # 'recipient',
    # 'award',
    # 'agency',
    # 'fiscal_year'

    queryset = FinancialAccountsByAwards.objects.all()

    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        # budget_function - DONE
        if key == 'budget_function':
            queryset = queryset.filter(treasury_account__budget_function_code=value)

        # budget_subfunction - DONE
        elif key == 'budget_subfunction':
            queryset = queryset.filter(treasury_account__budget_subfunction_code=value)

        # federal_account - DONE
        elif key == 'federal_account':
            queryset = queryset.filter(treasury_account__federal_account__main_account_code=value)

        # program_activity - DONE
        elif key == 'program_activity':
            queryset = queryset.filter(program_activity=value)

        # object_class - DONE
        elif key == 'object_class':
            queryset = queryset.filter(object_class__major_object_class=value)

        # recipient - DONE
        elif key == 'recipient':
            queryset = queryset.filter(award__recipient__recipient_unique_id=value)

        # award - DONE
        elif key == 'award':
            queryset = queryset.filter(award=value)

        # agency - DONE
        elif key == 'agency':
            try:
                agency_id = queryset.filter(award__awarding_agency__id=value)

            except urllib.error.HTTPError as e:
                try:
                    if e.code == 400:
                        agency_id = queryset.filter(award__awarding_agency__subtier_agency__subtier_code=value)
                    else:
                        raise InvalidParameterException('Invalid agency ID: ' + value + ' does not exist.')
                finally:
                    queryset = queryset.filter(award__awarding_agency__id=agency_id)

        # fiscal_year - DONE
        elif key == 'fiscal_year':
            queryset = queryset.filter(award__period_of_performance_current_end_date=value)

        else:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')

    return queryset
