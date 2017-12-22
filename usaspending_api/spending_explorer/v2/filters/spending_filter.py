import logging
import urllib
from urllib.error import HTTPError

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger(__name__)


def spending_filter(alt_set, queryset, filters, _type):

    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = ['budget_function', 'budget_subfunction', 'federal_account', 'program_activity', 'object_class',
                    'recipient', 'award', 'award_category', 'agency', 'agency_type', 'fy']

        if key not in key_list:
            raise InvalidParameterException(key + ' filter does not exist. '
                                                  'Valid Filters: budget_function, budget_subfunction, federal_account,'
                                                  'program_activity, object_class, recipient, award, award_category,'
                                                  'agency, agency_type, fy.')

        # Check _type to filter on correct set (alt_set or queryset)
        alt_set_keys = ['recipient', 'award', 'award_category', 'agency_type']
        if _type in alt_set_keys:
            # Apply filters
            # budget_function
            if key == 'budget_function':
                and_alt_set = alt_set.filter(treasury_account__budget_function_code=value)
                alt_set &= and_alt_set

            # budget_subfunction
            elif key == 'budget_subfunction':
                and_alt_set = alt_set.filter(treasury_account__budget_subfunction_code=value)
                alt_set &= and_alt_set

            # federal_account
            elif key == 'federal_account':
                and_alt_set = alt_set.filter(treasury_account__federal_account=value)
                alt_set &= and_alt_set

            # program_activity
            elif key == 'program_activity':
                and_alt_set = alt_set.filter(program_activity=value)
                alt_set &= and_alt_set

            # object_class
            elif key == 'object_class':
                or_alt_set = alt_set.filter(object_class__major_object_class=value)
                alt_set &= or_alt_set

            # recipient
            elif key == 'recipient':
                and_alt_set = alt_set.filter(award__recipient_id=value)
                alt_set &= and_alt_set

            # award, award_category
            elif key == 'award' or key == 'award_category':
                and_alt_set = alt_set.filter(award__id=value)
                alt_set &= and_alt_set

            # agency
            elif key == 'agency':
                # TODO: Will need to incorporate "agency_type" here to filter based on toptier or subtier.

                # Currently default to filtering on toptier
                agency = Agency.objects.filter(toptier_flag=True, id=value).first()
                if agency is None:
                    raise InvalidParameterException('Agency ID provided does not correspond to a toptier agency')

                and_alt_set = alt_set.filter(treasury_account__funding_toptier_agency=agency.toptier_agency)
                alt_set &= and_alt_set

        # All other _type
        else:
            # budget_function
            if key == 'budget_function':
                and_queryset = queryset.filter(treasury_account__budget_function_code=value)
                queryset &= and_queryset

            # budget_subfunction
            elif key == 'budget_subfunction':
                and_queryset = queryset.filter(treasury_account__budget_subfunction_code=value)
                queryset &= and_queryset

            # federal_account
            elif key == 'federal_account':
                and_queryset = queryset.filter(treasury_account__federal_account=value)
                queryset &= and_queryset

            # program_activity
            elif key == 'program_activity':
                and_queryset = queryset.filter(program_activity=value)
                queryset &= and_queryset

            # object_class
            elif key == 'object_class':
                and_queryset = queryset.filter(object_class__major_object_class=value)
                queryset &= and_queryset

            # recipient
            elif key == 'recipient':
                and_queryset = queryset.\
                    filter(treasury_account__in=alt_set.
                           filter(award__in=Award.objects.all().
                                  filter(recipient_id=value)).
                           values_list('treasury_account_id', flat=True))
                queryset &= and_queryset

            # award, award_category
            elif key == 'award' or key == 'award_category':
                and_queryset = queryset.\
                    filter(treasury_account__in=alt_set.
                           filter(award__id=value).
                           values_list('treasury_account_id', flat=True))
                queryset &= and_queryset

            # agency
            elif key == 'agency':
                # TODO: Will need to incorporate "agency_type" here to filter based on toptier or subtier.

                # Currently default to filtering on toptier
                agency = Agency.objects.filter(toptier_flag=True, id=value).first()
                if agency is None:
                    raise InvalidParameterException('Agency ID provided does not correspond to a toptier agency')

                and_queryset = queryset.filter(treasury_account__funding_toptier_agency=agency.toptier_agency)
                queryset &= and_queryset

    return alt_set, queryset
