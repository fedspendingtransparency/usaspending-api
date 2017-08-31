import logging
import urllib
from decimal import Decimal
from urllib.error import HTTPError

from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger(__name__)


def spending_filter(queryset, filters):

    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = ['budget_function',
                    'budget_subfunction',
                    'federal_account',
                    'program_activity',
                    'object_class',
                    'recipient',
                    'award',
                    'award_category',
                    'agency',
                    'agency_top',
                    'agency_sub',
                    'fy']

        if key not in key_list:
            raise InvalidParameterException(
                key + ' filter does not exist.'
                      'Valid Filters: budget_function, budget_subfunction, federal_account,'
                      'program_activity, object_class, recipient, award, award_category,'
                      'agency, agency_top, agency_sub fy.'
            )

        # budget_function - DONE
        if key == 'budget_function':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(treasury_account__budget_function_code=value)
            else:
                or_queryset = queryset.filter(treasury_account__budget_function_code=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # budget_subfunction - DONE
        elif key == 'budget_subfunction':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(treasury_account__budget_subfunction_code=value)
            else:
                or_queryset = queryset.filter(treasury_account__budget_subfunction_code=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # federal_account - DONE
        elif key == 'federal_account':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(treasury_account__federal_account__main_account_code=value)
            else:
                or_queryset = queryset.filter(treasury_account__federal_account__main_account_code=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # program_activity - DONE
        elif key == 'program_activity':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(program_activity__program_activity_code=value)
            else:
                or_queryset = queryset.filter(program_activity__program_activity_code=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # object_class - DONE
        elif key == 'object_class':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(object_class__major_object_class=value)
            else:
                or_queryset = queryset.filter(object_class__major_object_class=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # recipient - DONE
        elif key == 'recipient':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(award__recipient__recipient_unique_id=value)
            else:
                or_queryset = queryset.filter(award__recipient__recipient_unique_id=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # award - DONE
        elif key == 'award':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(award__awarding_agency__id=value)
            else:
                or_queryset = queryset.filter(award__awarding_agency__id=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # award_category - DONE
        elif key == 'award_category':
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.filter(award__category=value)
            else:
                or_queryset = queryset.filter(award__category=value)
            if or_queryset is not None:
                queryset &= or_queryset

        # agency - DONE
        elif key == 'agency':
            or_queryset = None
            try:
                if or_queryset:
                    or_queryset |= or_queryset.filter(award__awarding_agency__toptier_agency__toptier_agency_id=value)
                else:
                    or_queryset = queryset.filter(award__awarding_agency__toptier_agency__toptier_agency_id=value)
                if or_queryset is not None:
                    queryset &= or_queryset
            except urllib.error.HTTPError as e:
                try:
                    if e.code == 400:
                        try:
                            if or_queryset:
                                or_queryset |= or_queryset.filter(
                                    award__awarding_agency__toptier_agency__cgac_code=value)
                            else:
                                or_queryset = queryset.filter(
                                    award__awarding_agency__toptier_agency__cgac_code=value)
                            if or_queryset is not None:
                                queryset &= or_queryset
                        except urllib.error.HTTPError as e:
                            try:
                                if e.code == 400:
                                    if or_queryset:
                                        or_queryset |= or_queryset.filter(
                                            award__awarding_agency__subtier_agency__subtier_code=value)
                                    else:
                                        or_queryset = queryset.filter(
                                            award__awarding_agency__subtier_agency__subtier_code=value)
                                else:
                                    raise InvalidParameterException('Invalid agency ID: ' + value + ' does not exist.')
                            finally:
                                if or_queryset is not None:
                                    queryset &= or_queryset
                    else:
                        raise InvalidParameterException('Invalid agency ID: ' + value + ' does not exist.')
                finally:
                    if or_queryset is not None:
                        queryset &= or_queryset

        # agency_top - DONE
        elif key == 'agency_top':
            or_queryset = None
            try:
                if or_queryset:
                    or_queryset |= or_queryset.filter(award__awarding_agency__toptier_agency__cgac_code=value)
                else:
                    or_queryset = queryset.filter(award__awarding_agency__toptier_agency__cgac_code=value)
                if or_queryset is not None:
                    queryset &= or_queryset
            except urllib.error.HTTPError as e:
                try:
                    if e.code == 400:
                        try:
                            if or_queryset:
                                or_queryset |= or_queryset.filter(
                                    award__awarding_agency__subtier_agency__subtier_code=value)
                            else:
                                or_queryset = queryset.filter(
                                    award__awarding_agency__subtier_agency__subtier_code=value)
                            if or_queryset is not None:
                                queryset &= or_queryset
                        except urllib.error.HTTPError as e:
                            try:
                                if e.code == 400:
                                    if or_queryset:
                                        or_queryset |= or_queryset.filter(
                                            award__awarding_agency__id=value)
                                    else:
                                        or_queryset = queryset.filter(
                                            award__awarding_agency__id=value)
                                else:
                                    raise InvalidParameterException('Invalid agency ID: ' + value + ' does not exist.')
                            finally:
                                if or_queryset is not None:
                                    queryset &= or_queryset
                    else:
                        raise InvalidParameterException('Invalid agency ID: ' + value + ' does not exist.')
                finally:
                    if or_queryset is not None:
                        queryset &= or_queryset

        # agency_top - DONE
        elif key == 'agency_sub':
            or_queryset = None
            try:
                if or_queryset:
                    or_queryset |= or_queryset.filter(award__awarding_agency__subtier_agency__subtier_code=value)
                else:
                    or_queryset = queryset.filter(award__awarding_agency__subtier_agency__subtier_code=value)
                if or_queryset is not None:
                    queryset &= or_queryset
            except urllib.error.HTTPError as e:
                try:
                    if e.code == 400:
                        try:
                            if or_queryset:
                                or_queryset |= or_queryset.filter(
                                    award__awarding_agency__toptier_agency__cgac_code=value)
                            else:
                                or_queryset = queryset.filter(
                                    award__awarding_agency__toptier_agency__cgac_code=value)
                            if or_queryset is not None:
                                queryset &= or_queryset
                        except urllib.error.HTTPError as e:
                            try:
                                if e.code == 400:
                                    if or_queryset:
                                        or_queryset |= or_queryset.filter(
                                            award__awarding_agency__id=value)
                                    else:
                                        or_queryset = queryset.filter(
                                            award__awarding_agency__id=value)
                                else:
                                    raise InvalidParameterException(
                                        'Invalid agency ID: ' + value + ' does not exist.')
                            finally:
                                if or_queryset is not None:
                                    queryset &= or_queryset
                    else:
                        raise InvalidParameterException('Invalid agency ID: ' + value + ' does not exist.')
                finally:
                    if or_queryset is not None:
                        queryset &= or_queryset

        else:
            # Remove NaN and null values
            or_queryset = None
            if or_queryset:
                or_queryset |= or_queryset.exclude(obligations_incurred_total_by_award_cpe__isnull=True)
                for item in queryset.values('obligations_incurred_total_by_award_cpe'):
                    for index, dec in item.items():
                        if dec != dec or dec == Decimal('Inf') or dec == Decimal('-Inf'):
                            queryset = queryset.exclude(obligations_incurred_total_by_award_cpe=dec)
            else:
                or_queryset = queryset.exclude(obligations_incurred_total_by_award_cpe__isnull=True)
                for item in queryset.values('obligations_incurred_total_by_award_cpe'):
                    for index, dec in item.items():
                        if dec != dec or dec == Decimal('Inf') or dec == Decimal('-Inf'):
                            queryset = queryset.exclude(obligations_incurred_total_by_award_cpe=dec)
            if or_queryset is not None:
                queryset &= or_queryset

    return queryset
