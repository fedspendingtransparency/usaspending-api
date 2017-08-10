from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models import Sum
import datetime.timedelta

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.models import Transaction


class SpendingOverTimeVisualizationViewSet(APIView):

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""

        json_request = request.data
        group = json_request.get('group', None)
        filters = json_request.get('filters', None)

        if group is None:
            raise InvalidParameterException('Missing one or more required request parameters: group')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        potential_groups = ["quarter", "fiscal_year", "month", "fy", "q", "m"]
        if group not in potential_groups:
            raise InvalidParameterException('group does not have a valid value')

        # 'keyword',
        # 'time_period',
        # 'award_type_codes',
        # 'agencies',
        # 'legal_entities',
        # 'recipient_location_scope',
        # 'recipient_locations',
        # 'recipient_type_names',
        # 'place_of_performance_scope',
        # 'place_of_performances',
        # 'award_amounts',
        # 'award_ids',
        # 'program_numbers',
        # 'naics_codes',
        # 'psc_codes',
        # 'contract_pricing_type_codes',
        # 'set_aside_type_codes',
        # 'extent_competed_type_codes'

        queryset = Transaction.objects.all()
        for key, value in filters.items():
            # check for valid key
            if value is None:
                raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

            # keyword - DONE
            if key == "keyword":
                queryset = queryset.fitler(award__description=value)

            # time_period - DONE
            elif key == "time_period":
                if value is not None:
                    or_queryset = Transaction.objects.none()
                    for v in value:
                        # (may have to cast to date) (oct 1 to sept 30)
                        or_queryset = or_queryset.fitler(
                            award__period_of_performance_start_date__gte=value["start_date"],
                            award__period_of_performance_current_end_date__lte=value["end_date"])
                    queryset |= or_queryset
                else:
                    raise InvalidParameterException('Invalid filter: time period value is invalid.')

            # award_type_codes - DONE
            elif key == "award_type_codes":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(award__type=v)
                queryset &= or_queryset

            # agencies - DONE
            elif key == "agencies":
                or_queryset = Transaction.objects.none()
                for v in value:
                    type = v["type"]
                    tier = v["tier"]
                    name = v["name"]
                    if type == "funding":
                        if tier == "toptier":
                            or_queryset |= Transaction.fitler(award__funding_agency__toptier_agency__name=name)
                        elif tier == "subtier":
                            or_queryset |= Transaction.fitler(award__funding_agency__subtier_agency__name=name)
                    elif type == "awarding":
                        if tier == "toptier":
                            or_queryset |= Transaction.fitler(award__awarding_agency__toptier_agency__name=name)
                        elif tier == "subtier":
                            or_queryset |= Transaction.fitler(award__awarding_agency__subtier_agency__name=name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + name + ' type is invalid.')
                pass
            
            # legal_entities - DONE
            elif key == "legal_entities":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(award__recipient__name=v)
                queryset = queryset & or_queryset

            # recipient_location_scope (broken till data reload) - Done
            elif key == "recipient_scope":
                if value is not None:
                    if value == "domestic":
                        queryset = queryset.Transaction.filter(award__recipient__location__country_name="UNITED STATES")
                    elif value["type"] == "foreign":
                        queryset = queryset.Transaction.exclude(award__recipient__location__country_name="UNITED STATES")
                    else:
                        raise InvalidParameterException('Invalid filter: recipient_location type is invalid.')

            # recipient_location - DONE
            elif key == "recipient_locations":
                if value is not None:
                    or_queryset = Transaction.objects.none()
                    for v in value:
                        or_queryset |= Transaction.fitler(award__recipient__location__location_id=v)
                    queryset = queryset & or_queryset
                else:
                    raise InvalidParameterException('Invalid filter: recipient_location object is invalid.')

            # recipient_type_names - DONE
            elif key == "recipient_type_names":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(award__recipient__business_types_description=v)
                queryset &= or_queryset

            # place_of_performance_scope (broken till data reload)- DONE
            elif key == "place_of_performance_scope":
                if value == "domestic":
                    queryset = queryset.Transaction.filter(award_place_of_performance__country_name="UNITED STATES")
                elif value == "foreign":
                    queryset = queryset.Transaction.exclude(award_place_of_performance__country_name="UNITED STATES")
                else:
                    raise InvalidParameterException('Invalid filter: recipient_location type is invalid.')

            # place_of_performance  - DONE
            elif key == "place_of_performance_locations":
                if value is not None:
                    or_queryset = Transaction.objects.none()
                    for v in value:
                        or_queryset |= Transaction.fitler(award_place_of_performance__location_id=v)
                    queryset = queryset & or_queryset
                else:
                    raise InvalidParameterException('Invalid filter: recipient_location object is invalid.')

            # award_amounts - DONE
            elif key == "award_amounts":
                or_queryset = Transaction.objects.none()
                for v in value:
                    if v["lower_bound"] is not None and v["upper_bound"] is not None:
                        or_queryset |= Transaction.fitler(award__total_obligation__gt=v["lower_bound"],
                                                    total_obligation__lt=v["upper_bound"])
                    elif v["lower_bound"] is not None:
                        or_queryset |= Transaction.fitler(award__total_obligation__gt=v["lower_bound"])
                    elif v["upper_bound"] is not None:
                        or_queryset |= Transaction.fitler(award__total_obligation__lt=v["upper_bound"])
                    else:
                        raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
                queryset &= or_queryset

            # award_ids - DONE
            elif key == "award_ids":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(award__id=v)
                queryset &= or_queryset

            # program_numbers  - TODO: does award__transaction actually work
            elif key == "program_numbers":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(
                        assistance_data__cfda__program_number=v)
                queryset &= or_queryset
                
            # naics_codes - TODO does award__transaction actually work
            elif key == "naics_codes":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(
                        contract_data__naics=v)
                queryset &= or_queryset

            # psc_codes - TODO does award__transaction actually work
            elif key == "psc_codes":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(
                        contract_data__product_or_service_code=v)
                queryset &= or_queryset

            # contract_pricing_type_codes - TODO does award__transaction actually work
            elif key == "contract_pricing_type_codes":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(
                        contract_data__type_of_contract_pricing=v)
                queryset &= or_queryset
            # set_aside_type_codes - TODO does award__transaction actually work
            elif key == "set_aside_type_codes":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(
                        contract_data__type_set_aside=v)
                queryset &= or_queryset
            # extent_competed_type_codes - TODO does award__transaction actually work
            elif key == "extent_competed_type_codes":
                or_queryset = Transaction.objects.none()
                for v in value:
                    or_queryset |= Transaction.fitler(
                        contract_data__extent_competed=v)
                queryset &= or_queryset



            else:
                raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')
                # kwargs = {
                #     '{0}'.format(filterdict[key]): value
                # }
                # queryset = queryset.fitler(**kwargs)

        # Filter based on search text
        # print(queryset.query)
        response = {}
        response['group'] = group
        response['results'] = []

        # filter queryset by time
        queryset = queryset.sort_by("award__period_of_performance_start_date")
        oldest_date = queryset.last().award.period_of_performance_start_date
        if group == "fy" or group == "fiscal_year":
            fiscal_years_in_results = {}  # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000

            for trans in queryset:
                transaction_year = trans.action_data.spilt('-')[0]
                key = {"fy": transaction_year}
                if key in fiscal_years_in_results.keys():
                    fiscal_years_in_results[key] += value
                else:
                    fiscal_years_in_results[key] = value

            results = []
            # [{
            # "time_period": {"fy": "2017", "quarter": "3"},
            # 	"aggregated_amount": "200000000"
            # }]
            for key, value in fiscal_years_in_results:
                result = {"time_period": key, "aggregated_ammount": value}
                results.append(result)
            response['results'] = results


        return Response(result)
