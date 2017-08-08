from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models import Sum

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.models import Award


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



        # 'keyword',
        # 'time_period',
        # 'award_type_codes',
        # 'agencies',
        # 'legal_entities',
        # 'recipient_location_scope',
        # 'recipient_location',
        # 'recipient_type_names',
        # 'place_of_performance_scope',
        # 'place_of_performance',
        # 'award_amount',
        # 'award_ids',
        # 'program_numbers',
        # 'naics_codes',
        # 'psc_codes',
        # 'contract_pricing_type_codes',
        # 'set_aside_type_codes',
        # 'extent_competed_type_codes'





        filterdict = {
            "keyword": 'description',
            "location_scope": 'place_of_performance__location_country_code',
            "location": 'place_of_performance__location_id',
            "award_type": 'type',
            "award_id": 'id',
            "award_amount": 'total_obligation',
            "recipient_id": 'recipient__legal_entity_id',
            "recipient_location_scope": 'recipient__location__location_country_code',
            "recipient_location": 'recipient__location__location_id',
            "recipient_name": 'recipient__recipient_name',
            "recipient_DUNS": 'recipient__recipient_unique_id',
            "recipient_type": 'recipient__business_categories',
            "funding_agency": {
                "toptier": 'funding_agency__toptier_agency__name',
                "subtier": 'funding_agency__subtier_agency__name'
            },
            "awarding_agency": {
                "toptier": 'awarding_agency__toptier_agency__name',
                "subtier": 'awarding_agency__subtier_agency__name'
            },
            "cfda_number": 'latest_transaction__assistance_data__cfda__program_number',
            "cfda_title": 'latest_transaction__assistance_data__cfda__program_title',
            "naics": 'latest_transaction__contract_data__naics',
            "naics_description": 'latest_transaction__contract_data__naics_description',
            "psc": 'latest_transaction__contract_data__product_or_service_code'
        }

        queryset = Award.objects.all()
        for key, value in filters.items():
            # check for valid key
            if value is None:
                raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')
            # keyword
            if key == "keyword" and isinstance(value, str):
                queryset = queryset.filter(description=value)
            # time_period
            elif key == "time_period":
                if value["type"] == "fy":
                    orqueryset = Award.objects.none()
                    for v in value["value"]:
                        # TODO: FILTER BY DATE
                        orqueryset = orqueryset | Award.filter(date=v)
                    queryset = queryset & orqueryset
                elif value["type"] == "dr":
                    # TODO: FILTER BY Date
                    queryset = queryset.filter(date=value["value"]["start_date"])
                    queryset = queryset.filter(date=value["value"]["end_date"])
                else:
                    raise InvalidParameterException('Invalid filter: time period type is invalid.')

            # award_type_codes
            elif key == "award_type_codes":
                or_queryset = Award.objects.none()
                for v in value:
                    or_queryset |= Award.filter(type=v)
                queryset &= orqueryset

            # agencies
            elif key == "agencies":
                for v in value:
                    type = v["type"]
                    tier = v["tier"]
                    name = v["name"]
                    if type == "funding":
                        pass
                    elif type == "awarding":
                        pass
                    if tier == "toptier":
                        pass
                    if tier == "subtier":
                        pass
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + name + ' type is invalid.')
                pass
            
            # legal_entities
            elif key == "legal_entities":
                pass
            # recipient_location_scope
            elif key == "recipient_location_scope":
                pass
            # recipient_location
            elif key == "recipient_location":
                pass
            # recipient_type_names
            elif key == "recipient_type_names":
                pass
            # place_of_performance_scope
            elif key == "place_of_performance_scope":
                pass
            # place_of_performance
            elif key == "place_of_performance":
                pass
            # award_amount
            elif key == "award_amount":
                pass
            # award_ids
            elif key == "award_ids":
                pass
            # program_numbers
            elif key == "program_numbers":
                pass
            # naics_codes
            elif key == "naics_codes":
                pass
            # psc_codes
            elif key == "psc_codes":
                pass
            # contract_pricing_type_codes
            elif key == "contract_pricing_type_codes":
                pass
            # set_aside_type_codes
            elif key == "set_aside_type_codes":
                pass
            # extent_competed_type_codes
            elif key == "extent_competed_type_codes":
                pass

            elif key == "array":
                orqueryset = Award.objects.none()
                for v in value:
                    orqueryset = orqueryset | Award.filter(keyword=v)
                queryset = queryset & orqueryset


            else:
                kwargs = {
                    '{0}'.format(filterdict[key]): value
                }
                queryset = queryset.filter(**kwargs)

        # Filter based on search text
        # print(queryset.query)
        response = {}
        response['group'] = group
        response['results'] = []

        # filter queryset by time
        for i in time:
            queryset_time = queryset.filter()
            result_dic = queryset_time.aggregate(
                aggregated_amount=Sum('total_obligation')  # obligated_amount=Sum('obligations_incurred_total_by_award_cpe')
            )

            response['results'].append({'aggregated_amount': float(result_dic['aggregated_amount']),
                                'time_period': {'year': 2017, 'quarter': 3}})


        return Response(response)
