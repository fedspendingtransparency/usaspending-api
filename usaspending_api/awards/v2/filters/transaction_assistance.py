from usaspending_api.awards.models import Transaction, TransactionAssistance
from usaspending_api.common.exceptions import InvalidParameterException


def transaction_assistance_filter(filters):
    # 'keyword',
    # 'time_period',
    # 'award_type_codes',
    # 'agencies',xs
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

    queryset = TransactionAssistance.objects.all()
    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        # keyword - DONE
        if key == "keyword":
            queryset = queryset.filter(transaction__award__description=value)

        # time_period - DONE
        elif key == "time_period":
            if value is not None:
                or_queryset = TransactionAssistance.objects.none()
                for v in value:
                    # (may have to cast to date) (oct 1 to sept 30)
                    or_queryset = or_queryset.filter(
                        transaction__award__period_of_performance_start_date__gte=v["start_date"],
                        transaction__award__period_of_performance_current_end_date__lte=v["end_date"])
                queryset |= or_queryset  # TODO: really or, not and?
            else:
                raise InvalidParameterException('Invalid filter: time period value is invalid.')

        # award_type_codes - DONE
        elif key == "award_type_codes":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(transaction__award__type=v)
            queryset &= or_queryset

        # agencies - DONE
        elif key == "agencies":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                type = v["type"]
                tier = v["tier"]
                name = v["name"]
                if type == "funding":
                    if tier == "toptier":
                        or_queryset |= TransactionAssistance.objects.filter(
                            transaction__award__funding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        or_queryset |= TransactionAssistance.objects.filter(
                            transaction__award__funding_agency__subtier_agency__name=name)
                elif type == "awarding":
                    if tier == "toptier":
                        or_queryset |= TransactionAssistance.objects.filter(
                            transaction__award__awarding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        or_queryset |= TransactionAssistance.objects.filter(
                            transaction__award__awarding_agency__subtier_agency__name=name)
                else:
                    raise InvalidParameterException('Invalid filter: agencies ' + name + ' type is invalid.')
            queryset &= or_queryset

        # legal_entities - DONE
        elif key == "legal_entities":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(transaction__award__recipient__name=v)
            queryset = queryset & or_queryset

        # recipient_location_scope (broken till data reload) - Done
        elif key == "recipient_scope":
            if value is not None:
                if value == "domestic":
                    queryset = queryset.TransactionAssistance.objects.filter(
                        transaction__award__recipient__location__country_name="UNITED STATES")
                elif value["type"] == "foreign":
                    queryset = queryset.TransactionAssistance.exclude(
                        transaction__award__recipient__location__country_name="UNITED STATES")
                else:
                    raise InvalidParameterException('Invalid filter: recipient_location type is invalid.')
            # TODO: "and" it with queryset?

        # recipient_location - DONE
        elif key == "recipient_locations":
            if value is not None:
                or_queryset = TransactionAssistance.objects.none()
                for v in value:
                    or_queryset |= TransactionAssistance.objects.filter(transaction__award__recipient__location__location_id=v)
                queryset = queryset & or_queryset
            else:
                raise InvalidParameterException('Invalid filter: recipient_location object is invalid.')

        # recipient_type_names - DONE
        elif key == "recipient_type_names":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(transaction__award__recipient__business_types_description=v)
            queryset &= or_queryset

        # place_of_performance_scope (broken till data reload)- DONE
        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.TransactionAssistance.objects.filter(
                    transaction__award__place_of_performance__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.TransactionAssistance.exclude(
                    transaction__award__place_of_performance__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: recipient_location type is invalid.')
            # TODO: "and" it with queryset?

        # place_of_performance  - DONE
        elif key == "place_of_performance_locations":
            if value is not None:
                or_queryset = TransactionAssistance.objects.none()
                for v in value:
                    or_queryset |= TransactionAssistance.objects.filter(transaction__award__place_of_performance__location_id=v)
                queryset = queryset & or_queryset
            else:
                raise InvalidParameterException('Invalid filter: recipient_location object is invalid.')

        # award_amounts - DONE
        elif key == "award_amounts":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                if v["lower_bound"] is not None and v["upper_bound"] is not None:
                    or_queryset |= TransactionAssistance.objects.filter(transaction__award__total_obligation__gt=v["lower_bound"],
                                                                        total_obligation__lt=v["upper_bound"])
                elif v["lower_bound"] is not None:
                    or_queryset |= TransactionAssistance.objects.filter(transaction__award__total_obligation__gt=v["lower_bound"])
                elif v["upper_bound"] is not None:
                    or_queryset |= TransactionAssistance.objects.filter(transaction__award__total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            queryset &= or_queryset

        # award_ids - DONE
        elif key == "award_ids":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(transaction__award__id=v)
            queryset &= or_queryset

        # program_numbers  - DONE
        elif key == "program_numbers":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(
                    transaction__assistance_data__cfda__program_number=v)
            queryset &= or_queryset

        # naics_codes - DONE
        elif key == "naics_codes":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(
                    transaction__contract_data__naics=v)
            queryset &= or_queryset

        # psc_codes - DONE
        elif key == "psc_codes":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(
                    transaction__contract_data__product_or_service_code=v)
            queryset &= or_queryset

        # contract_pricing_type_codes - DONE
        elif key == "contract_pricing_type_codes":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(
                    transaction__contract_data__type_of_contract_pricing=v)
            queryset &= or_queryset
        # set_aside_type_codes - DONE
        elif key == "set_aside_type_codes":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(
                    transaction__contract_data__type_set_aside=v)
            queryset &= or_queryset
        # extent_competed_type_codes - DONE
        elif key == "extent_competed_type_codes":
            or_queryset = TransactionAssistance.objects.none()
            for v in value:
                or_queryset |= TransactionAssistance.objects.filter(
                    transaction__contract_data__extent_competed=v)
            queryset &= or_queryset

        else:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')
            # kwargs = {
            #     '{0}'.format(filterdict[key]): value
            # }
            # queryset = queryset.filter(**kwargs)

    return queryset
