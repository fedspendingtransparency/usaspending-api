from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import InvalidParameterException


def award_filter(filters):
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

    # REMOVED FOR AWARD
    # 'program_numbers',
    # 'naics_codes',
    # 'psc_codes',
    # 'contract_pricing_type_codes',
    # 'set_aside_type_codes',
    # 'extent_competed_type_codes'

    queryset = Award.objects.all()
    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        # keyword - DONE
        if key == "keyword":
            print(value)
            queryset = queryset.filter(description=value)

        # time_period - DONE
        elif key == "time_period":
            if value is not None:
                or_queryset = Award.objects.none()
                for v in value:
                    # (may have to cast to date) (oct 1 to sept 30)
                    or_queryset = or_queryset.filter(
                        period_of_performance_start_date__gte=v.get("start_date"),
                        period_of_performance_current_end_date__lte=v.get("end_date"))
                queryset |= or_queryset
            else:
                raise InvalidParameterException('Invalid filter: time period value is invalid.')

        # award_type_codes - DONE
        elif key == "award_type_codes":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(type=v)
                else:
                    or_queryset = Award.objects.filter(type=v)
            if or_queryset is not None:
                queryset &= or_queryset

        # agencies - DONE
        elif key == "agencies":
            or_queryset = Award.objects.none()
            for v in value:
                type = v["type"]
                tier = v["tier"]
                name = v["name"]
                if type == "funding":
                    if tier == "toptier":
                        or_queryset |= or_queryset.filter(funding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        or_queryset |= or_queryset.filter(funding_agency__subtier_agency__name=name)
                elif type == "awarding":
                    if tier == "toptier":
                        or_queryset |= or_queryset.filter(awarding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        or_queryset |= or_queryset.filter(awarding_agency__subtier_agency__name=name)
                else:
                    raise InvalidParameterException('Invalid filter: agencies ' + name + ' type is invalid.')
            pass

        # legal_entities - DONE
        elif key == "legal_entities":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(recipient__recipient_name=v)
                else:
                    or_queryset = Award.objects.filter(recipient__recipient_name=v)
            queryset = queryset & or_queryset

        # recipient_location_scope (broken till data reload) - Done
        elif key == "recipient_scope":
            if value is not None:
                if value == "domestic":
                    queryset = queryset.filter(recipient__location__country_name="UNITED STATES")
                elif value["type"] == "foreign":
                    queryset = queryset.exclude(recipient__location__country_name="UNITED STATES")
                else:
                    raise InvalidParameterException('Invalid filter: recipient_location type is invalid.')

        # recipient_location - DONE
        elif key == "recipient_locations":
            if value is not None:
                or_queryset = None
                for v in value:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(recipient__location__location_id=v)
                    else:
                        or_queryset = Award.objects.filter(recipient__location__location_id=v)
                queryset = queryset & or_queryset
            else:
                raise InvalidParameterException('Invalid filter: recipient_location object is invalid.')

        # recipient_type_names - DONE
        elif key == "recipient_type_names":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(recipient__business_types_description=v)
                else:
                    or_queryset = Award.objects.filter(recipient__business_types_description=v)
            queryset &= or_queryset

        # place_of_performance_scope (broken till data reload)- DONE
        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(place_of_performance__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(place_of_performance__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: recipient_location type is invalid.')

        # place_of_performance  - DONE
        elif key == "place_of_performance_locations":
            if value is not None:
                or_queryset = None
                for v in value:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(place_of_performance__location_id=v)
                    else:
                        or_queryset = Award.objects.filter(place_of_performance__location_id=v)
                queryset = queryset & or_queryset
            else:
                raise InvalidParameterException('Invalid filter: recipient_location object is invalid.')

        # award_amounts - DONE
        elif key == "award_amounts":
            or_queryset = None
            for v in value:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(total_obligation__gt=v["lower_bound"],
                                                          total_obligation__lt=v["upper_bound"])
                    else:
                        or_queryset = Award.objects.filter(total_obligation__gt=v["lower_bound"],
                                                           total_obligation__lt=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(total_obligation__gt=v["lower_bound"])
                    else:
                        or_queryset = Award.objects.filter(total_obligation__gt=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(total_obligation__lt=v["upper_bound"])
                    else:
                        or_queryset = Award.objects.filter(total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            queryset &= or_queryset

        # award_ids - DONE
        elif key == "award_ids":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(id=v)
                else:
                    or_queryset = Award.objects.filter(id=v)
            queryset &= or_queryset

        # program_numbers  - REMOVED FOR AWARD

        elif key == "program_numbers":
            pass

        #     or_queryset = None
        #     for v in value:
        #         if or_queryset:
        #             or_queryset |= or_queryset.filter(
        #                 assistance_data__cfda__program_number=v)
        #         else:
        #             or_queryset = Transaction.objects.filter(
        #                 assistance_data__cfda__program_number=v)
        #     queryset &= or_queryset

        # naics_codes - - REMOVED FOR AWARD
        elif key == "naics_codes":
            pass
        #     or_queryset = None
        #     for v in value:
        #         if or_queryset:
        #             or_queryset |= or_queryset.filter(
        #                 contract_data__naics=v)
        #         else:
        #             or_queryset = Transaction.objects.filter(
        #                 contract_data__naics=v)
        #     queryset &= or_queryset

        # psc_codes - - REMOVED FOR AWARD
        elif key == "psc_codes":
            pass
        #     or_queryset = None
        #     for v in value:
        #         if or_queryset:
        #             or_queryset |= or_queryset.filter(
        #                 contract_data__product_or_service_code=v)
        #         else:
        #             or_queryset = Transaction.objects.filter(
        #                 contract_data__product_or_service_code=v)
        #     queryset &= or_queryset

        # contract_pricing_type_codes - REMOVED FOR AWARD
        elif key == "contract_pricing_type_codes":
            pass
        #     or_queryset = None
        #     for v in value:
        #         if or_queryset:
        #             or_queryset |= or_queryset.filter(
        #                 contract_data__type_of_contract_pricing=v)
        #         else:
        #             or_queryset = Transaction.objects.filter(
        #                 contract_data__type_of_contract_pricing=v)
        #     queryset &= or_queryset
        # set_aside_type_codes - - REMOVED FOR AWARD
        elif key == "set_aside_type_codes":
            pass
        #     or_queryset = None
        #     for v in value:
        #         if or_queryset:
        #             or_queryset |= or_queryset.filter(
        #                 contract_data__type_set_aside=v)
        #         else:
        #             or_queryset = Transaction.objects.filter(
        #                 contract_data__type_set_aside=v)
        #     queryset &= or_queryset
        # extent_competed_type_codes - - REMOVED FOR AWARD
        elif key == "extent_competed_type_codes":
            pass
        #     or_queryset = None
        #     for v in value:
        #         if or_queryset:
        #             or_queryset |= or_queryset.filter(
        #                 contract_data__extent_competed=v)
        #         else:
        #             or_queryset = Transaction.objects.filter(
        #                 contract_data__extent_competed=v)
        #     queryset &= or_queryset

        else:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')
            # kwargs = {
            #     '{0}'.format(filterdict[key]): value
            # }
            # queryset = queryset.filter(**kwargs)
        # print("-------------1----------")
        # print(key)
        # print("-------------2----------")
        # print(queryset.query)

    return queryset
