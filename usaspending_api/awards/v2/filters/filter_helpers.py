from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.constants import WEBSITE_AWARD_BINS
from usaspending_api.common.helpers import dates_are_fiscal_year_bookends
from usaspending_api.common.helpers import generate_all_fiscal_years_in_range
from usaspending_api.common.helpers import generate_date_from_string
from usaspending_api.common.helpers import dates_are_month_bookends


def date_or_fy_queryset(date_dict, table, fiscal_year_column, action_date_column):
    full_fiscal_years = []
    for v in date_dict:
        s = generate_date_from_string(v.get("start_date"))
        e = generate_date_from_string(v.get("end_date"))
        if dates_are_fiscal_year_bookends(s, e):
            full_fiscal_years.append((s, e))

    if len(full_fiscal_years) == len(date_dict):
        fys = []
        for s, e in full_fiscal_years:
            fys.append(generate_all_fiscal_years_in_range(s, e))
        all_fiscal_years = set([x for sublist in fys for x in sublist])
        fiscal_year_filters = {"{}__in".format(fiscal_year_column): all_fiscal_years}
        return True, table.objects.filter(**fiscal_year_filters)

    or_queryset = None
    queryset_init = False

    for v in date_dict:
        kwargs = {}
        if v.get("start_date") is not None:
            kwargs["{}__gte".format(action_date_column)] = v.get("start_date")
        if v.get("end_date") is not None:
            kwargs["{}__lte".format(action_date_column)] = v.get("end_date")
        # (may have to cast to date) (oct 1 to sept 30)
        if queryset_init:
            or_queryset |= table.objects.filter(**kwargs)
        else:
            queryset_init = True
            or_queryset = table.objects.filter(**kwargs)
    if queryset_init:
        return True, or_queryset
    return False, None


def total_obligation_queryset(amount_obj, model):
    bins = []
    for v in amount_obj:
        lower_bound = v.get("lower_bound")
        upper_bound = v.get("upper_bound")
        for key, limits in WEBSITE_AWARD_BINS.items():
            if lower_bound == limits['lower'] and upper_bound == limits['upper']:
                bins.append(key)
                break

    if len(bins) == len(amount_obj):
        return True, model.objects.filter(total_obl_bin__in=bins)
    else:
        or_queryset = None
        queryset_init = False

        for v in amount_obj:
            if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                if queryset_init:
                    or_queryset |= model.objects.filter(
                        total_obligation__gte=v["lower_bound"],
                        total_obligation__lte=v["upper_bound"]
                    )
                else:
                    queryset_init = True
                    or_queryset = model.objects.filter(
                        total_obligation__gte=v["lower_bound"],
                        total_obligation__lte=v["upper_bound"])
            elif v.get("lower_bound") is not None:
                if queryset_init:
                    or_queryset |= model.objects.filter(
                        total_obligation__gte=v["lower_bound"]
                    )
                else:
                    queryset_init = True
                    or_queryset = model.objects.filter(
                        total_obligation__gte=v["lower_bound"])
            elif v.get("upper_bound") is not None:
                if queryset_init:
                    or_queryset |= model.objects.filter(
                        total_obligation__lte=v["upper_bound"]
                    )
                else:
                    queryset_init = True
                    or_queryset = model.objects.filter(
                        total_obligation__lte=v["upper_bound"])
            else:
                raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
    if queryset_init:
        return True, or_queryset
    return False, None


def can_use_month_aggregation(time_period):
    '''
        time_period is the list of action_date ranges from API
    '''
    try:
        for v in time_period:
            s = generate_date_from_string(v.get("start_date"))
            e = generate_date_from_string(v.get("end_date"))
            if not dates_are_month_bookends(s, e):
                return False
    except Exception:
        return False
    return True
