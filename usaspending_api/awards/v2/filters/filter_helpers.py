from django.db.models import Sum, F, Q, FloatField, Case, When
from django.db.models.functions import Coalesce

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.constants import WEBSITE_AWARD_BINS
from usaspending_api.common.helpers import dates_are_fiscal_year_bookends
from usaspending_api.common.helpers import generate_all_fiscal_years_in_range
from usaspending_api.common.helpers import generate_date_from_string
from usaspending_api.common.helpers import dates_are_month_bookends
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping, loan_type_mapping
from usaspending_api.awards.models import TransactionNormalized


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


def sum_transaction_amount(qs, aggregated_name='transaction_amount', filter_types=award_type_mapping,
                           calculate_totals=True):
    """ Returns correct amount for transaction if loan (07, 08) vs all other award types (covers IDV)"""
    aggregate_dict = {}
    if calculate_totals:
        qs = qs.annotate(total_subsidy_cost=Sum(Case(When(type__in=list(loan_type_mapping),
                                                          then=F('original_loan_subsidy_cost')),
                                                     default=0,
                                                     output_field=FloatField())))

        qs = qs.annotate(total_obligation=Sum(Case(When(~Q(type__in=list(loan_type_mapping)),
                                                        then=F('federal_action_obligation')),
                                                   default=0,
                                                   output_field=FloatField())))

    # Coalescing total_obligation and total_subsidy since fields can be null
    if not set(filter_types) & set(loan_type_mapping):
        # just sans loans
        aggregate_dict[aggregated_name] = Coalesce(F('total_obligation'), 0)
    elif set(filter_types) <= set(loan_type_mapping):
        # just loans
        aggregate_dict[aggregated_name] = Coalesce(F('total_subsidy_cost'), 0)
    else:
        # mix of loans and other award types
        # Adding null field to a populated field will return null, used Coalesce to fixes that
        aggregate_dict[aggregated_name] = Coalesce(F('total_subsidy_cost'), 0) + Coalesce(F('total_obligation'), 0)

    return qs.annotate(**aggregate_dict)


def get_total_transaction_columns(filters, model):
    total_transaction_columns = []
    award_types_requested = filters['award_type_codes'] if 'award_type_codes' in filters else award_type_mapping
    awards_sans_loans = [award_type for award_type in award_type_mapping if type not in award_type_mapping]
    award_join = 'award__' if model == TransactionNormalized else ''
    if set(award_types_requested) & set(loan_type_mapping):
        # if there are any loans
        total_transaction_columns.append('{}total_subsidy_cost'.format(award_join))
    if set(award_types_requested) & set(awards_sans_loans):
        # if there is anything else besides loans
        total_transaction_columns.append('{}total_obligation'.format(award_join))
    return total_transaction_columns


def total_obligation_queryset(amount_obj, model, filters):
    bins = []
    for v in amount_obj:
        lower_bound = v.get("lower_bound")
        upper_bound = v.get("upper_bound")
        for key, limits in WEBSITE_AWARD_BINS.items():
            if lower_bound == limits['lower'] and upper_bound == limits['upper']:
                bins.append(key)
                break

    total_transaction_columns = get_total_transaction_columns(filters, model)

    if len(bins) == len(amount_obj):
        return True, model.objects.filter(total_obl_bin__in=bins)
    else:
        or_queryset = None
        queryset_init = False

        for v in amount_obj:
            for column in total_transaction_columns:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    bound_dict = {
                        '{}__gte'.format(column): v["lower_bound"],
                        '{}__lte'.format(column): v["upper_bound"]
                    }
                    if queryset_init:
                        or_queryset |= model.objects.filter(**bound_dict)
                    else:
                        queryset_init = True
                        or_queryset = model.objects.filter(**bound_dict)
                elif v.get("lower_bound") is not None:
                    bound_dict = {
                        '{}__gte'.format(column): v["lower_bound"]
                    }
                    if queryset_init:
                        or_queryset |= model.objects.filter(**bound_dict)
                    else:
                        queryset_init = True
                        or_queryset = model.objects.filter(**bound_dict)
                elif v.get("upper_bound") is not None:
                    bound_dict = {
                        '{}__lte'.format(column): v["upper_bound"]
                    }
                    if queryset_init:
                        or_queryset |= model.objects.filter(**bound_dict)
                    else:
                        queryset_init = True
                        or_queryset = model.objects.filter(**bound_dict)
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


def can_use_total_obligation_enum(amount_obj):
    bins = []
    try:
        for v in amount_obj:
            lower_bound = v.get("lower_bound")
            upper_bound = v.get("upper_bound")
            for key, limits in WEBSITE_AWARD_BINS.items():
                if lower_bound == limits['lower'] and upper_bound == limits['upper']:
                    bins.append(key)
                    break

        if len(bins) == len(amount_obj):
            return True
    except Exception:
        pass
    return False
