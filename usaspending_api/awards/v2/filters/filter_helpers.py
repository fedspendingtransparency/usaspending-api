from collections import namedtuple
from datetime import datetime
from datetime import timedelta
import logging

from django.db.models import Sum, F, Q, Case, When
from django.db.models.functions import Coalesce

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.models_matviews import SubawardView
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.awards.v2.lookups.lookups import loan_type_mapping
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import dates_are_month_bookends
from usaspending_api.common.helpers.generic_helper import generate_date_from_string
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.references.constants import WEBSITE_AWARD_BINS


logger = logging.getLogger(__name__)


Range = namedtuple('Range', ['start', 'end'])


def merge_date_ranges(date_range_list):
    """
        Given a list of date ranges (using the defined namedtuple "Range"), combine overlapping date ranges
        While adjacent fiscal years do not overlap the desired behavior is to combine them
            FY2010 ends on 2010-09-30, FY2011 start on 2010-10-01.
        To address this, when comparing ranges 1 day is removed from the start date and 1 day is added to the end date
        Then the overlapping ranges must be > 1 instead of > 0
        Inspired by Raymond Hettinger [https://stackoverflow.com/a/9044111]
    """
    ordered_list = sorted([sorted(t) for t in date_range_list])
    saved_range = Range(start=ordered_list[0][0], end=ordered_list[0][1])
    for st, en in ordered_list[1:]:
        r = Range(st, en)
        latest_start = max(r.start, saved_range.start) + timedelta(days=-1)
        earliest_end = min(r.end, saved_range.end) + timedelta(days=1)
        delta = (earliest_end - latest_start).days + 1  # added since ranges are closed on both ends
        if delta > 1:  # since the overlap is potentially extended by 1-2 days, the overlap needs to be at least 2 days
            saved_range = Range(start=min(saved_range.start, st), end=max(saved_range.end, en))
        else:
            yield (saved_range.start, saved_range.end)
            saved_range = Range(start=st, end=en)
    yield (saved_range.start, saved_range.end)


def date_list_to_queryset(date_list, table):
    or_queryset = Q()
    for v in date_list:
        # Modified May 2018 so that there will always be a start and end value from combine_date_range_queryset()

        date_type = v.get("date_type", "action_date")
        if date_type not in ["action_date", "last_modified_date", "date_signed"]:
            raise InvalidParameterException('Invalid date_type: {}'.format(date_type))

        kwargs = {
            "{}__gte".format(date_type): v["start_date"],
            "{}__lte".format(date_type): v["end_date"],
        }
        or_queryset |= Q(**kwargs)

    return table.objects.filter(or_queryset)


def combine_date_range_queryset(date_dicts, table, min_start, max_end, dt_format='%Y-%m-%d'):
    final_ranges = []
    for date_type in set([v.get('date_type', 'action_date') for v in date_dicts]):
        list_of_ranges = [
            (
                datetime.strptime(v.get('start_date', None) or min_start, dt_format),
                datetime.strptime(v.get('end_date', None) or max_end, dt_format)
            ) for v in date_dicts]  # convert date strings to datetime objects

        final_ranges.extend([{'start_date': r[0], 'end_date': r[1], 'date_type': date_type}
                             for r in list(merge_date_ranges(list_of_ranges))])
    return date_list_to_queryset(final_ranges, table)


def sum_transaction_amount(qs, aggregated_name='transaction_amount', filter_types=award_type_mapping,
                           calculate_totals=True):
    """
    Returns queryset with aggregation (annotated with aggregation_name) for transactions if loan (07, 08)
    vs all other award types (covers IDV)
    """
    aggregate_dict = {}
    if calculate_totals:
        qs = qs.annotate(total_subsidy_cost=Sum(Case(When(type__in=list(loan_type_mapping),
                                                          then=F('original_loan_subsidy_cost')),
                                                     default=0)))

        qs = qs.annotate(total_obligation=Sum(Case(When(~Q(type__in=list(loan_type_mapping)),
                                                        then=F('federal_action_obligation')),
                                                   default=0)))

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
    """
    Returns array of query strings with column joins based on request filters. Used to calculate award amounts where
    rows can be award type loan and/or all other award types
    """
    total_transaction_columns = []
    if model == SubawardView:
        total_transaction_columns.append('amount')
    else:
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

    if len(bins) == len(amount_obj):
        or_queryset = model.objects.filter(total_obl_bin__in=bins)
    else:
        total_transaction_columns = get_total_transaction_columns(filters, model)
        bound_filters = Q()

        for v in amount_obj:
            for column in total_transaction_columns:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    bound_dict = {
                        '{}__gte'.format(column): v["lower_bound"],
                        '{}__lte'.format(column): v["upper_bound"]
                    }

                elif v.get("lower_bound") is not None:
                    bound_dict = {
                        '{}__gte'.format(column): v["lower_bound"]
                    }
                elif v.get("upper_bound") is not None:
                    bound_dict = {
                        '{}__lte'.format(column): v["upper_bound"]
                    }
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
                bound_filters |= Q(**bound_dict)
        or_queryset = model.objects.filter(bound_filters)
    return or_queryset


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


def only_action_date_type(time_period):
    '''
        if a date_type is last_modified_date, don't use the matview this applies to
    '''
    try:
        for v in time_period:
            if v.get('date_type', 'action_date') != 'action_date':
                return False
    except Exception:
        return False
    return True


def transform_keyword(request, api_version):
    filter_obj = request.data.get("filters", None)
    if filter_obj:
        if "keyword" not in filter_obj and "keywords" not in filter_obj:
            return request
        keyword_array_passed = filter_obj.get('keywords', False)
        keyword_string_passed = filter_obj.pop("keyword", None)
        if api_version < 3:
            keywords = keyword_array_passed if keyword_array_passed else [keyword_string_passed]
        else:
            if keyword_array_passed:
                keywords = keyword_array_passed
            else:
                raise InvalidParameterException("keyword' is deprecated. Please use 'keywords'."
                                                "See documentation for more information.")
        filter_obj['keywords'] = keywords
        request.data["filters"] = filter_obj
    return request


def get_descendant_award_ids(root_idv_award_id, include_child_idvs):
    """
    Unfortunately, there's no clean way to get IDV descendants using the Django
    ORM so we will turn to the dark side to get what we need.  For the provided
    IDV award id (surrogate, integer, internal award id), this function will
    return the award id for all child Awards and grandchild Awards and, if
    include_child_idvs is True, all child IDVs as well.
    """
    sql = """
        with cte as (
            select      award_id
            from        parent_award
            where       award_id = %(root_idv_award_id)s
            union all
            select      cpa.award_id
            from        parent_award ppa
                        inner join parent_award cpa on
                            cpa.parent_award_id = ppa.award_id
            where       ppa.award_id = %(root_idv_award_id)s
        )
        select  ca.id
        from    cte
                inner join awards pa on
                    pa.id = cte.award_id
                inner join awards ca on
                    ca.parent_award_piid = pa.piid and
                    ca.fpds_parent_agency_id = pa.fpds_agency_id
    """ + ("" if include_child_idvs else " and ca.type not like 'IDV%%'")
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(sql, {'root_idv_award_id': root_idv_award_id})
        return [row[0] for row in cursor.fetchall()]
