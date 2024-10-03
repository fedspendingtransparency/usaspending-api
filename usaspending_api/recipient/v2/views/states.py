import logging
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime

from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year
from usaspending_api.common.helpers.orm_helpers import StringAggWithDefault
from usaspending_api.recipient.models import StateData
from usaspending_api.recipient.v2.helpers import validate_year
from usaspending_api.search.models import SummaryStateView

logger = logging.getLogger(__name__)

# Storing FIPS codes + state codes in memory to avoid hitting the database for the same data
VALID_FIPS = {}


def populate_fips():
    global VALID_FIPS

    if not VALID_FIPS:
        VALID_FIPS = {
            fips_code: {"code": state_code, "name": state_name, "type": state_type}
            for fips_code, state_code, state_name, state_type in list(
                StateData.objects.distinct("fips").values_list("fips", "code", "name", "type")
            )
        }


def validate_fips(fips):
    global VALID_FIPS
    populate_fips()

    if fips not in VALID_FIPS:
        raise InvalidParameterException("Invalid fips: {}.".format(fips))

    return fips


def obtain_state_totals(fips, year=None, award_type_codes=None, subawards=False):

    # Determine the fiscal year filter based on the provided year
    if year == "latest":
        filters = {"fiscal_year": generate_fiscal_year(datetime.now())}
    elif year == "all":
        filters = {}  # No fiscal year filter; this will include all years
    else:
        filters = {"fiscal_year": year}

    filters["pop_state_code"] = VALID_FIPS[fips]["code"]

    if award_type_codes:
        filters["type__in"] = award_type_codes

    if not subawards:
        queryset = (
            SummaryStateView.objects.filter(**filters)
            .values("pop_state_code")
            .annotate(
                total=Sum("generated_pragmatic_obligation"),
                distinct_awards=StringAggWithDefault("distinct_awards", ","),
                total_face_value_loan_amount=Sum("face_value_loan_guarantee"),
                outlay_total=Sum("total_outlays"),
            )
            .values("distinct_awards", "pop_state_code", "total", "total_face_value_loan_amount", "outlay_total")
        )
        # if award_type_codes is not None and type(award_type_codes) == list:
        #     queryset = queryset.filter(type__in = award_type_codes)

    try:
        row = list(queryset)[0]
        result = {
            "pop_state_code": row["pop_state_code"],
            "total": row["total"],
            "count": len(set(row["distinct_awards"].split(","))),
            "total_face_value_loan_amount": row["total_face_value_loan_amount"],
            "total_outlays": row["outlay_total"],
        }
        return result
    except IndexError:
        # would prefer to catch an index error gracefully if the SQL query produces 0 rows
        logger.warning("No results found for FIPS {} with filters: {}".format(fips, filters))

    return {"count": 0, "pop_state_code": None, "total": 0, "total_face_value_loan_amount": 0, "total_outlays": 0}


def get_all_states(year=None, award_type_codes=None, subawards=False):
    fiscal_year = year if year != "latest" else generate_fiscal_year(datetime.now())
    filters = {"fiscal_year": fiscal_year, "pop_country_code": "USA", "pop_state_code__isnull": False}

    if award_type_codes:
        filters["type__in"] = award_type_codes

    if subawards:
        # Currently, subawards are not supported by this function
        return []
    else:
        # calculate award total filtered by state
        fiscal_year_queryset = (
            SummaryStateView.objects.filter(**filters)
            .values("pop_state_code")
            .annotate(
                total=Sum("generated_pragmatic_obligation"),
                distinct_awards=StringAggWithDefault("distinct_awards", ","),
                outlay_total=Sum("total_outlays"),
            )
            .values("pop_state_code", "total", "distinct_awards", "outlay_total")
        )
        results = [
            {
                "pop_state_code": row["pop_state_code"],
                "total": row["total"],
                "count": len(set(row["distinct_awards"].split(","))),
                "total_outlays": row["outlay_total"],
            }
            for row in list(fiscal_year_queryset)
        ]

        existing_state_codes = [state["pop_state_code"] for state in results]

        # Get all other states and return `0` for their award count and totals
        all_states_queryset = SummaryStateView.objects.all().distinct("pop_state_code").values("pop_state_code")
        for row in all_states_queryset:
            if row["pop_state_code"] not in existing_state_codes:
                results.append(
                    {
                        "pop_state_code": row["pop_state_code"],
                        "total": 0,
                        "count": 0,
                        "total_outlays": 0,
                    }
                )

    return results


class StateMetaDataViewSet(APIView):
    """
    This route returns basic information about the specified state.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/state/fips.md"

    def get_state_data(self, state_data_results, field, year=None):
        """Finds which earliest or latest state data to use based on the year and what data is available"""
        state_data = OrderedDict(
            sorted(
                [(str(state_data["year"]), state_data) for state_data in state_data_results if state_data[field]],
                key=lambda pair: pair[0],
            )
        )
        earliest = list(state_data.keys())[0]
        latest = list(state_data.keys())[-1]
        if year and year.isdigit() and year < earliest:
            return state_data[earliest]
        elif year and year.isdigit() and earliest <= year <= latest:
            return state_data[year]
        else:
            return state_data[latest]

    @cache_response()
    def get(self, request, fips):
        get_request = request.query_params
        year = validate_year(get_request.get("year", "latest"))
        fips = validate_fips(fips)

        state_data_qs = StateData.objects.filter(fips=fips)
        state_data_results = state_data_qs.values()
        general_state_data = state_data_results[0]
        state_pop_data = self.get_state_data(state_data_results, "population", year)
        state_mhi_data = self.get_state_data(state_data_results, "median_household_income", year)

        state_aggregates = obtain_state_totals(fips, year=year)
        state_loans = obtain_state_totals(fips, year=year, award_type_codes=all_award_types_mappings["loans"])

        if year == "all" or (year and year.isdigit() and int(year) == generate_fiscal_year(datetime.now())):
            amt_per_capita = None
        else:
            amt_per_capita = (
                round(state_aggregates["total"] / state_pop_data["population"], 2) if state_aggregates["count"] else 0
            )

        result = {
            "name": general_state_data["name"],
            "code": general_state_data["code"],
            "fips": general_state_data["fips"],
            "type": general_state_data["type"],
            "population": state_pop_data["population"],
            "pop_year": state_pop_data["year"],
            "pop_source": state_pop_data["pop_source"],
            "median_household_income": state_mhi_data["median_household_income"],
            "mhi_year": state_mhi_data["year"],
            "mhi_source": state_mhi_data["mhi_source"],
            "total_prime_amount": state_aggregates["total"],
            "total_prime_awards": state_aggregates["count"],
            "total_face_value_loan_amount": state_aggregates["total_face_value_loan_amount"],
            "total_face_value_loan_prime_awards": state_loans["count"],
            "award_amount_per_capita": amt_per_capita,
            "total_outlays": state_aggregates["total_outlays"],
            # Commented out for now
            # 'total_subaward_amount': total_subaward_amount,
            # 'total_subawards': total_subaward_count,
        }

        return Response(result)


# The StateAwardBreakdownViewSet endpoint is not yet ready to support IDVs.
# We will remove "idvs" from the global all_award_types_mappings so that this
# endpoint continues to behave normally.  When ready to support idvs, remove
# this bit here and replace the reference to _all_award_types_mappings below
# with all_award_types_mappings.
_all_award_types_mappings = deepcopy(all_award_types_mappings)
if "idvs" in _all_award_types_mappings:
    del _all_award_types_mappings["idvs"]


class StateAwardBreakdownViewSet(APIView):
    """
    This endpoint returns the award amounts and totals, based on award
    type, of a specific state or territory, given its USAspending.gov `id`.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/state/awards/fips.md"

    @cache_response()
    def get(self, request, fips):
        get_request = request.query_params
        year = validate_year(get_request.get("year", "latest"))
        fips = validate_fips(fips)

        results = []
        for award_type, award_type_codes in _all_award_types_mappings.items():
            result = obtain_state_totals(fips, year=year, award_type_codes=award_type_codes)
            results.append(
                {
                    "type": award_type,
                    "amount": result["total"],
                    "count": result["count"],
                    "total_outlays": result["total_outlays"],
                }
            )
        return Response(results)


class ListStates(APIView):
    """
    This endpoint returns a list of states and their amounts.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/recipient/state.md"

    @cache_response()
    def get(self, request):
        populate_fips()
        valid_states = {v["code"]: k for k, v in VALID_FIPS.items()}
        results = []
        for item in get_all_states(year="latest"):
            if item["pop_state_code"] not in valid_states.keys():
                continue
            fips = valid_states[item["pop_state_code"]]
            results.append(
                {
                    "fips": fips,
                    "code": item["pop_state_code"],
                    "name": VALID_FIPS[fips]["name"],
                    "type": VALID_FIPS[fips]["type"],
                    "amount": item["total"],
                    "count": item["count"],
                }
            )
        return Response(results)
