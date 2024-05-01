import logging

from django.db.models import Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)


def spending_filter(alt_set, queryset, filters, _type):
    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException("Invalid filter: " + key + " has null as its value.")

        key_list = [
            "budget_function",
            "budget_subfunction",
            "federal_account",
            "program_activity",
            "object_class",
            "recipient",
            "award",
            "award_category",
            "agency",
            "agency_type",
            "fy",
            "quarter",
            "period",
        ]

        if key not in key_list:
            raise InvalidParameterException(
                key + " filter does not exist. "
                "Valid Filters: budget_function, budget_subfunction, federal_account,"
                "program_activity, object_class, recipient, award, award_category,"
                "agency, agency_type, fy, quarter, period."
            )

        # Check _type to filter on correct set (alt_set or queryset)
        alt_set_keys = ["recipient", "award", "award_category", "agency_type"]
        if _type in alt_set_keys:
            # Apply filters
            # budget_function
            if key == "budget_function":
                alt_set = alt_set.filter(treasury_account__budget_function_code=value)

            # budget_subfunction
            elif key == "budget_subfunction":
                alt_set = alt_set.filter(treasury_account__budget_subfunction_code=value)

            # federal_account
            elif key == "federal_account":
                alt_set = alt_set.filter(treasury_account__federal_account=value)

            # program_activity
            elif key == "program_activity":
                alt_set = alt_set.filter(program_activity=value)

            # object_class
            elif key == "object_class":
                alt_set = alt_set.filter(object_class__major_object_class=value)

            # recipient
            elif key == "recipient":
                alt_set = alt_set.filter(Q(award__latest_transaction_search__recipient_name=value))

            # award, award_category
            elif key == "award" or key == "award_category":
                alt_set = alt_set.filter(award__award_id=value)

            # agency
            elif key == "agency":
                toptier_agency = ToptierAgency.objects.filter(agency__id=value, agency__toptier_flag=True).first()
                if toptier_agency is None:
                    raise InvalidParameterException("Agency ID provided does not correspond to a toptier agency")
                alt_set = alt_set.filter(treasury_account__funding_toptier_agency=toptier_agency)

        # All other _type
        else:
            # budget_function
            if key == "budget_function":
                queryset = queryset.filter(treasury_account__budget_function_code=value)

            # budget_subfunction
            elif key == "budget_subfunction":
                queryset = queryset.filter(treasury_account__budget_subfunction_code=value)

            # federal_account
            elif key == "federal_account":
                queryset = queryset.filter(treasury_account__federal_account=value)

            # program_activity
            elif key == "program_activity":
                queryset = queryset.filter(program_activity=value)

            # object_class
            elif key == "object_class":
                queryset = queryset.filter(object_class__major_object_class=value)

            # recipient
            elif key == "recipient":
                queryset = queryset.filter(
                    treasury_account__in=alt_set.filter(
                        Q(award__latest_transaction_search__recipient_name_raw=value)
                    ).values_list("treasury_account_id", flat=True)
                )

            # award, award_category
            elif key == "award" or key == "award_category":
                queryset = queryset.filter(
                    treasury_account__in=alt_set.filter(award__award_id=value).values_list(
                        "treasury_account_id", flat=True
                    )
                )

            # agency
            elif key == "agency":
                toptier_agency = ToptierAgency.objects.filter(agency__id=value, agency__toptier_flag=True).first()
                if toptier_agency is None:
                    raise InvalidParameterException("Agency ID provided does not correspond to a toptier agency")
                queryset = queryset.filter(treasury_account__funding_toptier_agency=toptier_agency)

    return alt_set, queryset
