import logging

from django.db.models import Q

from usaspending_api.references.models import Agency
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC, DOD_CGAC

logger = logging.getLogger(__name__)


def spending_filter(alt_set, queryset, filters, _type):
    dod_agency = str(Agency.objects.filter(toptier_agency__toptier_code=DOD_CGAC).values_list("id", flat=True).first())
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
        ]

        if key not in key_list:
            raise InvalidParameterException(
                key + " filter does not exist. "
                "Valid Filters: budget_function, budget_subfunction, federal_account,"
                "program_activity, object_class, recipient, award, award_category,"
                "agency, agency_type, fy, quarter."
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
                alt_set = alt_set.filter(award__recipient__recipient_name=value)

            # award, award_category
            elif key == "award" or key == "award_category":
                alt_set = alt_set.filter(award__id=value)

            # agency
            elif key == "agency":
                # TODO: Will need to incorporate "agency_type" here to filter based on toptier or subtier.
                # Currently default to filtering on toptier
                if value == dod_agency:
                    dod_agencies = Agency.objects.filter(
                        toptier_flag=True, toptier_agency__toptier_code__in=DOD_ARMED_FORCES_CGAC
                    ).values_list("toptier_agency", flat=True)

                    and_alt_set = Q(treasury_account__funding_toptier_agency__in=dod_agencies)
                else:
                    agency = Agency.objects.filter(toptier_flag=True, id=value).first()
                    if agency is None:
                        raise InvalidParameterException("Agency ID provided does not correspond to a toptier agency")

                    and_alt_set = Q(treasury_account__funding_toptier_agency=agency.toptier_agency)

                alt_set = alt_set.filter(and_alt_set)

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
                    treasury_account__in=alt_set.filter(award__recipient__recipient_name=value).values_list(
                        "treasury_account_id", flat=True
                    )
                )

            # award, award_category
            elif key == "award" or key == "award_category":
                and_queryset = queryset.filter(
                    treasury_account__in=alt_set.filter(award__id=value).values_list("treasury_account_id", flat=True)
                )
                queryset &= and_queryset

            # agency
            elif key == "agency":
                # TODO: Will need to incorporate "agency_type" here to filter based on toptier or subtier.

                # Currently default to filtering on toptier

                if value == dod_agency:
                    dod_agencies = Agency.objects.filter(
                        toptier_flag=True, toptier_agency__toptier_code__in=DOD_ARMED_FORCES_CGAC
                    ).values_list("toptier_agency", flat=True)

                    and_queryset = Q(treasury_account__funding_toptier_agency__in=dod_agencies)
                else:
                    agency = Agency.objects.filter(toptier_flag=True, id=value).first()
                    if agency is None:
                        raise InvalidParameterException("Agency ID provided does not correspond to a toptier agency")

                    and_queryset = Q(treasury_account__funding_toptier_agency=agency.toptier_agency)
                queryset = queryset.filter(and_queryset)

    return alt_set, queryset
