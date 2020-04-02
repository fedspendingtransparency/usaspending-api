from django.db.models import Exists, F, OuterRef, Q
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.business_logic_helpers import cfo_presentation_order
from usaspending_api.references.models import Agency, SubtierAgency, ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes

ACCOUNT_AGENCIES = "account_agencies"
AWARD_AGENCIES = "award_agencies"
AGENCY_LIST_TYPES = (ACCOUNT_AGENCIES, AWARD_AGENCIES)


class DownloadListAgenciesViewSet(APIView):
    """
    This route returns one of three result set flavors.  For "account_agencies" requests it returns a list
    of all toptier agencies with at least one DABS submission.  For "award_agencies" requests it returns a
    list of all user selectable flagged toptier agencies with at least one subtier agency.  For specific agency
    requests it returns a list of all user selectable flagged subtier agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/bulk_download/list_agencies.md"

    def post(self, request):

        post_data = request.data or {}
        toptier_agency_id = post_data.get("agency")
        list_type = post_data.get("type")
        if list_type not in AGENCY_LIST_TYPES:
            raise InvalidParameterException(f"type must be one of: {AGENCY_LIST_TYPES}")

        if list_type == ACCOUNT_AGENCIES:
            # Get toptier agencies that have DABS submissions.  As per discussion with PO, these are not
            # subject to user selectable flag.
            toptier_agencies = ToptierAgency.objects.filter(
                toptier_code__in=SubmissionAttributes.objects.filter(toptier_code__isnull=False)
                .distinct()
                .values("toptier_code")
            )

        else:  # AWARD_AGENCIES
            # Get toptier agencies that have a subtier and for which the user_selectable flag is True.
            toptier_agencies = ToptierAgency.objects.annotate(
                include_agency=Exists(
                    Agency.objects.filter(
                        user_selectable=True, subtier_agency_id__isnull=False, toptier_agency_id=OuterRef("pk")
                    ).values("pk")
                )
            ).filter(include_agency=True)

        toptier_agencies = toptier_agencies.values("name", "toptier_agency_id", "toptier_code")
        response_data = {"agencies": [], "sub_agencies": []}

        if not toptier_agency_id:
            # Convert to list since we'll be iterating over the results twice which would otherwise run the query twice.
            toptier_agencies = list(toptier_agencies)

            response_data["agencies"] = cfo_presentation_order(toptier_agencies)

        else:
            # Get the top tier agency object based on the agency id provided.  It must still meet all of the
            # account/award type filtering above.
            toptier_agency = toptier_agencies.filter(pk=toptier_agency_id).first()
            if not toptier_agency:
                raise InvalidParameterException("Agency ID not found")

            # Get the subtier agencies associated with the toptier agency but only if they're user selectable and
            # the subtier name doesn't match the toptier name.
            response_data["sub_agencies"] = list(
                SubtierAgency.objects.annotate(
                    include_agency=Exists(
                        Agency.objects.filter(
                            ~Q(toptier_agency__name=OuterRef("name")),
                            user_selectable=True,
                            subtier_agency_id=OuterRef("pk"),
                            toptier_agency_id=toptier_agency_id,
                        ).values("pk")
                    )
                )
                .filter(include_agency=True)
                .values(subtier_agency_name=F("name"))
                .distinct()
                .order_by("subtier_agency_name")
            )

        return Response(response_data)
