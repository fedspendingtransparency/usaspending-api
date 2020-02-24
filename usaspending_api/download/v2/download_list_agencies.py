from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.models import Agency
from usaspending_api.references.models import SubtierAgency, ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes


class DownloadListAgenciesViewSet(APIView):
    """
    This route lists all the agencies and the subagencies or federal accounts associated under specific agencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/bulk_download/list_agencies.md"
    sub_agencies_map = {}

    def pull_modified_agencies_cgacs_subtiers(self):
        self.sub_agencies_map = {
            sa["subtier_code"]: sa["agency__toptier_agency__toptier_code"]
            for sa in SubtierAgency.objects.filter(agency__user_selectable=True).values(
                "subtier_code", "agency__toptier_agency__toptier_code"
            )
        }

    def post(self, request):
        """Return list of agencies if no POST data is provided.
        Otherwise, returns sub_agencies/federal_accounts associated with the agency provided"""
        response_data = {"agencies": [], "sub_agencies": [], "federal_accounts": []}

        agency_id = None
        post_data = request.data
        if post_data:
            if "agency" not in post_data:
                raise InvalidParameterException("Missing one or more required body parameters: agency")
            agency_id = post_data["agency"]

        # Get all the top tier agencies that have submissions.
        submitters = SubmissionAttributes.objects.filter(toptier_code__isnull=False).distinct().values("toptier_code")
        toptier_agencies = list(
            ToptierAgency.objects.filter(toptier_code__in=submitters).values(
                "name", "toptier_agency_id", "toptier_code"
            )
        )

        if not agency_id:
            # Return all the agencies if no agency id provided
            cfo_agencies = sorted(
                [a for a in toptier_agencies if a["toptier_code"] in CFO_CGACS],
                key=lambda a: CFO_CGACS.index(a["toptier_code"]),
            )
            other_agencies = sorted(
                [a for a in toptier_agencies if a["toptier_code"] not in CFO_CGACS], key=lambda a: a["name"]
            )
            response_data["agencies"] = {"cfo_agencies": cfo_agencies, "other_agencies": other_agencies}
        else:
            if not self.sub_agencies_map:
                # populate the sub_agencies dictionary
                self.pull_modified_agencies_cgacs_subtiers()

            # Get the top tier agency object based on the agency id provided
            top_tier_agency = list(filter(lambda toptier: toptier["toptier_agency_id"] == agency_id, toptier_agencies))
            if not top_tier_agency:
                raise InvalidParameterException("Agency ID not found")
            top_tier_agency = top_tier_agency[0]
            # Get the sub agencies and federal accounts associated with that top tier agency
            # Removed distinct subtier_agency_name since removing subtiers with multiple codes that aren't in the
            # modified list
            response_data["sub_agencies"] = (
                Agency.objects.filter(toptier_agency_id=agency_id)
                .values(
                    subtier_agency_name=F("subtier_agency__name"), subtier_agency_code=F("subtier_agency__subtier_code")
                )
                .order_by("subtier_agency_name")
            )
            # Tried converting this to queryset filtering but ran into issues trying to
            # double check the right used subtier_agency by cross checking the toptier_code
            # see the last 2 lines of the list comprehension below
            response_data["sub_agencies"] = [
                subagency
                for subagency in response_data["sub_agencies"]
                if subagency["subtier_agency_code"] in self.sub_agencies_map
                and self.sub_agencies_map[subagency["subtier_agency_code"]] == top_tier_agency["toptier_code"]
            ]
            for subagency in response_data["sub_agencies"]:
                del subagency["subtier_agency_code"]

            response_data["federal_accounts"] = (
                FederalAccount.objects.filter(agency_identifier=top_tier_agency["toptier_code"])
                .values(federal_account_name=F("account_title"), federal_account_id=F("id"))
                .order_by("federal_account_name")
            )
        return Response(response_data)
