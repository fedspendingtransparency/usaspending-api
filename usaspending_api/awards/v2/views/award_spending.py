from django.db.models import Case, CharField, F, Q, Sum, TextField, Value, When
from django.db.models.functions import Coalesce
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.serializers_v2.serializers import RecipientAwardSpendingSerializer
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import check_valid_toptier_agency
from usaspending_api.common.views import CachedDetailViewSet


class RecipientAwardSpendingViewSet(CachedDetailViewSet):
    """
    Return all award spending by recipient for a given fiscal year and agency id
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/award_spending/recipient.md"
    serializer_class = RecipientAwardSpendingSerializer

    def get_queryset(self):
        # retrieve post request payload
        json_request = self.request.query_params

        # Retrieve fiscal_year & awarding_agency_id from Request
        fiscal_year = json_request.get("fiscal_year")
        awarding_agency_id = json_request.get("awarding_agency_id")

        # Optional Award Category
        award_category = json_request.get("award_category")

        # Required Query Parameters were not Provided
        if not (fiscal_year and awarding_agency_id):
            raise InvalidParameterException(
                "Missing one or more required query parameters: fiscal_year, awarding_agency_id"
            )
        if not check_valid_toptier_agency(awarding_agency_id):
            raise InvalidParameterException("Awarding Agency ID provided must correspond to a toptier agency")

        queryset = TransactionNormalized.objects.filter(
            federal_action_obligation__isnull=False, fiscal_year=fiscal_year, awarding_agency_id=awarding_agency_id
        ).annotate(
            award_category=F("award__category"),
            recipient_name=Coalesce(
                F("award__latest_transaction__assistance_data__awardee_or_recipient_legal"),
                F("award__latest_transaction__contract_data__awardee_or_recipient_legal"),
                output_field=TextField(),
            ),
        )

        if award_category is not None:
            # Filter based on award_category
            if award_category != "other":
                queryset = queryset.filter(award_category=award_category)
            else:
                queryset = queryset.filter(Q(award_category="insurance") | Q(award_category="other")).annotate(
                    award_category=Case(When(award_category="insurance", then=Value("other")), output_field=CharField())
                )

        # Sum Obligations for each Recipient
        queryset = (
            queryset.values("award_category", "recipient_name")
            .annotate(obligated_amount=Sum("federal_action_obligation"))
            .order_by("-obligated_amount")
        )

        return queryset
