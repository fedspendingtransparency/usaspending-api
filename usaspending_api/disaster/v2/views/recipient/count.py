from django.db.models import Count, Case, When, F, TextField, Q, Exists, OuterRef
from django.db.models.functions import Cast
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.awards.models import FinancialAccountsByAwards, FinancialAccountMonetaryMatview
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES
from usaspending_api.search.models import AwardSearchView


class RecipientCountViewSet(DisasterBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        # faba_filters = [
        #     self.all_closed_defc_submissions,
        #     self.is_in_provided_def_codes,
        #     self.has_award_of_provided_type,
        # ]

        # dollar_annotations = {
        #     "inner_obligation": self.obligated_field_annotation,
        #     "inner_outlay": self.outlay_field_annotation,
        # }

        # cte = With(
        #     FinancialAccountsByAwards.objects.filter(*faba_filters).values("award_id").annotate(**dollar_annotations)
        # )

        # recipients = (
        #     cte.join(AwardSearchView, award_id=cte.col.award_id)
        #     .with_cte(cte)
        #     .annotate(total_obligation_by_award=cte.col.inner_obligation, total_outlay_by_award=cte.col.inner_outlay)

        #     .values("award_id")
        #     .aggregate(
        #         count=Count(
        #             Case(
        #                 When(recipient_name__in=SPECIAL_CASES, then=F("recipient_name")),
        #                 default=Cast(F("recipient_hash"), output_field=TextField()),
        #             ),
        #             distinct=True,
        #         )
        #     )
        # )



        # NEED TO INCLUDE FILTER FOR award type codes
        recipients = (
            FinancialAccountMonetaryMatview.objects.filter(def_code__in=self.def_codes)
            .values("award_id")
            .aggregate(
                count=Count(
                    Case(
                        When(recipient_name__in=SPECIAL_CASES, then=F("AwardSearchView__recipient_name")),
                        default=Cast(F("AwardSearchView__recipient_hash"), output_field=TextField()),
                    ),
                    distinct=True,
                )
            )
        )

        return Response({"count": recipients["count"]})
