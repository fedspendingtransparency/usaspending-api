from django.db import models
from django.db.models import Q, F

from usaspending_api.search.models.base_award_search import BaseAwardSearchModel


class AwardSearch(BaseAwardSearchModel):

    class Meta:
        db_table = 'rpt"."award_search'
        indexes = [
            models.Index(fields=["award_id"], name="as_idx_award_id"),
            models.Index(
                fields=["recipient_hash"], name="as_idx_recipient_hash", condition=Q(action_date__gte="2007-10-01")
            ),
            models.Index(
                fields=["recipient_unique_id"],
                name="as_idx_recipient_unique_id",
                condition=Q(recipient_unique_id__isnull=False) & Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                F("action_date").desc(nulls_last=True),
                name="as_idx_action_date",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["funding_agency_id"],
                name="as_idx_funding_agency_id",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_location_congressional_code"],
                name="as_idx_recipient_cong_code",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_location_county_code"],
                name="as_idx_recipient_county_code",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            models.Index(
                fields=["recipient_location_state_code"],
                name="as_idx_recipient_state_code",
                condition=Q(action_date__gte="2007-10-01"),
            ),
            # mimicking transaction_search's indexes, this additional index accounts for pre-2008 data
            models.Index(
                F("action_date").desc(nulls_last=True),
                name="as_idx_action_date_pre2008",
                condition=Q(action_date__lt="2007-10-01"),
            ),
        ]
