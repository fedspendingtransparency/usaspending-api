from usaspending_api.matviews.models.base_award_search import BaseAwardSearchModel


class AwardSearchView(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "vw_award_search"
