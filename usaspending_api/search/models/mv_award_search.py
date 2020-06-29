from usaspending_api.search.models.base_award_search import BaseAwardSearchModel


class AwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_award_search"
