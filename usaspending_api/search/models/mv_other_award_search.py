from usaspending_api.search.models.base_award_search import BaseAwardSearchModel


class OtherAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_other_award_search"
