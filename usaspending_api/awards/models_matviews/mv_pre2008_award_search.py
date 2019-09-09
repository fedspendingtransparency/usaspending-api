from usaspending_api.awards.models_matviews.base_award_search import BaseAwardSearchModel


class Pre2008AwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_pre2008_award_search"
