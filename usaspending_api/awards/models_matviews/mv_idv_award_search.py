from usaspending_api.awards.models_matviews.base_award_search import BaseAwardSearchModel


class IDVAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_idv_award_search"
