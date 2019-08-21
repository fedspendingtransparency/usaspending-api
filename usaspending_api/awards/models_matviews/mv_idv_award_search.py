from usaspending_api.awards.models_matviews.base_mv_model import BaseAwardSearchModel


class IDVAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_idv_award_search"
