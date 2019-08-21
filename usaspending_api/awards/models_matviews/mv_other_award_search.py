from usaspending_api.awards.models_matviews.base_mv_model import BaseAwardSearchModel


class OtherAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_other_award_search"
