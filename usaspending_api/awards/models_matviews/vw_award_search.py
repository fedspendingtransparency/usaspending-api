from usaspending_api.awards.models_matviews.base_mv_model import BaseAwardSearchModel


class AwardSearchView(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "vw_award_search"
