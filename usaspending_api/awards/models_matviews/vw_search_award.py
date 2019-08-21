from usaspending_api.awards.models_matviews.base_mv_model import BaseSearchAwardModel


class ViewSearchAward(BaseSearchAwardModel):
    class Meta:
        managed = False
        db_table = "vw_search_award"
