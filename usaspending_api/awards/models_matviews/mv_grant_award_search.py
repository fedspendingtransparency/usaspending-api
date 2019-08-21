from usaspending_api.awards.models_matviews.base_mv_model import BaseAwardSearchModel


class GrantAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_grant_award_search"
