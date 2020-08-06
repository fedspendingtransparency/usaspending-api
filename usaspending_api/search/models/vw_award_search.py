from django_cte import CTEManager

from usaspending_api.search.models.base_award_search import BaseAwardSearchModel


class AwardSearchView(BaseAwardSearchModel):
    objects = CTEManager()

    class Meta:
        managed = False
        db_table = "vw_award_search"
