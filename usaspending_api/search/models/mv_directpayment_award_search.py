from usaspending_api.search.models.base_award_search import BaseAwardSearchModel


class DirectPaymentAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_directpayment_award_search"
