from usaspending_api.matviews.models.base_award_search import BaseAwardSearchModel


class LoanAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_loan_award_search"
