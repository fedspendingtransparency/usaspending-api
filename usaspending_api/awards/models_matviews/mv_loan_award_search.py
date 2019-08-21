from usaspending_api.awards.models_matviews.base_mv_model import BaseAwardSearchModel


class LoanAwardSearchMatview(BaseAwardSearchModel):
    class Meta:
        managed = False
        db_table = "mv_loan_award_search"
