from usaspending_api.accounts.models import AppropriationAccountBalances


def populate_final_of_fy():
    AppropriationAccountBalances.populate_final_of_fy()
