from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


def populate_final_of_fy():
    AppropriationAccountBalances.populate_final_of_fy()
    FinancialAccountsByProgramActivityObjectClass.populate_final_of_fy()
