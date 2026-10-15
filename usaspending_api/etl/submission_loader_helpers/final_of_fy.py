from usaspending_api.accounts.models import AppropriationAccountBalances


def populate_final_of_fy(is_single_submission: bool = False) -> None:
    AppropriationAccountBalances.populate_final_of_fy(is_single_submission=is_single_submission)
