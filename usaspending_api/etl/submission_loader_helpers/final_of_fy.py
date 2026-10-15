from usaspending_api.accounts.models import AppropriationAccountBalances


def populate_final_of_fy(
    is_single_submission: bool = False,
    fiscal_year: int | None = None,
    tai_list: list | None = None
) -> None:
    AppropriationAccountBalances.populate_final_of_fy(
        is_single_submission=is_single_submission,
        fiscal_year=fiscal_year,
        tai_list=tai_list
    )
