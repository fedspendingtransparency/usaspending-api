import pytest

from model_bakery import baker
from usaspending_api.accounts.models import AppropriationAccountBalances


@pytest.fixture
def app_acc_bal_models():
    sub_16_1 = baker.make("submissions.SubmissionAttributes", reporting_fiscal_year=2016)
    sub_16_2 = baker.make("submissions.SubmissionAttributes", reporting_fiscal_year=2016)
    sub_17_1 = baker.make("submissions.SubmissionAttributes", reporting_fiscal_year=2017)
    sub_17_2 = baker.make("submissions.SubmissionAttributes", reporting_fiscal_year=2017)
    tas_1 = baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="ABC", _fill_optional=True)
    tas_2 = baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="XYZ", _fill_optional=True)
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_16_1,
        _fill_optional=True,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_16_2,
        _fill_optional=True,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_17_1,
        _fill_optional=True,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_17_2,
        _fill_optional=True,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        submission=sub_16_1,
        _fill_optional=True,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        submission=sub_16_2,
        _fill_optional=True,
    )
    AppropriationAccountBalances.populate_final_of_fy()


@pytest.mark.django_db
def test_final_of_fy_population(app_acc_bal_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    assert AppropriationAccountBalances.objects.count() == 6
    assert AppropriationAccountBalances.final_objects.count() == 3
    assert AppropriationAccountBalances.objects.filter(final_of_fy=True).count() == 3
