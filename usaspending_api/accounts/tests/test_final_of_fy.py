from datetime import date

import pytest
from model_mommy import mommy

from usaspending_api.accounts.models import AppropriationAccountBalances


@pytest.fixture
def app_acc_bal_models():
    sub_16_1 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2016, 1, 1))
    sub_16_2 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2016, 4, 1))
    sub_17_1 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2016, 10, 1))
    sub_17_2 = mommy.make("submissions.SubmissionAttributes", reporting_period_start=date(2017, 2, 1))
    tas_1 = mommy.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="ABC", _fill_optional=True)
    tas_2 = mommy.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="XYZ", _fill_optional=True)
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_16_1,
        _fill_optional=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_16_2,
        _fill_optional=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_17_1,
        _fill_optional=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_1,
        submission=sub_17_2,
        _fill_optional=True,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=tas_2,
        submission=sub_16_1,
        _fill_optional=True,
    )
    mommy.make(
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
