import pytest
from model_bakery import baker

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.management.commands.delta_downloads.award_financial.filters import AccountDownloadFilter


@pytest.fixture
def agency_models(db):
    baker.make("references.ToptierAgency", pk=1, toptier_code="123")
    baker.make("references.ToptierAgency", pk=2, toptier_code="456")
    baker.make("references.ToptierAgency", pk=3, toptier_code="789")


@pytest.fixture
def federal_account_models(db):
    baker.make("accounts.FederalAccount", pk=1, agency_identifier="123", main_account_code="0111")
    baker.make("accounts.FederalAccount", pk=2, agency_identifier="234", main_account_code="0222")
    baker.make("accounts.FederalAccount", pk=3, agency_identifier="345", main_account_code="0333")


def test_account_download_filter_cast_to_int(agency_models, federal_account_models):
    test_data = {"fy": "2018", "quarter": "4", "agency": "2", "federal_account": "3"}
    result = AccountDownloadFilter(**test_data)
    assert result.fy == 2018
    assert result.quarter == 4
    assert result.agency == 2
    assert result.federal_account == 3


def test_account_download_handle_all(agency_models, federal_account_models):
    test_data = {
        "fy": "2018",
        "quarter": "4",
        "agency": "all",
        "federal_account": "all",
        "budget_function": "all",
        "budget_subfunction": "all",
    }
    result = AccountDownloadFilter(**test_data)
    assert result.fy == 2018
    assert result.quarter == 4
    assert result.agency is None
    assert result.federal_account is None
    assert result.budget_function is None
    assert result.budget_subfunction is None


def test_account_download_both_period_quarter(agency_models, federal_account_models):
    test_data = {"fy": "2018", "period": "12", "quarter": "4"}
    with pytest.warns() as warnings:
        result = AccountDownloadFilter(**test_data)
    assert result.fy == 2018
    assert result.period == 12
    assert result.quarter is None
    assert len(warnings) == 1
    assert str(warnings[0].message) == "Both quarter and period are set.  Only using period."


def test_account_download_none_period_quarter(agency_models, federal_account_models):
    test_data = {"fy": "2018"}
    with pytest.raises(InvalidParameterException, match="Must define period or quarter."):
        AccountDownloadFilter(**test_data)


def test_account_download_no_agency(agency_models, federal_account_models):
    test_data = {"fy": "2018", "period": 2, "agency": 3}
    result = AccountDownloadFilter(**test_data)
    assert result.agency == 3
    test_data = {"fy": "2018", "period": 2, "agency": 4}
    with pytest.raises(InvalidParameterException, match="Agency with that ID does not exist"):
        AccountDownloadFilter(**test_data)


def test_account_download_no_federal_account(agency_models, federal_account_models):
    test_data = {"fy": "2018", "period": 2, "federal_account": 3}
    result = AccountDownloadFilter(**test_data)
    assert result.federal_account == 3
    test_data = {"fy": "2018", "period": 2, "federal_account": 4}
    with pytest.raises(InvalidParameterException, match="Federal Account with that ID does not exist"):
        result = AccountDownloadFilter(**test_data)
