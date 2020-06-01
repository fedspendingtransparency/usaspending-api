import pytest

from django.conf import settings
from django.core.management import call_command
from usaspending_api.references.models import DisasterEmergencyFundCode

test_data_directory = str(settings.APP_DIR / "data" / "testing_data" / "def_codes")
happy_path_file = f"{test_data_directory}/happy_path.csv"
invalid_code_file = f"{test_data_directory}/invalid_code.csv"
missing_code_file = f"{test_data_directory}/missing_code.csv"
missing_public_law_file = f"{test_data_directory}/missing_public_law.csv"


@pytest.fixture()
def happy_path_test_data():
    call_command("load_disaster_emergency_fund_codes", happy_path_file)


def invalid_code_test_data():
    call_command("load_disaster_emergency_fund_codes", invalid_code_file)


def missing_code_test_data():
    call_command("load_disaster_emergency_fund_codes", missing_code_file)


def missing_public_law_test_data():
    call_command("load_disaster_emergency_fund_codes", missing_public_law_file)


@pytest.mark.django_db(transaction=True)
def test_happy_path(happy_path_test_data):
    def_codes = list(DisasterEmergencyFundCode.objects.order_by("code").all())

    assert len(def_codes) == 3

    assert def_codes[0].code == "J"
    assert def_codes[0].public_law == "Wildfire Suppression PL 116-94"
    assert def_codes[0].title == "Further Consolidated Appropriations Act, 2020"
    assert def_codes[0].group_name is None

    assert def_codes[1].code == "K"
    assert def_codes[1].public_law == "Emergency PL 116-113"
    assert def_codes[1].title == "United States-Mexico-Canada Agreement Implementation Act"
    assert def_codes[1].group_name == "covid_19"

    assert def_codes[2].code == "Q"
    assert def_codes[2].public_law == "Excluded from tracking"
    assert def_codes[2].title is None
    assert def_codes[2].group_name is None


@pytest.mark.django_db(transaction=True)
def test_invalid_def_code():
    try:
        invalid_code_test_data()
    except RuntimeError as e:
        assert str(e) == "1 problem(s) have been found with the raw DEF Code file.  See log for details."
    else:
        assert False, "Expected a RuntimeError to occur."


@pytest.mark.django_db(transaction=True)
def test_missing_def_code():
    try:
        missing_code_test_data()
    except RuntimeError as e:
        assert str(e) == "1 problem(s) have been found with the raw DEF Code file.  See log for details."
    else:
        assert False, "Expected a RuntimeError to occur."


@pytest.mark.django_db(transaction=True)
def test_missing_public_law():
    try:
        missing_public_law_test_data()
    except RuntimeError as e:
        assert str(e) == "1 problem(s) have been found with the raw DEF Code file.  See log for details."
    else:
        assert False, "Expected a RuntimeError to occur."
