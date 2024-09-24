import csv
import pytest

from django.core.management import call_command
from usaspending_api.references.models import ObjectClass
from usaspending_api import settings


# This is created as needed.  It is not a static file.

OBJECT_CLASS_FILE = settings.APP_DIR / "references" / "tests" / "data" / "test_object_classes.csv"

# Several data samples.
GOOD_SAMPLE = [("100", "Test 100"), ("1000", "Test 1000")]
IGNORE_BLANK_SAMPLE = [("100", "Test 100"), ("1000", "Test 1000"), ("", "")]
ADDITIONAL_SAMPLE = [("200", "Test 200"), ("2000", "Test 2000")]
UPDATE_SAMPLE = [("100", "Test 100 update"), ("1000", "Test 1000 update")]
NON_DIGIT_SAMPLE = [("x00", "Not digits")]
TOO_MANY_DIGITS_SAMPLE = [("11100", "too many digits")]
MISSING_NAME_SAMPLE = [("1100", "")]
LEADING_TRAILING_SPACES_SAMPLE = [(" 100 ", " Test 100 "), (" 1000 ", " Test 1000 ")]


@pytest.fixture
def disable_vacuuming(monkeypatch):
    """
    We cannot run vacuums in a transaction.  Since tests are run in a transaction, we'll NOOP the
    function that performs the vacuuming.
    """
    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_object_classes.Command._vacuum_tables", lambda a: None
    )


@pytest.fixture(scope="session")
def remove_csv_file():
    """Ensure the CSV file goes away at the end of the tests."""
    yield
    try:
        OBJECT_CLASS_FILE.unlink()
    except FileNotFoundError:
        pass  # it's ok if the file to be deleted is not there


def mock_data(object_classes):
    """[(object class, object class name), ...]"""
    with open(OBJECT_CLASS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([("MAX OC Code", "MAX Object Class name")] + object_classes)


@pytest.mark.django_db(transaction=True)
def test_happy_path(remove_csv_file):
    """
    We're running this one without a transaction just to ensure the vacuuming doesn't blow up.  For
    the remaining tests we'll run inside of a transaction since it's faster.
    """

    # Confirm everything is empty.
    assert ObjectClass.objects.count() == 0

    # Load all the things.
    mock_data(GOOD_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))

    # We should have 6 records; the three we added plus the three default "000" object classes.
    assert ObjectClass.objects.count() == 6

    # Perform two deep dives; one for the records we just added and one for a "000" object class.
    oc = ObjectClass.objects.get(object_class="10.0", direct_reimbursable="D")
    assert oc.major_object_class == "10"
    assert oc.major_object_class_name == "Personnel compensation and benefits"
    assert oc.object_class == "10.0"
    assert oc.object_class_name == "Test 100"
    assert oc.direct_reimbursable == "D"
    assert oc.direct_reimbursable_name == "Direct"
    assert oc.create_date is not None
    assert oc.update_date is not None

    oc = ObjectClass.objects.get(object_class="00.0", direct_reimbursable=None)
    assert oc.major_object_class == "00"
    assert oc.major_object_class_name == "Unknown"
    assert oc.object_class == "00.0"
    assert oc.object_class_name == "Unknown"
    assert oc.direct_reimbursable is None
    assert oc.direct_reimbursable_name is None
    assert oc.create_date is not None
    assert oc.update_date is not None

    # These will blow up if a record is missing.
    ObjectClass.objects.get(object_class="00.0", direct_reimbursable=None)
    ObjectClass.objects.get(object_class="00.0", direct_reimbursable="D")
    ObjectClass.objects.get(object_class="00.0", direct_reimbursable="R")
    ObjectClass.objects.get(object_class="10.0", direct_reimbursable=None)
    ObjectClass.objects.get(object_class="10.0", direct_reimbursable="D")
    ObjectClass.objects.get(object_class="10.0", direct_reimbursable="R")


@pytest.mark.django_db
def test_ignore_blanks(disable_vacuuming, remove_csv_file):

    assert ObjectClass.objects.count() == 0
    mock_data(IGNORE_BLANK_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))
    assert ObjectClass.objects.count() == 6


@pytest.mark.django_db
def test_adding_rows(disable_vacuuming, remove_csv_file):

    assert ObjectClass.objects.count() == 0
    mock_data(GOOD_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))
    assert ObjectClass.objects.count() == 6
    mock_data(ADDITIONAL_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))
    assert ObjectClass.objects.count() == 9


@pytest.mark.django_db
def test_updating_rows(disable_vacuuming, remove_csv_file):

    assert ObjectClass.objects.count() == 0
    mock_data(GOOD_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))
    assert ObjectClass.objects.count() == 6
    mock_data(UPDATE_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))
    assert ObjectClass.objects.count() == 6

    oc = ObjectClass.objects.get(object_class="10.0", direct_reimbursable="D")
    assert oc.object_class_name == "Test 100 update"


@pytest.mark.django_db
def test_leading_trailing_spaces(disable_vacuuming, remove_csv_file):

    assert ObjectClass.objects.count() == 0
    mock_data(LEADING_TRAILING_SPACES_SAMPLE)
    call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))
    assert ObjectClass.objects.get(object_class="10.0", direct_reimbursable=None).object_class_name == "Test 100"
    assert ObjectClass.objects.get(object_class="10.0", direct_reimbursable="D").object_class_name == "Test 100"
    assert ObjectClass.objects.get(object_class="10.0", direct_reimbursable="R").object_class_name == "Test 100"


@pytest.mark.django_db
def test_bad_data(remove_csv_file):

    assert ObjectClass.objects.count() == 0

    mock_data(NON_DIGIT_SAMPLE)
    with pytest.raises(RuntimeError):
        call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))

    mock_data(TOO_MANY_DIGITS_SAMPLE)
    with pytest.raises(RuntimeError):
        call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))

    mock_data(MISSING_NAME_SAMPLE)
    with pytest.raises(RuntimeError):
        call_command("load_object_classes", object_class_file=str(OBJECT_CLASS_FILE))

    assert ObjectClass.objects.count() == 0
