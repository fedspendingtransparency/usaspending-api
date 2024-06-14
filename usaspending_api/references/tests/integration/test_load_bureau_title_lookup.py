import pytest

from django.core.management import call_command
from django.db.utils import IntegrityError

from usaspending_api import settings
from usaspending_api.references.models.bureau_title_lookup import BureauTitleLookup


@pytest.mark.django_db
def test_happy_path():

    test_data_path = str(settings.APP_DIR / "data" / "testing_data" / "FMS_GWA_EXPORT_APPN.csv")
    call_command("load_bureau_title_lookup", path=test_data_path)

    # There is a duplicate record and two bad records
    assert BureauTitleLookup.objects.count() == 4

    lookup_1 = BureauTitleLookup.objects.filter(federal_account_code="000-0100").first()
    assert lookup_1.bureau_title == "Senate"
    assert lookup_1.bureau_slug == "senate"

    lookup_2 = BureauTitleLookup.objects.filter(federal_account_code="044-0170").first()
    assert lookup_2.bureau_title == "Architect of the Capitol"
    assert lookup_2.bureau_slug == "architect-of-the-capitol"

    # Ensure that CSV row with "DUMMY" type is not loaded
    lookup_3_count = BureauTitleLookup.objects.filter(federal_account_code="111-1111").count()
    assert lookup_3_count == 0

    # Ensure that CSV row with "999" aid is not loaded
    lookup_4_count = BureauTitleLookup.objects.filter(federal_account_code="999-9999").count()
    assert lookup_4_count == 0


@pytest.mark.django_db
def test_failed_insert():

    # Correctly insert data
    test_data_path = str(settings.APP_DIR / "data" / "testing_data" / "FMS_GWA_EXPORT_APPN.csv")
    call_command("load_bureau_title_lookup", path=test_data_path)

    # Attempt to insert data that will cause insert failure (duplicate key error)
    bad_data_path = str(settings.APP_DIR / "data" / "testing_data" / "FMS_GWA_EXPORT_APPN_DUPLICATE_KEY.csv")
    with pytest.raises(IntegrityError):
        call_command("load_bureau_title_lookup", path=bad_data_path)

    # Ensure previously loaded data still exists
    assert BureauTitleLookup.objects.count() == 4
