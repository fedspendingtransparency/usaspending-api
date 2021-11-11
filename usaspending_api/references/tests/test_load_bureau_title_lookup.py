from django.core.management import call_command

from usaspending_api import settings
from usaspending_api.references.models.bureau_title_lookup import BureauTitleLookup


def test_load_bureau_title_lookup(db):

    test_data_path = str(settings.APP_DIR / "data" / "testing_data" / "FMS_GWA_EXPORT_APPN.csv")
    call_command("load_bureau_title_lookup", path=test_data_path)

    # There are 2 duplicate records. Included 2, to ensure one row isn't read as a header
    assert BureauTitleLookup.objects.count() == 4

    lookup_1 = BureauTitleLookup.objects.filter(federal_account_code="000-0100").first()
    assert lookup_1.bureau_title == "Senate"
    assert lookup_1.bureau_slug == "senate"

    lookup_2 = BureauTitleLookup.objects.filter(federal_account_code="044-0170").first()
    assert lookup_2.bureau_title == "Architect of the Capitol"
    assert lookup_2.bureau_slug == "architect-of-the-capitol"
