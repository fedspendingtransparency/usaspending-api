import pytest

from django.db.utils import IntegrityError

from usaspending_api import settings
from usaspending_api.references.models import Definition
from usaspending_api.references.management.commands.load_glossary import load_glossary


@pytest.mark.django_db
def test_glossary_load():
    """
    Ensure definition guide data can can be loaded from source file
    """
    test_data_path = str(settings.APP_DIR / "data" / "testing_data" / "USAspendingGlossary.xlsx")
    Definition.objects.all().delete()

    assert Definition.objects.count() == 0
    load_glossary(path=test_data_path, append=False)
    rows = Definition.objects.count()

    # Verify that loaded definitions have a mix of nulls and non-nulls in nullable fields
    assert rows == 2
    assert 0 < Definition.objects.filter(resources__isnull=True).count() < rows

    # Re-loading with append=False should succeed and load the same rows
    load_glossary(path=test_data_path, append=False)
    assert Definition.objects.count() == rows

    # Trying to append the same rows should throw integrity errors
    with pytest.raises(IntegrityError):
        load_glossary(path=test_data_path, append=True)
