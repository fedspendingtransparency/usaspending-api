from usaspending_api.references.models import Definition
from usaspending_api.references.management.commands.load_guide import load_guide, Command
import pytest
from django.db.utils import IntegrityError


def test_guide_load(db):
    """
    Ensure definition guide data can can be loaded from source file
    """

    Definition.objects.all().delete()

    assert Definition.objects.count() == 0
    load_guide(path=Command.default_path, append=False)
    rows = Definition.objects.count()

    # Verify that loaded definitions have a mix of nulls and non-nulls in nullable fields
    assert rows > 0
    assert 0 < Definition.objects.filter(resources__isnull=True).count() < rows
    assert 0 < Definition.objects.filter(official__isnull=True).count() < rows

    # Re-loading with append=False should succeed and load the same rows
    load_guide(path=Command.default_path, append=False)
    assert Definition.objects.count() == rows

    # Trying to append the same rows should throw integrity errors
    with pytest.raises(IntegrityError):
        load_guide(path=Command.default_path, append=True)
