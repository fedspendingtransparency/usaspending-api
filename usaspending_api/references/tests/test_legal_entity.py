import pytest

from usaspending_api.references.models import LegalEntity


@pytest.mark.django_db
def test_get_or_create_by_duns():
    # Null duns should always return True for created
    assert LegalEntity.get_or_create_by_duns(duns=None)[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns=None)[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns=None)[1]  # Assert created is True

    assert LegalEntity.get_or_create_by_duns(duns="")[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns="")[1]  # Assert created is True
    assert LegalEntity.get_or_create_by_duns(duns="")[1]  # Assert created is True

    # Let's create a new record with a DUNS
    entity, created = LegalEntity.get_or_create_by_duns(duns="123456789")
    assert created
    assert LegalEntity.get_or_create_by_duns(duns="123456789")[0] == entity
    assert not LegalEntity.get_or_create_by_duns(duns="123456789")[1]
