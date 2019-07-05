import pytest

from django.core.management import call_command
from usaspending_api.references.models import CGAC
from usaspending_api.references.management.commands.load_cgacs import Command, MAX_DIFF


new_cgacs = None


@pytest.fixture
def agency_file_fixture(monkeypatch, transactional_db):
    """
    So we don't have to repeatedly read the agency file over the web, cache
    it after the first read.  The _get_new_cgacs code still gets tested, but
    only once.
    """
    original_get_new_cgacs = Command._get_new_cgacs

    def _get_new_cgacs(*args, **kwargs):
        global new_cgacs
        if new_cgacs is None:
            new_cgacs = original_get_new_cgacs()
        return new_cgacs

    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_cgacs.Command._get_new_cgacs",
        _get_new_cgacs
    )


def test_load_cgacs(agency_file_fixture):

    global new_cgacs

    # Confirm the CGACs table is empty.
    assert CGAC.objects.all().count() == 0

    # This should load everything since the table is empty.
    call_command("load_cgacs")
    the_count = CGAC.objects.all().count()
    assert the_count > 0

    # This should do nothing.
    call_command("load_cgacs")
    assert CGAC.objects.all().count() == the_count

    # Deleting a record should cause a reload.
    CGAC.objects.first().delete()
    assert CGAC.objects.all().count() < the_count
    call_command("load_cgacs")
    assert CGAC.objects.all().count() == the_count

    # The --force switch should cause a reload.
    call_command("load_cgacs", "--force")
    assert CGAC.objects.all().count() == the_count

    # Changing a value should cause a reload.
    new_aa = "this is a test... this is only a test"
    o = CGAC.objects.first()
    old_aa = o.agency_abbreviation
    o.agency_abbreviation = new_aa
    o.save()
    assert CGAC.objects.filter(cgac_code=o.cgac_code).first().agency_abbreviation == new_aa
    call_command("load_cgacs")
    assert CGAC.objects.all().count() == the_count
    assert CGAC.objects.filter(cgac_code=o.cgac_code).first().agency_abbreviation == old_aa

    # Deleting too many values should throw an error.
    for n in range(MAX_DIFF + 1):
        CGAC.objects.first().delete()
    assert CGAC.objects.all().count() < the_count
    with pytest.raises(RuntimeError):
        call_command("load_cgacs")

    # An empty CGAC file should cause an error.
    new_cgacs = []
    with pytest.raises(RuntimeError):
        call_command("load_cgacs")
