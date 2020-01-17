from usaspending_api.references import helpers


def test_canonicalize_string():
    assert helpers.canonicalize_string(" Däytön\n") == "DÄYTÖN"
