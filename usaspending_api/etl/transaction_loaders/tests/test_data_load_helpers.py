from usaspending_api.etl.transaction_loaders.data_load_helpers import capitalize_and_compress_if_string, false_if_null


def test_capitalize_if_string():
    assert capitalize_and_compress_if_string("bob4") == "BOB4"
    assert capitalize_and_compress_if_string(7) == 7
    assert capitalize_and_compress_if_string(None) is None


def test_compress_string():
    assert capitalize_and_compress_if_string("bob") == "BOB"
    assert capitalize_and_compress_if_string("bob bob") == "BOB BOB"
    assert capitalize_and_compress_if_string("bob  bob bob") == "BOB BOB BOB"


def test_false_if_null():
    assert false_if_null("true") == "true"
    assert false_if_null("false") == "false"
    assert false_if_null("n") == "n"
    assert false_if_null(True)
    assert not false_if_null(False)
    assert not false_if_null(None)
