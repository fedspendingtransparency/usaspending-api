from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names


def test_slugify_text_for_file_names():
    assert slugify_text_for_file_names("test") == "test"
    assert slugify_text_for_file_names("test", "default") == "test"
    assert slugify_text_for_file_names("test", "default", 50) == "test"
    assert slugify_text_for_file_names("test", "default", 2) == "te"

    assert slugify_text_for_file_names(None) is None
    assert slugify_text_for_file_names(None, None) is None
    assert slugify_text_for_file_names(None, "default") == "default"
    assert slugify_text_for_file_names(None, "default", 50) == "default"
    assert slugify_text_for_file_names(None, "default", 2) == "default"

    assert slugify_text_for_file_names("---") is None
    assert slugify_text_for_file_names("---", "default") == "default"
    assert slugify_text_for_file_names("---", "default", 50) == "default"
    assert slugify_text_for_file_names("---", "default", 2) == "default"

    garbage = r")*(&*()THIS(#@$@^*IS<,>.?/:;\"'{[}]|\+=_-)(*&^%$#@!A><?>\":}TEST()(*&(*&*(*"
    assert slugify_text_for_file_names(garbage) == "THIS_IS_A_TEST"
    assert slugify_text_for_file_names(garbage, "default") == "THIS_IS_A_TEST"
    assert slugify_text_for_file_names(garbage, "default", 50) == "THIS_IS_A_TEST"
    assert slugify_text_for_file_names(garbage, "default", 2) == "TH"

    assert slugify_text_for_file_names("áéíóúüñ") == "aeiouun"
    assert slugify_text_for_file_names("áéíóúüñ", "default") == "aeiouun"
    assert slugify_text_for_file_names("áéíóúüñ", "default", 50) == "aeiouun"
    assert slugify_text_for_file_names("áéíóúüñ", "default", 2) == "ae"

    assert slugify_text_for_file_names("buenos días") == "buenos_dias"
    assert slugify_text_for_file_names("buenos días", "default") == "buenos_dias"
    assert slugify_text_for_file_names("buenos días", "default", 50) == "buenos_dias"
    assert slugify_text_for_file_names("buenos días", "default", 2) == "bu"

    # These all contain no ascii compatible characters.
    assert slugify_text_for_file_names("Καλημέρα") is None
    assert slugify_text_for_file_names("Καλημέρα", "default") == "default"
    assert slugify_text_for_file_names("Καλημέρα", "default", 50) == "default"
    assert slugify_text_for_file_names("Καλημέρα", "default", 2) == "default"

    assert slugify_text_for_file_names("Доброе утро") is None
    assert slugify_text_for_file_names("Доброе утро", "default") == "default"
    assert slugify_text_for_file_names("Доброе утро", "default", 50) == "default"
    assert slugify_text_for_file_names("Доброе утро", "default", 2) == "default"

    assert slugify_text_for_file_names("早上好/ 早") is None
    assert slugify_text_for_file_names("早上好/ 早", "default") == "default"
    assert slugify_text_for_file_names("早上好/ 早", "default", 50) == "default"
    assert slugify_text_for_file_names("早上好/ 早", "default", 2) == "default"
