from django.conf import settings


def test_secret_key_length():
    assert settings.SECRET_KEY is not None, "SECRET_KEY should be set"
    assert len(settings.SECRET_KEY) >= 50, "SECRET_KEY should be at least 50 characters"
