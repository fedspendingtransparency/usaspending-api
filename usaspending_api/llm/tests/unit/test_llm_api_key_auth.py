import pytest

from usaspending_api.llm.v2 import auth as auth_module
from usaspending_api.llm.v2.auth import SECRET_CACHE_TTL_SECONDS, SECRET_REFETCH_FLOOR_SECONDS, LLMApiKeyAuth

GOOD_KEY = "11111111-2222-3333-4444-555555555555"
ROTATED_KEY = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


class FakeSecretsManager:
    """Stands in for Secrets Manager. Counts fetches and lets a test change the stored value."""

    def __init__(self, value):
        self.value = value
        self.fetches = 0

    async def fetch(self):
        self.fetches += 1
        return self.value


@pytest.fixture
def clock(monkeypatch):
    state = {"now": 1000.0}
    monkeypatch.setattr(auth_module.time, "monotonic", lambda: state["now"])
    return state


@pytest.fixture
def secrets(monkeypatch):
    LLMApiKeyAuth.reset_cache()
    fake = FakeSecretsManager(GOOD_KEY)
    monkeypatch.setattr(LLMApiKeyAuth, "_fetch_secret_from_aws", staticmethod(fake.fetch))
    yield fake
    LLMApiKeyAuth.reset_cache()


@pytest.mark.asyncio
async def test_valid_key_fetches_the_secret_once(clock, secrets):
    auth = LLMApiKeyAuth()
    for _ in range(50):
        assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY
    assert secrets.fetches == 1


@pytest.mark.asyncio
async def test_surrounding_whitespace_is_still_tolerated(clock, secrets):
    auth = LLMApiKeyAuth()
    assert await auth.authenticate(None, f"  {GOOD_KEY}\n") == f"  {GOOD_KEY}\n"


@pytest.mark.asyncio
async def test_empty_key_never_touches_aws(clock, secrets):
    auth = LLMApiKeyAuth()
    assert await auth.authenticate(None, "") is None
    assert secrets.fetches == 0


@pytest.mark.asyncio
async def test_bad_keys_cannot_force_repeated_fetches(clock, secrets):
    auth = LLMApiKeyAuth()
    assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY
    assert secrets.fetches == 1

    # Within the refetch floor, bad keys are answered from the cache only.
    for _ in range(100):
        assert await auth.authenticate(None, "not-the-key") is None
    assert secrets.fetches == 1

    # Once the floor has passed, a mismatch is allowed exactly one refetch.
    clock["now"] += SECRET_REFETCH_FLOOR_SECONDS
    for _ in range(100):
        assert await auth.authenticate(None, "not-the-key") is None
    assert secrets.fetches == 2


@pytest.mark.asyncio
async def test_rotated_secret_is_picked_up_after_the_floor(clock, secrets):
    auth = LLMApiKeyAuth()
    assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY

    secrets.value = ROTATED_KEY
    clock["now"] += SECRET_REFETCH_FLOOR_SECONDS
    assert await auth.authenticate(None, ROTATED_KEY) == ROTATED_KEY
    assert await auth.authenticate(None, GOOD_KEY) is None


@pytest.mark.asyncio
async def test_cache_expires_after_ttl(clock, secrets):
    auth = LLMApiKeyAuth()
    assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY
    clock["now"] += SECRET_CACHE_TTL_SECONDS
    assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY
    assert secrets.fetches == 2


@pytest.mark.asyncio
async def test_failed_refresh_keeps_serving_the_last_good_secret(clock, secrets):
    auth = LLMApiKeyAuth()
    assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY

    secrets.value = None  # Secrets Manager is unreachable for a while
    clock["now"] += SECRET_CACHE_TTL_SECONDS
    assert await auth.authenticate(None, GOOD_KEY) == GOOD_KEY
    assert secrets.fetches == 2


@pytest.mark.asyncio
async def test_cold_cache_with_aws_down_fails_closed_and_backs_off(clock, secrets):
    secrets.value = None
    auth = LLMApiKeyAuth()
    for _ in range(100):
        assert await auth.authenticate(None, GOOD_KEY) is None
    assert secrets.fetches == 1
