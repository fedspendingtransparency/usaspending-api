import pytest

from usaspending_api.common.sqs.sqs_handler import (
    get_sqs_queue,
    UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME,
    UNITTEST_FAKE_QUEUE_NAME,
    FakeSQSMessage,
    _FakeStatelessLoggingSQSDeadLetterQueue,
)
from usaspending_api.conftest_helpers import get_unittest_fake_sqs_queue


@pytest.fixture()
def _patch_get_sqs_queue(fake_sqs_queue, monkeypatch):
    """
    Patch the use of get_sqs_queue HERE, in tests in this module (defined by ``__name__``), where the function is
    imported as a new name in this module.

    Chaining to fixture ``fake_sqs_queue`` will also take care of fake-queue purging and cleanup.
    """
    monkeypatch.setattr(f"{__name__}.get_sqs_queue", get_unittest_fake_sqs_queue)
    should_be_fake = get_sqs_queue()
    # Check that the patch worked, and the fake unit test queue is returned
    assert should_be_fake.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME


# Autouse the specified fixture(s) for each test in this module
pytestmark = pytest.mark.usefixtures("_patch_get_sqs_queue")


def test_unittest_gets_fake_queue():
    q = get_sqs_queue()
    assert q.url.split("/")[-1] == UNITTEST_FAKE_QUEUE_NAME


def test_getting_fake_dead_letter_queue():
    q = get_sqs_queue(queue_name=UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME)
    assert q.url == _FakeStatelessLoggingSQSDeadLetterQueue.url


def test_fake_send_message():
    assert len(get_sqs_queue()._messages()) == 0
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    assert len(get_sqs_queue()._messages()) == 1


def test_enqueued_fake_message_type():
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    msg = get_sqs_queue()._messages().pop()
    assert isinstance(msg, FakeSQSMessage)
    assert msg.body == "1235"
    assert msg.message_attributes.get("attr2") == 111


def test_fake_receive_message():
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    q.send_message("2222")
    q.send_message("3333")
    q.send_message("4444", {"attr1": "v1", "attr2": "v2"})
    msgs = q.receive_messages(10)
    assert len(msgs) == 1
    assert msgs[0].body == "1235"
    assert msgs[0].message_attributes.get("attr2") == 111


def test_fake_receive_messages():
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    q.send_message("2222")
    q.send_message("3333")
    q.send_message("4444", {"attr1": "v1", "attr2": "v2"})
    msgs = q.receive_messages(10, MaxNumberOfMessages=10)
    assert len(msgs) == 4
    assert msgs[0].body == "1235"
    assert msgs[0].message_attributes.get("attr2") == 111
    assert msgs[3].body == "4444"
    assert msgs[3].message_attributes.get("attr2") == "v2"
    q.purge()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    q.send_message("2222")
    q.send_message("3333")
    q.send_message("4444", {"attr1": "v1", "attr2": "v2"})
    msgs = q.receive_messages(10, MaxNumberOfMessages=2)
    assert len(msgs) == 2
    assert msgs[0].body == "1235"
    assert msgs[0].message_attributes.get("attr2") == 111
    assert msgs[1].body == "2222"


# TODO - FAILING HERE: "static import" of FAKE_QUEUE gets the unmocked/unpatched value (before it can be patched by
# the fixture), so it refers to the local queue.
# could just force the call of get_sqs_queue everywhere and remove the FAKE_QUEUE value. But this then ruins any way
# to refer back to the "Singleton" queue instance (same ref of obj in memory) during a process run.
# Do we still need singleton references considering:
#   - the local queue will always refer to the same file?
#   - the unit test queue is now created and maintained per session
# PROBABLY NOT: The only place FAKE_QUEUE is being referenced is from unit tests and that will return the consistent
# config'd patched/test object
def test_delete_fake_received_message():
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    q.send_message("2222")
    q.send_message("3333")
    q.send_message("4444", {"attr1": "v1", "attr2": "v2"})
    assert len(get_sqs_queue()._messages()) == 4
    bodies = [m.body for m in get_sqs_queue()._messages()]
    assert "1235" in bodies
    msgs = q.receive_messages(10)
    assert len(msgs) == 1
    assert len(get_sqs_queue()._messages()) == 4
    msg = msgs[0]
    assert msg.body == "1235"
    assert msg.message_attributes.get("attr2") == 111
    msg.delete()
    assert len(get_sqs_queue()._messages()) == 3
    bodies = [m.body for m in get_sqs_queue()._messages()]
    assert "1235" not in bodies
