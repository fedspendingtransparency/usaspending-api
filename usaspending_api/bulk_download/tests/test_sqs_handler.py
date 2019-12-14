import pytest

from usaspending_api.bulk_download.sqs_handler import (
    get_sqs_queue,
    FAKE_QUEUE,
    FAKE_DEAD_LETTER_QUEUE,
    FakeSQSMessage,
)


@pytest.fixture
def purged_queue(local_queue_dir):
    FAKE_QUEUE.purge()


pytestmark = pytest.mark.usefixtures("purged_queue")  # run fixture before every test function in this module


def test_unittest_gets_fake_queue():
    q = get_sqs_queue()
    assert q.queue_url == FAKE_QUEUE.FAKE_QUEUE_URL


def test_getting_fake_dead_letter_queue():
    q = get_sqs_queue(queue_name=FAKE_DEAD_LETTER_QUEUE.UNITTEST_FAKE_DEAD_LETTER_QUEUE)
    assert q.queue_url == FAKE_DEAD_LETTER_QUEUE.FAKE_DEAD_LETTER_QUEUE_URL


def test_fake_send_message():
    assert len(FAKE_QUEUE._messages()) == 0
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    assert len(FAKE_QUEUE._messages()) == 1


def test_enqueued_fake_message_type():
    get_sqs_queue().send_message("1235", {"attr1": "val1", "attr2": 111})
    msg = FAKE_QUEUE._messages().pop()
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


def test_delete_fake_received_message():
    q = get_sqs_queue()
    q.send_message("1235", {"attr1": "val1", "attr2": 111})
    q.send_message("2222")
    q.send_message("3333")
    q.send_message("4444", {"attr1": "v1", "attr2": "v2"})
    assert len(FAKE_QUEUE._messages()) == 4
    bodies = [m.body for m in FAKE_QUEUE._messages()]
    assert "1235" in bodies
    msgs = q.receive_messages(10)
    assert len(msgs) == 1
    assert len(FAKE_QUEUE._messages()) == 4
    msg = msgs[0]
    assert msg.body == "1235"
    assert msg.message_attributes.get("attr2") == 111
    msg.delete()
    assert len(FAKE_QUEUE._messages()) == 3
    bodies = [m.body for m in FAKE_QUEUE._messages()]
    assert "1235" not in bodies
