import os

from collections import namedtuple
from tempfile import NamedTemporaryFile
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description


def test_save_file_description():

    with NamedTemporaryFile() as f:
        working_dir, file_name = os.path.split(f.name)
        file_path = save_file_description(working_dir, file_name, "this is a test")
        assert file_path == f.name
        assert f.read().decode("utf-8") == "this is a test"

    with NamedTemporaryFile() as f:
        working_dir, file_name = os.path.split(f.name)
        file_path = save_file_description(working_dir, file_name, "this\ris\na\r\ntest")
        assert file_path == f.name
        assert f.read().decode("utf-8") == "this\r\nis\r\na\r\ntest"


def test_build_file_description():

    with NamedTemporaryFile() as f:
        f.write(b"test")
        f.flush()
        fd = build_file_description(f.name, [])
        assert fd == "test"

    with NamedTemporaryFile() as f:
        f.write(b"\n".join([b"this", b"is", b"a", b"test"]))
        f.flush()
        fd = build_file_description(f.name, [])
        assert fd == "this\nis\na\ntest"

    with NamedTemporaryFile() as f:
        f.write(b"\n".join([b"this", b"is", b"#not", b"a", b"test"]))
        f.flush()
        fd = build_file_description(f.name, [])
        assert fd == "this\nis\na\ntest"

    with NamedTemporaryFile() as f:
        source = namedtuple("str", "source_type file_name")
        source.source_type = "replace_me"
        source.file_name = "also"

        f.write(b"\n".join([b"this", b"is", b"#not", b"{replace_me}", b"a", b"test"]))
        f.flush()
        fd = build_file_description(f.name, [source])
        assert fd == "this\nis\nalso\na\ntest"
