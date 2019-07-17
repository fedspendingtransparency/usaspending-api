import os
import zipfile

from tempfile import NamedTemporaryFile
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file


def test_append_files_to_zip_file():
    with NamedTemporaryFile() as zip_file:
        with NamedTemporaryFile() as include_file_1:
            with NamedTemporaryFile() as include_file_2:
                include_file_1.write(b"this is a test")
                include_file_1.flush()
                include_file_2.write(b"this is also a test")
                include_file_2.flush()
                append_files_to_zip_file([include_file_1.name, include_file_2.name], zip_file.name)

                with zipfile.ZipFile(zip_file.name, "r") as zf:
                    assert [z.filename for z in zf.filelist] == [
                        os.path.basename(include_file_1.name),
                        os.path.basename(include_file_2.name),
                    ]
