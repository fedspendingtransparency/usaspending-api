import os
import zipfile

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.download.helpers import write_to_download_log as write_to_log


def build_file_description(source_file_template, sources):

    # Read in file description ignoring lines that start with #.
    with RetrieveFileFromUri(source_file_template).get_file_object() as f:
        file_description_text = "".join(tuple(l.decode("utf-8") for l in f if not l.startswith(b"#")))

    return file_description_text.format(**{source.source_type: source.file_name for source in sources})


def include_file_description_in_zip(file_description_text, destination_file_name, working_dir, zipfile_path):
    write_to_log(message="Adding file description")

    destination_file_path = os.path.join(working_dir, destination_file_name)
    with open(destination_file_path, "w") as f:
        f.write(file_description_text)

    zip_file = zipfile.ZipFile(zipfile_path, 'a', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
    zip_file.write(destination_file_path, destination_file_name)
