import os

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri


def build_file_description(source_file_template, sources):

    # Read in file description ignoring lines that start with #.
    with RetrieveFileFromUri(source_file_template).get_file_object() as f:
        file_description_text = "".join(tuple(line.decode("utf-8") for line in f if not line.startswith(b"#")))

    # Replace source.source_type keys with source.file_name values.
    return file_description_text.format(**{source.source_type: source.file_name for source in sources})


def save_file_description(working_dir, file_name, file_description):

    # Because this file will be read by Windows machines, it will need carriage
    # returns and line feeds.  /sigh.
    file_description = "\r\n".join(file_description.splitlines())

    # Build the destination path and create the file using the provided text.
    destination_file_path = os.path.join(working_dir, file_name)
    with open(destination_file_path, "w") as f:
        f.write(file_description)

    return destination_file_path
