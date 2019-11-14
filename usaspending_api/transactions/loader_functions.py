from re import search as regex
from pathlib import Path
from typing import List, Tuple

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri


def store_ids_in_file(id_iter: List, file_name: str = "temp_file") -> Tuple[Path, int]:
    """Store the provided values in a file
        returning filepath and count of ids
    """
    total_ids = 0
    file_path = Path(file_name)

    with open(str(file_path), "w") as f:
        for id_string in id_iter:
            if not id_string:
                continue
            total_ids += 1
            int_characters = regex(r"\d+", str(id_string)).group()
            f.writelines("{}\n".format(int_characters))

    return file_path.resolve(), total_ids


def filepath_command_line_argument_type(chunk_count):
    """helper function for parsing files provided by the user"""
    def _filepath_command_line_argument_type(provided_uri):
        return read_file_for_database_ids(provided_uri, chunk_count)

    return _filepath_command_line_argument_type


def read_file_for_database_ids(provided_uri, chunk_count):
    """wrapped generator to read file and stream IDs"""
    try:
        with RetrieveFileFromUri(provided_uri).get_file_object() as file:
            chunk_list = []
            for line in file.readlines():
                chunk_list.append(int(line.decode("utf-8")))
                if len(chunk_list) >= chunk_count:
                    yield chunk_list
                    chunk_list = []

            yield chunk_list

    except Exception as e:
        raise RuntimeError("Issue with reading/parsing file: {}".format(e))
