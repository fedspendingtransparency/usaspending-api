from re import search as regex
from pathlib import Path
from typing import List, Tuple

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri


def store_ids_in_file(id_iter: List, file_name: str = "temp_file", is_numeric: bool = True) -> Tuple[Path, int]:
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
            if is_numeric:
                id_characters = regex(r"\d+", str(id_string)).group()
            elif isinstance(id_string, list):
                id_characters = "\n".join([str(id) for id in id_string])
                total_ids += len(id_string) - 1  # count all ids in the list, minus the 1 already added above
            else:
                id_characters = id_string
            f.writelines("{}\n".format(id_characters))

    return file_path.resolve(), total_ids


def filepath_command_line_argument_type(chunk_count):
    """helper function for parsing files provided by the user"""

    def _filepath_command_line_argument_type(provided_uri):
        return read_file_for_database_ids(provided_uri, chunk_count, is_numeric=False)

    return _filepath_command_line_argument_type


def read_file_for_database_ids(provided_uri, chunk_count, is_numeric: bool = True):
    """wrapped generator to read file and stream IDs"""

    use_binary = not is_numeric
    try:
        with RetrieveFileFromUri(provided_uri).get_file_object(text=use_binary) as file:
            chunk_list = []
            while True:
                line = file.readline()
                if not line:
                    break
                elif is_numeric:
                    chunk_list.append(int(line))
                else:
                    chunk_list.append(line.strip())

                if len(chunk_list) >= chunk_count:
                    yield chunk_list
                    chunk_list = []

            yield chunk_list

    except Exception as e:
        raise RuntimeError(f"Issue with reading/parsing file: {e}")
