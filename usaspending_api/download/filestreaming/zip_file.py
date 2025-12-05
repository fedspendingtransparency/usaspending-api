import os
import zipfile
from ctypes import CDLL, POINTER, c_char_p, c_int

from usaspending_api.config.envs.default import _PROJECT_ROOT_DIR


def append_files_to_zip_file(file_paths, zip_file_path):
    """
    Create zip archive at the specified zip_file_path if it does not exist, and add all the files at provided
    file_paths to it.

    NOTE: If a zip file already exists at zip_file_path, the given files will be added in addition to the ones
    already in the zip when using append (`a`) mode. If that zip contains a file with the same name as one provided,
    it will throw a UserWarning and duplicate the file.
    Use caution in this case by removing the zip in the `finally` of an exception and also checking for and removing
    the zip if it exists before you begin to create it from scratch
    """
    with zipfile.ZipFile(zip_file_path, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip_file:
        for file_path in file_paths:
            archive_name = os.path.basename(file_path)
            zip_file.write(file_path, archive_name)


def append_files_to_zip_file_go(file_paths: list[str] | list[bytes], zip_file_path: str | bytes):
    """Create zip archive at the specified zip_file_path if it does not exist, and add all the files at provided
    file_paths to it. This uses a shared library written in Go and imported using the CDLL function.

    NOTE: If a zip file already exists at zip_file_path, the given files will be added in addition to the ones
    already in the zip when using append (`a`) mode. If that zip contains a file with the same name as one provided,
    it will throw a UserWarning and duplicate the file.
    Use caution in this case by removing the zip in the `finally` of an exception and also checking for and removing
    the zip if it exists before you begin to create it from scratch.

    Args:
        file_paths: List of file paths to zip
        zip_file_path: Path to zip file
    """

    # Import the shared library
    lib = CDLL(f"{_PROJECT_ROOT_DIR}/utilities/zipper.so")

    # Document the function's argument and return types
    lib.AppendFilesToZipFile.argtypes = [POINTER(c_char_p), c_int, c_char_p]
    lib.AppendFilesToZipFile.restype = c_int

    file_paths = [p.encode() if type(p) is str else p for p in file_paths]
    c_file_paths = (c_char_p * len(file_paths))(*file_paths)
    zip_file_path = zip_file_path.encode() if type(zip_file_path) is str else zip_file_path

    zip_result: int = lib.AppendFilesToZipFile(c_file_paths, len(c_file_paths), zip_file_path)

    if zip_result != 0:
        raise Exception(f"Zip file {zip_file_path} was not created")
