import os
import subprocess
import zipfile
from pathlib import Path
from subprocess import CompletedProcess


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


def append_files_to_zip_file_go(file_paths: list[str] | list[bytes], zip_file_path: str | bytes | Path):
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

    print("Using Go to create zip")

    go_executable_path = f"{Path(__file__).parent.parent}/utilities/zipper_v2.so"
    subprocess_output: CompletedProcess = subprocess.run(
        args=[go_executable_path, "-out", zip_file_path, *file_paths],
        capture_output=True,
    )

    if subprocess_output.returncode != 0:
        print(f"Error while calling the Go executable: {subprocess_output.stderr}")
    else:
        print(f"Successfully created zip file: {zip_file_path}")

    # # Import the shared library
    # library_path = f"{Path(__file__).parent.parent}/utilities/zipper.so"
    # if library_path != "/databricks/python/lib/python3.10/site-packages/usaspending_api/download/utilities/zipper.so":
    #     print("Path not correct")
    #     library_path = "/databricks/python/lib/python3.10/site-packages/usaspending_api/download/utilities/zipper.so"
    # else:
    #     print("Path is correct!")
    #
    # lib = CDLL(library_path)
    #
    # # Document the function's argument and return types
    # lib.AppendFilesToZipFile.argtypes = [POINTER(c_char_p), c_int, c_char_p]
    # lib.AppendFilesToZipFile.restype = c_int
    #
    # # Ensure we're using the correct datatypes that the shared library expects
    # file_paths = [p.encode() if type(p) is str else p for p in file_paths]
    # c_file_paths = (c_char_p * len(file_paths))(*file_paths)
    #
    # match zip_file_path:
    #     case Path() | str():
    #         zip_file_path = str(zip_file_path).encode()
    #     case bytes():
    #         ...
    #     case _:
    #         raise TypeError(f"zip_file_path must be bytes, string, or path. Got {type(zip_file_path).__name__}")
    #
    # zip_result: int = lib.AppendFilesToZipFile(c_file_paths, len(c_file_paths), zip_file_path)
    #
    # if zip_result != 0:
    #     raise Exception(f"Zip file {zip_file_path} was not created")
