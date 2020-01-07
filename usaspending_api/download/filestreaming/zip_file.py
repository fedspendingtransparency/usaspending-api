import os
import zipfile


def append_files_to_zip_file(file_paths, zip_file_path):
    """
    Create zip archive at the specified zip_file_path if it does not exist, and add all the files at provided
    file_paths to it.

    NOTE: If a zip file already exists at zip_file_path, the given files will be added in addition to the ones
    already in the zip when using append (`a`) mode. If that zip contains a file with the same name as one provided,
    it will throw a UserWarning and duplicate the file.
    """
    with zipfile.ZipFile(zip_file_path, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip_file:
        for file_path in file_paths:
            archive_name = os.path.basename(file_path)
            zip_file.write(file_path, archive_name)


def write_files_to_zip_file(file_paths, zip_file_path):
    """(Re)create a zip archive at the specified zip_file_path, and add all the files at provided file_paths to it.

    NOTE: If a (zip) file already exists at zip_file_path, it will be overwritten in write (`w`) mode.
    """
    with zipfile.ZipFile(zip_file_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip_file:
        for file_path in file_paths:
            archive_name = os.path.basename(file_path)
            zip_file.write(file_path, archive_name)
