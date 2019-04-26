import os
import zipfile


def append_files_to_zip_file(file_paths, zip_file_path):

    # Append all of the provided files to the specified zip file.
    with zipfile.ZipFile(zip_file_path, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip_file:
        for file_path in file_paths:
            archive_name = os.path.basename(file_path)
            zip_file.write(file_path, archive_name)
