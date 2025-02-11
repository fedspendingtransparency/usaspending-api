import os
import time
import zipfile

from django.db import connection


def append_files_to_zip_file(file_paths, zip_file_path):
    """
    Create zip archive at the specified zip_file_path if it does not exist, and add all the files at provided
    file_paths to it.

    NOTE: If a zip file already exists at zip_file_path, the given files will be added in addition to the ones
    already in the zip when using append (`a`) mode. If that zip contains a file with the same name as one provided,
    it will throw a UserWarning and duplicate the file.
    Use caution in this case by removing the zip in the finally of an exception and also checking for and removing
    the zip if it exists before you begin to create it from scratch
    """
    start = time.time()
    with zipfile.ZipFile(zip_file_path, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip_file:
        for file_path in file_paths:
            archive_name = os.path.basename(file_path)
            zip_file.write(file_path, archive_name)
    with connection.cursor() as cursor:
        hours, remainder = divmod(time.time() - start, 3600)
        minutes, seconds = divmod(remainder, 60)
        cursor.execute(
            "INSERT INTO test_spark_download_perf (duration, stage, notes) "
            f"VALUES ('{int(hours)}h:{int(minutes)}m:{round(seconds, 3)}s', 'Create zip file', NULL)"
        )
