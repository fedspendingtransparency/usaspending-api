import os
import zipfile
from django.conf import settings
from usaspending_api.common.helpers.s3_helpers import download_s3_object, multipart_upload


def append_files_to_zip_file(file_paths, zip_file_path):
    """
    Create zip archive at the specified zip_file_path (not s3 or http). if it does not exist, and add all the files at provided
    file_paths to it.

    NOTE: If a zip file already exists at zip_file_path, the given files will be added in addition to the ones
    already in the zip when using append (`a`) mode. If that zip contains a file with the same name as one provided,
    it will throw a UserWarning and duplicate the file.
    Use caution in this case by removing the zip in the finally of an exception and also checking for and removing
    the zip if it exists before you begin to create it from scratch
    """
    with zipfile.ZipFile(zip_file_path, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip_file:
        for file_path in file_paths:
            archive_name = os.path.basename(file_path)
            zip_file.write(file_path, archive_name)


def append_files_to_zip_file_s3(
    file_paths, zip_file_name, zip_file_path, bucket_name, region_name: str = settings.USASPENDING_AWS_REGION
):
    """
    Create zip archive at the specified zip_file_path (s3 only). if it does not exist, and add all the files at provided
    file_paths to it.

    NOTE: If a zip file already exists at zip_file_path, the given files will be added in addition to the ones
    already in the zip when using append (`a`) mode. If that zip contains a file with the same name as one provided,
    it will throw a UserWarning and duplicate the file.
    Use caution in this case by removing the zip in the finally of an exception and also checking for and removing
    the zip if it exists before you begin to create it from scratch
    """
    print(file_paths, zip_file_name, zip_file_path, bucket_name)
    download_s3_object(bucket_name, zip_file_name, zip_file_path, region_name=region_name)
    append_files_to_zip_file(file_paths, zip_file_path)
    multipart_upload(bucket_name, region_name, zip_file_path, zip_file_name)
