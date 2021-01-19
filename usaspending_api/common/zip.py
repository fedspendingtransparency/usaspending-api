from zipfile import ZipFile


def extract_single_file_zip(zip_file_path, destination_directory_path):
    """
    Accepts a zip file path and destination directory path then extracts a single file from zip file
    into the destination directory.  ZIP archive must contain one and only one file.

    Returns the file path of the extracted file.
    """
    with ZipFile(zip_file_path) as zip_file:
        zip_files = zip_file.namelist()
        file_count = len(zip_files)
        if file_count < 1:
            raise RuntimeError("No files found in zip archive")
        if file_count > 1:
            raise NotImplementedError("Expected no more than one file in zip archive")
        return zip_file.extract(zip_files[0], path=destination_directory_path)
