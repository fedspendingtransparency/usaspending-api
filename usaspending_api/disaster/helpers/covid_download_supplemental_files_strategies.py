from abc import ABC, abstractmethod
from pathlib import Path
from usaspending_api.download.filestreaming.download_generation import (
    fetch_data_dictionary,
)
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file, append_files_to_zip_file_s3


class AbstractSupplementalFilesStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    The Covid-19 "supplemental file" strategy establishes the interface for a suite of algorthims that
    append the various ADDITIONAL files to an existing covid download zip file.
    """

    def __init__(self, data_dictionary_name, output_dir_path, *args, **kwargs):
        """
        Args:
            output_dir_path: A path on the host machine (as a string) to put files temporarily
        """
        super().__init__(*args, **kwargs)
        self.output_dir_path = output_dir_path
        self.data_dictionary_name = data_dictionary_name

    @abstractmethod
    def append_data_dictionary(
        self,
        covid_profile_download_zip_path: str,
    ) -> str:
        """
        Args:
            covid_profile_download_zip_path: The path (as a string) to the covid profile download zip file
        Returns:
            str: The path to the data dictionary as a string
        """
        pass

    @abstractmethod
    def append_description_file(
        self,
        readme_path: str,
        covid_profile_download_zip_path: str,
    ) -> str:
        """
        Args:
            readme_path: The path to a readme to append
            covid_profile_download_zip_path: The path (as a string) to the covid profile download zip file
        Returns:
            str: The path to the generated description file
        """
        pass


class AuroraSupplementalFilesStrategy(AbstractSupplementalFilesStrategy):
    def append_data_dictionary(self, covid_profile_download_zip_path):
        output_dir_path = Path(self.output_dir_path)
        output_dir_path / self.data_dictionary_name
        covid_profile_download_zip_path = Path(covid_profile_download_zip_path)
        data_dictionary_file_path = fetch_data_dictionary(str(output_dir_path))
        append_files_to_zip_file([data_dictionary_file_path], str(covid_profile_download_zip_path))
        return data_dictionary_file_path

    def append_description_file(self, readme_path, covid_profile_download_zip_path):
        covid_profile_download_zip_path = Path(covid_profile_download_zip_path)
        readme_path = Path(readme_path)
        file_description = build_file_description(str(readme_path), dict())
        file_description_path = save_file_description(
            str(covid_profile_download_zip_path.parent), readme_path.name, file_description
        )
        append_files_to_zip_file([file_description_path], str(covid_profile_download_zip_path))
        return file_description_path


class DatabricksSupplementalFilesStrategy(AbstractSupplementalFilesStrategy):
    def __init__(self, covid_profile_download_file_name, bucket_name, *args, **kwargs):
        """
        Args:
            output_dir_path: A path on the host machine (as a string) to put files temporarily
            covid_profile_download_file_name: The name of the covid profile download file
        """
        super().__init__(*args, **kwargs)
        self.covid_profile_download_file_name = covid_profile_download_file_name
        self.bucket_name = bucket_name

    def append_data_dictionary(self, covid_profile_download_zip_path):
        output_dir_path = Path(self.output_dir_path)
        output_dir_path.mkdir()
        (output_dir_path / self.data_dictionary_name).touch()
        data_dictionary_file_path = fetch_data_dictionary(str(output_dir_path))
        local_covid_profile_zip_path_download = output_dir_path / self.covid_profile_download_file_name
        print(
            data_dictionary_file_path,
            self.covid_profile_download_file_name,
            local_covid_profile_zip_path_download,
            self.bucket_name,
        )
        append_files_to_zip_file_s3(
            [data_dictionary_file_path],
            self.covid_profile_download_file_name,
            str(local_covid_profile_zip_path_download),
            bucket_name=self.bucket_name,
        )
        return data_dictionary_file_path

    def append_description_file(self, readme_path, covid_profile_download_zip_path):
        output_dir_path = Path(self.output_dir_path)
        output_dir_path.mkdir()
        readme_path = Path(readme_path)
        file_description = build_file_description(str(readme_path), dict())
        file_description_path = save_file_description(output_dir_path, readme_path.name, file_description)
        append_files_to_zip_file_s3(
            [file_description_path],
            self.covid_profile_download_file_name,
            covid_profile_download_zip_path,
            bucket_name=self.bucket_name,
        )
        return file_description_path