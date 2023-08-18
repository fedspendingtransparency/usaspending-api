from abc import ABC, abstractmethod
from pathlib import Path


class AbstractFileSystemStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    The Covid-19 "file system" strategy establishes the interface for a suite of file system
    methods. These methods are specific to the platform of the file system. Some file systems
    require specific representations of information in order for the file system to be worked with.
    For example, some file systems are in S3 while others are local.
    """

    @abstractmethod
    def prep_filesystem(self, covid_profile_zip_file_path: str) -> None:
        """
        Args:
            covid_profile_zip_file_path: The path to the covid profile zip location
        """
        pass


class AuroraFileSystemStrategy(AbstractFileSystemStrategy):
    def prep_filesystem(self, covid_profile_zip_file_path):
        covid_profile_zip_file_path = Path(covid_profile_zip_file_path)
        if covid_profile_zip_file_path.exists():
            # Clean up a zip file that might exist from a prior attempt at this download
            covid_profile_zip_file_path.unlink()

        if not covid_profile_zip_file_path.parent.exists():
            covid_profile_zip_file_path.parent.mkdir()