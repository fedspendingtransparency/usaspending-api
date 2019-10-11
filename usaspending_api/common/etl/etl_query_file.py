from pathlib import Path
from usaspending_api.common.etl import ETLQuery


class ETLQueryFile(ETLQuery):
    """ Same as ETLQuery but reads the query from a file. """

    def __init__(self, file_path: str) -> None:
        super(ETLQueryFile, self).__init__(Path(file_path).read_text())


__all__ = ["ETLQueryFile"]
