from pathlib import Path
from typing import Any

from usaspending_api.common.etl.postgres import ETLQuery


class ETLQueryFile(ETLQuery):
    """Same as ETLQuery but reads the query from a file."""

    def __init__(self, file_path: str, **sql_kwargs: Any) -> None:
        sql = Path(file_path).read_text().format(**sql_kwargs)
        super(ETLQueryFile, self).__init__(sql)


__all__ = ["ETLQueryFile"]
