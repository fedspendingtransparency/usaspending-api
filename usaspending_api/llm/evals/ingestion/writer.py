import json
import os
import tempfile
from pathlib import Path
from typing import Any

from usaspending_api.llm.evals.exceptions import DatasetError


def write_ground_truth_json(cases: list[dict[str, Any]], output_path: Path) -> None:
    """Write transformed cases to the runtime JSON dataset."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            json.dump(cases, temporary_file, indent=4, ensure_ascii=False)
            temporary_file.write("\n")
            temporary_path = Path(temporary_file.name)

        os.replace(temporary_path, output_path)
    except OSError as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise DatasetError(f"Unable to write generated ground truth '{output_path}': {exc}") from exc
