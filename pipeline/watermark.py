"""Watermark state: remembers how far the last successful run read."""
import json
import os
import tempfile
from datetime import datetime
from typing import Optional

TS_FMT = "%Y-%m-%d %H:%M:%S.%f"


class WatermarkStore:
    """JSON file based store. Writes are atomic (temp file + rename) so a crash never leaves a half-written state."""

    def __init__(self, path: str):
        self.path = path

    def read(self) -> Optional[datetime]:
        if not os.path.exists(self.path):
            return None
        with open(self.path) as f:
            value = json.load(f).get("last_watermark")
        return datetime.strptime(value, TS_FMT) if value else None

    def write(self, watermark: datetime, rows: int) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        payload = {
            "last_watermark": watermark.strftime(TS_FMT),
            "rows_last_run": rows,
            "updated_at": datetime.now().strftime(TS_FMT),
        }
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(self.path)))
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, self.path)
